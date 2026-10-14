# Kubeflow MPI Operator on the shared Talos cluster

Installed 2026-09-13 to orchestrate multi-node `capio_server` instances as MPI ranks
(one per node), which a plain DaemonSet cannot do: MPI's `MPI_Init_thread` on
`MPI_COMM_WORLD` is a collective operation and requires every rank to start together.

## What is installed

Version `v0.6.0` (image `mpioperator/mpi-operator:0.6.0`), from
`https://raw.githubusercontent.com/kubeflow/mpi-operator/v0.6.0/deploy/v2beta1/mpi-operator.yaml`.

- `mpi-operator-v0.6.0-upstream.yaml` — the upstream manifest, unmodified, kept as the
  reference of what was downloaded.
- `mpi-operator-v0.6.0-scoped.yaml` — what was actually applied. Differs from upstream by
  **exactly one line**: `--namespace=capio-tesi-cona` added to the controller's args, so it
  only reconciles MPIJobs in our namespace instead of cluster-wide (confirmed in the
  controller log: `Scoping operator to namespace capio-tesi-cona`).

Cluster-scoped objects it adds: the `mpijobs.kubeflow.org` CRD, four ClusterRoles
(`mpi-operator`, `kubeflow-mpijobs-admin/edit/view`), one ClusterRoleBinding, and the
`mpi-operator` namespace with a ServiceAccount and a single-replica Deployment.

It contains **no admission webhooks** (checked before installing) — so it cannot block API
operations for other tenants if it fails.

## Applying / updating

Must use **server-side apply**: this CRD's schema exceeds the 256KB limit for the
`kubectl.kubernetes.io/last-applied-configuration` annotation that plain `kubectl apply`
writes, and fails with `metadata.annotations: Too long`.

```
kubectl --context=streamflow apply --server-side -f mpi-operator-v0.6.0-scoped.yaml
```

## Uninstalling (full removal)

```
kubectl --context=streamflow delete -f mpi-operator-v0.6.0-scoped.yaml
```

This removes everything listed above, including the CRD. Deleting the CRD also deletes any
MPIJob objects still present, so make sure no MPIJob is in use first:

```
kubectl --context=streamflow get mpijobs -A
```

## Privileges to be aware of

The `mpi-operator` ClusterRole is broad, and stays broad regardless of the `--namespace`
restriction (that flag limits what the controller *acts on*, not what its ServiceAccount is
*permitted* to do):

- `secrets`: `create, list, watch, update` **cluster-wide** — in Kubernetes, `list` on
  secrets returns their contents, so this service account can read secrets in every
  namespace. The operator needs secrets to generate the launcher↔worker SSH keypair.
- `pods/exec`: `create` **cluster-wide** — can exec into any pod on the cluster.
- `pods`, `configmaps`, `services`, `endpoints`, `jobs`: create/update/delete cluster-wide.

This is the upstream-supported RBAC, left unmodified deliberately: narrowing it to
namespaced Roles is possible in principle but risks subtle, hard-to-debug permission
failures. If the cluster owners want it tightened, that is a reasonable follow-up — the
starting point would be converting the pods/secrets/configmaps/services/endpoints/jobs rules
into a `Role` in `capio-tesi-cona`, keeping only `customresourcedefinitions` (get) and
`priorityclasses` (get/list/watch) cluster-scoped.

## Validated recipe (smoke-tested 2026-09-13)

Three things had to be right before ranks actually landed one-per-node. All three were found
by smoke-testing the upstream `mpioperator/mpi-pi:openmpi` example before writing any driver
code against the operator.

1. **`sshAuthMountPath` must match the user the container runs as.** The operator defaults to
   mounting the generated SSH keypair at `/root/.ssh`; with `runAsUser: 1000` and an sshd
   configured for `/home/mpiuser/.ssh`, the workers died with
   `sshd: no hostkeys available -- exiting`. Set `spec.sshAuthMountPath` explicitly.

2. **Pass `--hostfile /etc/mpi/hostfile` to `mpirun` explicitly.** Do not rely on the
   operator's injected environment. The operator sets `OMPI_MCA_orte_default_hostfile`,
   which is an **ORTE** variable (Open MPI 4.x and earlier). The example image ships Open
   MPI 5.x, whose runtime is **PRRTE**, and PRRTE ignores `OMPI_MCA_orte_*` entirely —
   visible in the logs as `prterun-...` job ids. The symptom is silent and misleading: no
   error, the hostfile simply ignored, and `--display-map` showing

   ```
   Total slots allocated 4
   Data for node: smoke-hostname-launcher	Num slots: 4	Num procs: 2
   ```

   i.e. every rank crammed onto the launcher node. For CAPIO that would be fatal, not merely
   inefficient: two ranks on one physical machine collide on the SHM canary, which is the
   crash documented in the multi-node investigation. Passing `--hostfile` explicitly works
   under both ORTE and PRRTE.

3. **Worker pods need an sshd**, since the operator's launcher reaches them over SSH (port
   2222).

With all three, `mpirun --hostfile /etc/mpi/hostfile -n 2 hostname` correctly printed
`smoke-hostfile-worker-0` and `smoke-hostfile-worker-1`, on two different physical nodes.

## Implications for the CAPIO image

Checked against `localhost/capio-csi-driver:dev`:

- **Open MPI version: 4.1.4** (`libopenmpi3` from debian bookworm) — this is the ORTE
  generation, so the operator's injected env vars *would* work for us. Passing `--hostfile`
  explicitly anyway is the safer choice, since it is correct for both generations and
  survives a future base-image bump to Open MPI 5.x.
- **No sshd in the image.** This is a hard prerequisite for running `capio_server` as MPIJob
  workers, and the Dockerfile will need `openssh-server` plus the key/permission setup the
  operator expects. Not done yet.

## Note

This was installed on a cluster shared with other projects (`lofar`, `keda`,
`gitlab-colonne`). The cluster owners should be told it is here, along with the privilege
summary above.
