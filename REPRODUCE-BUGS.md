# Reproducing the three CAPIO bugs on unmodified upstream code

The `main` branch's `Dockerfile` applies three patches on top of this same commit; this one
skips that step entirely — same clone, same commit, no `git apply`.

The three bugs fixed on `main`:

1. **Concurrent local reads race** (`capio/server/src/capio_file.cpp`,
   `capio/server/include/handlers/read.hpp`) — `getBuffer()` exposes the file buffer via a
   raw pointer with no lock; concurrent readers of the same file can read through it while
   another thread frees/replaces it.
2. **Cross-node request routing** (`capio/server/src/mpi_backend.cpp`) —
   `rank_to_hostname` is never populated for remote ranks, so a node ends up replying to
   itself instead of the real requester and crashes.
3. **Remote read EOF** (`capio/server/include/remote/handlers/read.hpp`) — a remote read
   reply uses the client's requested read size instead of the real available size, so a
   client never sees EOF.

Bug 3 cannot be observed in isolation on this unmodified image: bug 2 crashes the
data-holding node before a client ever gets far enough into a remote read to notice the EOF
problem. The scenario below reproduces bugs 2 and 3 together, and notes exactly where the
crash happens.

## Build and push the unpatched image

```bash
podman build -t ghcr.io/<your-user>/capio-csi-driver:original-unpatched .
podman push ghcr.io/<your-user>/capio-csi-driver:original-unpatched
```

Deliberately a different tag from `:dev` — this image is only for reproducing the bugs, not
for running the driver.

## Cluster prerequisites

Same as `main`: a Kubernetes cluster with the driver deployed
(`deploy/talos-shared-cluster/{00..05}-*.yaml`, adjusted to your cluster/registry), the
Kubeflow MPI Operator installed for the multi-node scenario
(`deploy/talos-shared-cluster/mpi-operator/`), and — for the multi-node scenario only — the
in-namespace NFS server (`deploy/talos-shared-cluster/nfs/`). Update every
`image: ghcr.io/.../capio-csi-driver:dev` reference in the manifests below to your
`:original-unpatched` tag before applying.

## Bug 1: concurrent local reads

Same-node scenario, `deploy/talos-shared-cluster/example/{pvc.yaml,producer.yaml,consumers.yaml}`
(update the image tag in `producer.yaml`/`consumers.yaml` first):

```bash
kubectl apply -f deploy/talos-shared-cluster/example/pvc.yaml
kubectl apply -f deploy/talos-shared-cluster/example/producer.yaml
kubectl apply -f deploy/talos-shared-cluster/example/consumers.yaml

kubectl logs capio-consumer-1
kubectl logs capio-consumer-2
kubectl logs capio-consumer-3
```

**Expected (buggy) result** — inconsistent across runs, but reliably not all three clean:
one reader typically prints the correct content but its process never reaches `done`; one
prints nothing (empty read) but does reach `done`; one never prints anything at all and
blocks forever. Compare against `main`, where all three consistently print the correct
content and reach `done`.

## Bugs 2 and 3: cross-node read

Requires two schedulable nodes. Adjust the `nodeSelector` values below to match real node
names in your cluster:

```bash
kubectl apply -f deploy/talos-shared-cluster/example/storageclass-multinode.yaml
kubectl apply -f deploy/talos-shared-cluster/example/pvc-multinode.yaml
```

Wait for the PVC to reach `Bound` (`kubectl get pvc capio-shared-pvc-multinode -w`), which
means the MPIJob's two `capio_server` ranks are up and have completed their handshake — the
routing bug does not stop the servers from starting, only from replying to a remote request
correctly.

Then, with the image tag updated to `:original-unpatched`:

```bash
kubectl apply -f deploy/talos-shared-cluster/example/producer-multinode.yaml
kubectl apply -f deploy/talos-shared-cluster/example/consumer-multinode.yaml

kubectl logs capio-producer-multinode
kubectl logs capio-consumer-multinode
```

**Expected (buggy) result**:

- `capio-consumer-multinode` blocks forever, no output beyond "waiting for shared.dat".
- The MPIJob worker pod on the node holding the file terminates with a segfault. Find it
  with:

  ```bash
  kubectl get mpijob
  kubectl get pods -l training.kubeflow.org/job-name=<mpijob-name-from-above>
  kubectl logs <that-mpijob's-launcher-pod>
  ```

  and look for `Segfault detected!` in the launcher's log (it aggregates both workers'
  stdout). This is bug 2. Bug 3 (the EOF issue) is masked here — you will not see it until
  bug 2 is fixed, since the crash happens before a client would ever notice a reply length
  problem.

## Cleanup

```bash
kubectl delete -f deploy/talos-shared-cluster/example/consumers.yaml \
                -f deploy/talos-shared-cluster/example/producer.yaml \
                -f deploy/talos-shared-cluster/example/pvc.yaml \
                -f deploy/talos-shared-cluster/example/consumer-multinode.yaml \
                -f deploy/talos-shared-cluster/example/producer-multinode.yaml \
                -f deploy/talos-shared-cluster/example/pvc-multinode.yaml
```

The crashed MPIJob worker may leave its pod stuck mid-teardown (kubelet teardown latency
unrelated to CAPIO, seen throughout this project's testing) — if `kubectl delete` on the
MPIJob-related pods hangs, force it: `kubectl delete pod <name> --grace-period=0 --force`.
