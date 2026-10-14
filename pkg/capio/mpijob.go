package capio

import (
	"context"
	"fmt"
	"strconv"
	"time"

	mpiv2beta1 "github.com/kubeflow/mpi-operator/pkg/apis/kubeflow/v2beta1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/ptr"
)

// sharedDiscoveryMountPath is where the NFS volume used for CAPIO's
// cross-node file-location bookkeeping is mounted, and the working
// directory capio_server is started in on every worker (via mpirun's
// --wdir, not the pod spec's workingDir - see buildMPIJob for why). CAPIO
// writes files_location_<node>.txt relative to its own working directory,
// not under CAPIO_DIR, so this is the only way to make that file land on
// shared storage instead of the worker pod's own filesystem.
const sharedDiscoveryMountPath = "/shared"

// sharedDiscoveryPVCName is the PVC every MPIJob-backed volume mounts at
// sharedDiscoveryMountPath. A single shared PVC (backed by the in-namespace
// NFS server, see deploy/talos-shared-cluster/nfs/) is reused across every
// multi-node volume rather than provisioning one per volume: it only ever
// holds small per-workflow bookkeeping files (named after the workflow, so
// they don't collide with each other), not real data.
const sharedDiscoveryPVCName = "capio-discovery"

// mpiJobName is the MPIJob (and ConfigMap) name for a given CSI volume ID.
// No extra prefix beyond the id itself (which already starts with
// multiNodeIDPrefix): the MPI Operator appends up to "-worker-<N>" to
// derive each worker pod's own name, and pod names are DNS labels capped at
// 63 characters - CSI volume IDs from the external-provisioner sidecar are
// already "pvc-<uuid>" (40 chars), so there's very little room left for any
// prefix of our own without silently producing an MPIJob the operator
// rejects at validation (found the hard way: "capio-vol-" + that pushed
// worker-1's name to 65 characters, over the limit, with the only symptom
// being an MPIJob stuck forever with zero pods and a validation error
// buried in `kubectl describe mpijob`, not surfaced anywhere else).
func mpiJobName(volumeID string) string {
	return volumeID
}

// "-capiocl", not "-config": the MPI Operator itself auto-creates a
// ConfigMap named "<job-name>-config" for the SSH hostfile (found by
// hitting the collision directly - the operator logged
// `ErrResourceExists ... "<name>-config" ... not managed by MPIJob` and
// never created any pods).
func configMapName(volumeID string) string {
	return mpiJobName(volumeID) + "-capiocl"
}

// buildCapioCLConfigMap wraps the CAPIO-CL JSON (from workflow.go, unchanged
// between the single-node and multi-node paths) in a ConfigMap, so it can be
// mounted into worker pods instead of written to a local file as the
// single-node path does.
func buildCapioCLConfigMap(namespace, volumeID string, params *volumeParams) (*corev1.ConfigMap, error) {
	cfgBytes, err := buildWorkflowConfig(params).marshalIndent()
	if err != nil {
		return nil, fmt.Errorf("failed to marshal CAPIO-CL config: %w", err)
	}

	return &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      configMapName(volumeID),
			Namespace: namespace,
		},
		Data: map[string]string{"config.json": string(cfgBytes)},
	}, nil
}

// buildMPIJob builds the MPIJob for volumeID: one capio_server per node,
// backend "mpi", one MPI rank each, following the recipe validated by hand
// against this cluster's MPI Operator install (see
// docs/multinode-segfault-kubernetes-repro.md and
// deploy/talos-shared-cluster/mpi-operator/README.md for how each piece of
// this was found):
//
//   - sshAuthMountPath at /root/.ssh: our images run as root.
//   - mpirun is given --hostfile explicitly rather than relying on the
//     operator's injected OMPI_MCA_orte_default_hostfile env var, which is
//     an ORTE-generation variable that a PRRTE-based Open MPI (5.x+) would
//     silently ignore, dumping every rank onto the launcher node - fatal for
//     CAPIO, which crashes if two ranks share a physical node. Our image
//     ships Open MPI 4.1.4 (ORTE), so the injected var would work today, but
//     --hostfile is correct under both and doesn't rot if the base image
//     ever moves to 5.x.
//   - --wdir points every rank's working directory at the shared NFS mount,
//     because CAPIO's cross-node file-location bookkeeping file is written
//     relative to the process's cwd, not under CAPIO_DIR (confirmed by
//     inspecting a running server: the file showed up at
//     /files_location_<node>.txt, not under /capio). The pod spec's own
//     workingDir field would not have worked here: mpirun starts the actual
//     capio_server ranks over SSH into each worker, and an SSH session's
//     cwd is independent of the container's own initial-process workingDir.
//   - Workers need hostIPC (clients on the same node reach the local
//     capio_server over POSIX shared memory) and hostPID (fixes the
//     cross-pod PID collision documented in
//     docs/capio-concurrent-read-race.md - without it, two client processes
//     in different pods can be assigned the same small container-local PID
//     and silently corrupt each other's state in the server).
//   - Required podAntiAffinity keeps every worker on its own physical node -
//     CAPIO's own constraint, not a performance choice: two ranks on one
//     node collide on the same SHM canary name and one of them crashes.
func buildMPIJob(namespace, volumeID, image string, imagePullSecret string, params *volumeParams) *mpiv2beta1.MPIJob {
	name := mpiJobName(volumeID)
	nodeCount := int32(params.nodeCount)

	workerLabels := map[string]string{
		"training.kubeflow.org/job-name": name,
		"training.kubeflow.org/job-role": "worker",
	}

	pullSecrets := []corev1.LocalObjectReference{{Name: imagePullSecret}}
	capioDirVolume := corev1.Volume{
		Name:         "capio-dir",
		VolumeSource: corev1.VolumeSource{EmptyDir: &corev1.EmptyDirVolumeSource{}},
	}
	configVolume := corev1.Volume{
		Name: "capio-config",
		VolumeSource: corev1.VolumeSource{
			ConfigMap: &corev1.ConfigMapVolumeSource{
				LocalObjectReference: corev1.LocalObjectReference{Name: configMapName(volumeID)},
			},
		},
	}
	sharedVolume := corev1.Volume{
		Name: "shared-discovery",
		VolumeSource: corev1.VolumeSource{
			PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{ClaimName: sharedDiscoveryPVCName},
		},
	}
	capioDirEnv := corev1.EnvVar{Name: "CAPIO_DIR", Value: FixedMountPath}

	launcherArgs := []string{
		"--allow-run-as-root",
		"--hostfile", "/etc/mpi/hostfile",
		"-x", "CAPIO_DIR",
		"--wdir", sharedDiscoveryMountPath,
		"-n", strconv.Itoa(params.nodeCount),
		"/usr/local/bin/capio_server",
		"--backend", "mpi",
		"--config", "/etc/capio/config.json",
	}

	return &mpiv2beta1.MPIJob{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: namespace,
		},
		Spec: mpiv2beta1.MPIJobSpec{
			SlotsPerWorker:   ptr.To[int32](1),
			SSHAuthMountPath: "/root/.ssh",
			RunPolicy: mpiv2beta1.RunPolicy{
				CleanPodPolicy: ptr.To(mpiv2beta1.CleanPodPolicyNone),
			},
			MPIReplicaSpecs: map[mpiv2beta1.MPIReplicaType]*mpiv2beta1.ReplicaSpec{
				mpiv2beta1.MPIReplicaTypeLauncher: {
					Replicas: ptr.To[int32](1),
					Template: corev1.PodTemplateSpec{
						Spec: corev1.PodSpec{
							ImagePullSecrets: pullSecrets,
							Containers: []corev1.Container{{
								Name:            "launcher",
								Image:           image,
								ImagePullPolicy: corev1.PullAlways,
								Command:         []string{"mpirun"},
								Args:            launcherArgs,
								Env:             []corev1.EnvVar{capioDirEnv},
							}},
						},
					},
				},
				mpiv2beta1.MPIReplicaTypeWorker: {
					Replicas: &nodeCount,
					Template: corev1.PodTemplateSpec{
						ObjectMeta: metav1.ObjectMeta{Labels: workerLabels},
						Spec: corev1.PodSpec{
							ImagePullSecrets: pullSecrets,
							HostIPC:          true,
							HostPID:          true,
							Affinity: &corev1.Affinity{
								PodAntiAffinity: &corev1.PodAntiAffinity{
									RequiredDuringSchedulingIgnoredDuringExecution: []corev1.PodAffinityTerm{{
										LabelSelector: &metav1.LabelSelector{MatchLabels: workerLabels},
										TopologyKey:   "kubernetes.io/hostname",
									}},
								},
							},
							Containers: []corev1.Container{{
								Name:            "worker",
								Image:           image,
								ImagePullPolicy: corev1.PullAlways,
								Command:         []string{"/usr/sbin/sshd"},
								Args:            []string{"-De"},
								Env:             []corev1.EnvVar{capioDirEnv},
								// Deleting the volume deletes the MPIJob and with it this
								// pod: remove the workflow's segments from this node's
								// /dev/shm (hostIPC) before the container stops. The
								// binary is the driver itself, bundled in the same image.
								Lifecycle: &corev1.Lifecycle{
									PreStop: &corev1.LifecycleHandler{
										Exec: &corev1.ExecAction{
											Command: []string{"/usr/local/bin/capio-csi-driver", CleanSHMArg, params.workflowName},
										},
									},
								},
								VolumeMounts: []corev1.VolumeMount{
									{Name: capioDirVolume.Name, MountPath: FixedMountPath},
									{Name: configVolume.Name, MountPath: "/etc/capio"},
									{Name: sharedVolume.Name, MountPath: sharedDiscoveryMountPath},
								},
							}},
							Volumes: []corev1.Volume{capioDirVolume, configVolume, sharedVolume},
						},
					},
				},
			},
		},
	}
}

// waitForMPIJobReady polls the MPIJob's status until its Running condition
// is true, or ctx expires. There's no need for a full watch/informer here:
// CreateVolume is a single synchronous RPC call and only needs to block
// until the job is up, the same role the single-node path's fixed
// time.Sleep plays after starting its local subprocess - this just takes
// longer (an MPIJob start took ~30-45s in manual testing, not ~1s) and the
// actual duration varies with image pull time, so a fixed sleep won't do.
func waitForMPIJobReady(ctx context.Context, clients *k8sClients, namespace, volumeID string) error {
	name := mpiJobName(volumeID)
	for {
		job, err := clients.mpi.KubeflowV2beta1().MPIJobs(namespace).Get(ctx, name, metav1.GetOptions{})
		if err != nil {
			return fmt.Errorf("failed to get MPIJob %q: %w", name, err)
		}
		for _, cond := range job.Status.Conditions {
			if cond.Type == mpiv2beta1.JobRunning && cond.Status == corev1.ConditionTrue {
				return nil
			}
			if cond.Type == mpiv2beta1.JobFailed && cond.Status == corev1.ConditionTrue {
				return fmt.Errorf("MPIJob %q failed to start: %s", name, cond.Message)
			}
		}

		select {
		case <-ctx.Done():
			return fmt.Errorf("timed out waiting for MPIJob %q to become ready: %w", name, ctx.Err())
		case <-time.After(2 * time.Second):
		}
	}
}

// createMPIJobVolume provisions the ConfigMap + MPIJob pair for a multi-node
// volume and waits for it to be ready. Mirrors volumeManager.create's
// contract: idempotent on volumeID, since CSI requires CreateVolume to be
// safe to retry.
func createMPIJobVolume(ctx context.Context, clients *k8sClients, namespace, volumeID, image, imagePullSecret string, params *volumeParams) error {
	cm, err := buildCapioCLConfigMap(namespace, volumeID, params)
	if err != nil {
		return err
	}
	if _, err := clients.core.CoreV1().ConfigMaps(namespace).Create(ctx, cm, metav1.CreateOptions{}); err != nil && !apierrors.IsAlreadyExists(err) {
		return fmt.Errorf("failed to create ConfigMap %q: %w", cm.Name, err)
	}

	job := buildMPIJob(namespace, volumeID, image, imagePullSecret, params)
	if _, err := clients.mpi.KubeflowV2beta1().MPIJobs(namespace).Create(ctx, job, metav1.CreateOptions{}); err != nil && !apierrors.IsAlreadyExists(err) {
		return fmt.Errorf("failed to create MPIJob %q: %w", job.Name, err)
	}

	return waitForMPIJobReady(ctx, clients, namespace, volumeID)
}

// deleteMPIJobVolume tears down what createMPIJobVolume set up. Deleting the
// MPIJob cascades to its launcher and worker pods (owner references, set by
// the MPI Operator); safe to call on an id that was never created, as CSI
// requires DeleteVolume to be.
func deleteMPIJobVolume(ctx context.Context, clients *k8sClients, namespace, volumeID string) error {
	name := mpiJobName(volumeID)
	if err := clients.mpi.KubeflowV2beta1().MPIJobs(namespace).Delete(ctx, name, metav1.DeleteOptions{}); err != nil && !apierrors.IsNotFound(err) {
		return fmt.Errorf("failed to delete MPIJob %q: %w", name, err)
	}

	cmName := configMapName(volumeID)
	if err := clients.core.CoreV1().ConfigMaps(namespace).Delete(ctx, cmName, metav1.DeleteOptions{}); err != nil && !apierrors.IsNotFound(err) {
		return fmt.Errorf("failed to delete ConfigMap %q: %w", cmName, err)
	}

	return nil
}
