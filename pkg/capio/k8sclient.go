package capio

import (
	"fmt"

	mpiclientset "github.com/kubeflow/mpi-operator/pkg/client/clientset/versioned"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
)

// k8sClients bundles the two Kubernetes clients the multi-node (MPIJob-backed)
// path needs: the core clientset for ConfigMaps, and the MPI Operator's own
// typed clientset for MPIJob objects. Both are built from in-cluster config,
// since the driver only ever runs as a pod inside the cluster it manages.
type k8sClients struct {
	core kubernetes.Interface
	mpi  mpiclientset.Interface
}

// newK8sClients builds both clients from the pod's own in-cluster service
// account. Returns an error rather than panicking so the single-node path
// (which needs neither client) keeps working even if, for some reason, the
// driver isn't running inside a cluster (e.g. a unit test).
func newK8sClients() (*k8sClients, error) {
	cfg, err := rest.InClusterConfig()
	if err != nil {
		return nil, fmt.Errorf("failed to load in-cluster config: %w", err)
	}

	core, err := kubernetes.NewForConfig(cfg)
	if err != nil {
		return nil, fmt.Errorf("failed to build core clientset: %w", err)
	}

	mpi, err := mpiclientset.NewForConfig(cfg)
	if err != nil {
		return nil, fmt.Errorf("failed to build MPI Operator clientset: %w", err)
	}

	return &k8sClients{core: core, mpi: mpi}, nil
}
