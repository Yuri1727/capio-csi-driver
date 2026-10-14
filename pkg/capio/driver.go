// Package capio implements a CSI driver backed by CAPIO.
package capio

import (
	"context"
	"fmt"
	"net"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"github.com/kubernetes-csi/csi-lib-utils/protosanitizer"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"k8s.io/klog/v2"
)

const (
	// DriverName is the name reported to Kubernetes via GetPluginInfo.
	// It must be unique in the cluster and follows the reverse-DNS
	// convention used by every CSI driver.
	DriverName = "capio.csi.k8s.io"

	// DriverVersion is reported alongside DriverName.
	DriverVersion = "0.1.0"

	// TopologyNodeKey is the topology segment key this driver reports for
	// every node (NodeGetInfo) and attaches to single-node volumes
	// (CreateVolume). kubelet turns it into a label on each Node object, and
	// the external-provisioner turns a volume's segment into the PV's
	// nodeAffinity, so the scheduler places pods using a single-node volume
	// on the node that runs its capio_server without any node name having to
	// be written into a manifest. The key must stay outside the kubernetes.io
	// and k8s.io domains: the NodeRestriction admission plugin forbids kubelet
	// from setting node labels there, and plugin registration then fails.
	TopologyNodeKey = "topology.capio.csi/node"
)

// Driver bundles the three CSI gRPC services (Identity, Controller, Node)
// that together implement the CSI contract. A single binary runs all three,
// but in the cluster deployment only the relevant one is actually invoked
// depending on whether the pod is the controller or a per-node instance.
type Driver struct {
	csi.UnimplementedIdentityServer
	csi.UnimplementedControllerServer
	csi.UnimplementedNodeServer

	name     string
	version  string
	nodeID   string
	endpoint string

	volumes *volumeManager

	// mpiImage/mpiImagePullSecret are used to build the MPIJob launcher and
	// worker pods for the multi-node (nodeCount>1) path - see mpijob.go.
	// The same image that runs this driver already bundles capio_server,
	// mpirun and sshd, so it doubles as the MPIJob image.
	mpiImage           string
	mpiImagePullSecret string

	// k8sClientsOnce/k8sClients/k8sClientsErr lazily build the Kubernetes
	// clients the multi-node path needs on first use, so the single-node
	// path never needs in-cluster credentials to be available.
	k8sClientsOnce sync.Once
	k8sClients     *k8sClients
	k8sClientsErr  error
	namespace      string

	server *grpc.Server
}

// NewDriver builds a Driver ready to be started with Run.
// nodeID identifies the Kubernetes node this instance runs on (used by the
// Node service to answer NodeGetInfo); endpoint is the unix:// socket the
// gRPC server listens on, the same socket kubelet talks to. capioRoot is
// where per-volume directories and CAPIO-CL configs are kept; capioServerBin
// is the path to the capio_server binary the Controller spawns one instance
// of per volume. mpiImage/mpiImagePullSecret are only used for volumes
// requesting nodeCount>1 in their StorageClass (see workflow.go).
func NewDriver(nodeID, endpoint, capioRoot, capioServerBin, mpiImage, mpiImagePullSecret string) *Driver {
	return &Driver{
		name:               DriverName,
		version:            DriverVersion,
		nodeID:             nodeID,
		endpoint:           endpoint,
		volumes:            newVolumeManager(capioRoot, capioServerBin),
		mpiImage:           mpiImage,
		mpiImagePullSecret: mpiImagePullSecret,
	}
}

// namespaceFile is where every pod can read its own namespace, projected
// automatically by the service account token mechanism - no need to pass it
// as a flag or read it via the downward API.
const namespaceFile = "/var/run/secrets/kubernetes.io/serviceaccount/namespace"

// getK8sClients returns the lazily-built Kubernetes clients and the
// driver's own namespace, building both on first call. Only the multi-node
// path (mpijob.go) needs this.
func (d *Driver) getK8sClients() (*k8sClients, string, error) {
	d.k8sClientsOnce.Do(func() {
		d.k8sClients, d.k8sClientsErr = newK8sClients()
		if d.k8sClientsErr != nil {
			return
		}
		nsBytes, err := os.ReadFile(namespaceFile)
		if err != nil {
			d.k8sClientsErr = fmt.Errorf("failed to read own namespace from %q: %w", namespaceFile, err)
			return
		}
		d.namespace = strings.TrimSpace(string(nsBytes))
	})
	return d.k8sClients, d.namespace, d.k8sClientsErr
}

// Run starts the gRPC server and blocks until it stops.
func (d *Driver) Run() error {
	listener, cleanup, err := listen(d.endpoint)
	if err != nil {
		return err
	}
	defer cleanup()

	interceptor := grpc.UnaryInterceptor(logGRPC)
	d.server = grpc.NewServer(interceptor)

	csi.RegisterIdentityServer(d.server, d)
	csi.RegisterControllerServer(d.server, d)
	csi.RegisterNodeServer(d.server, d)

	klog.Infof("listening for connections on address: %v", listener.Addr())
	return d.server.Serve(listener)
}

// listen parses a unix:// endpoint and opens the corresponding socket,
// removing any stale socket file left over from a previous run.
func listen(endpoint string) (net.Listener, func(), error) {
	u, err := url.Parse(endpoint)
	if err != nil {
		return nil, nil, fmt.Errorf("invalid endpoint %q: %w", endpoint, err)
	}
	if u.Scheme != "unix" {
		return nil, nil, fmt.Errorf("unsupported endpoint scheme %q, only unix:// is supported", u.Scheme)
	}

	socketPath := filepath.Join(u.Host, u.Path)
	if err := os.Remove(socketPath); err != nil && !os.IsNotExist(err) {
		return nil, nil, fmt.Errorf("failed to remove stale socket %q: %w", socketPath, err)
	}

	listener, err := net.Listen("unix", socketPath)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to listen on %q: %w", socketPath, err)
	}

	cleanup := func() { _ = os.Remove(socketPath) }
	return listener, cleanup, nil
}

// logGRPC logs every request/response pair going through the server,
// redacting any field the CSI spec marks as secret (protosanitizer).
func logGRPC(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
	klog.V(4).Infof("gRPC call: %s", info.FullMethod)
	klog.V(5).Infof("gRPC request: %s", protosanitizer.StripSecrets(req))

	resp, err := handler(ctx, req)
	if err != nil {
		klog.Errorf("gRPC error: %v", err)
	} else {
		klog.V(5).Infof("gRPC response: %s", protosanitizer.StripSecrets(resp))
	}
	return resp, err
}

// unimplemented is a small helper for methods we haven't implemented yet:
// it keeps stubs one-line instead of repeating status.Errorf everywhere.
func unimplemented(method string) error {
	return status.Errorf(codes.Unimplemented, "%s is not implemented yet", method)
}
