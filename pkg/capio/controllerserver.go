package capio

import (
	"context"
	"strings"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// defaultCapacityBytes is reported back when a CreateVolumeRequest doesn't
// specify one. CAPIO doesn't pre-allocate fixed-size storage the way a block
// device would, so capacity here is informational rather than enforced.
const defaultCapacityBytes = 1 << 30 // 1Gi

// multiNodeIDPrefix marks a volume ID as belonging to the MPIJob-backed
// (nodeCount>1) path. DeleteVolumeRequest carries only a volume ID, not the
// VolumeContext CreateVolume returned, so there's no other way for
// DeleteVolume to know which backend provisioned a given id without this -
// and unconditionally trying both backends on every delete would mean every
// DeleteVolume call needs working in-cluster Kubernetes credentials with
// MPIJob/ConfigMap RBAC, even on a plain single-node deployment that
// was never given that RBAC. This keeps the two paths fully independent.
const multiNodeIDPrefix = "mnode-"

// ControllerGetCapabilities declares which Controller RPCs this driver
// actually supports. Kubernetes checks this before ever calling
// CreateVolume/DeleteVolume, so it must be kept in sync with what's
// implemented below.
func (d *Driver) ControllerGetCapabilities(ctx context.Context, req *csi.ControllerGetCapabilitiesRequest) (*csi.ControllerGetCapabilitiesResponse, error) {
	capabilities := []csi.ControllerServiceCapability_RPC_Type{
		csi.ControllerServiceCapability_RPC_CREATE_DELETE_VOLUME,
	}

	var out []*csi.ControllerServiceCapability
	for _, c := range capabilities {
		out = append(out, &csi.ControllerServiceCapability{
			Type: &csi.ControllerServiceCapability_Rpc{
				Rpc: &csi.ControllerServiceCapability_RPC{Type: c},
			},
		})
	}

	return &csi.ControllerGetCapabilitiesResponse{Capabilities: out}, nil
}

// CreateVolume provisions a new volume: it translates the StorageClass
// parameters into a CAPIO-CL workflow and starts a dedicated capio_server
// instance to serve it. Every capability the caller asks for must be
// ReadWriteMany, since that's the only access mode CAPIO's producer/consumer
// streaming model actually supports.
func (d *Driver) CreateVolume(ctx context.Context, req *csi.CreateVolumeRequest) (*csi.CreateVolumeResponse, error) {
	if req.GetName() == "" {
		return nil, status.Error(codes.InvalidArgument, "volume name is required")
	}
	if err := validateReadWriteMany(req.GetVolumeCapabilities()); err != nil {
		return nil, err
	}

	params, err := parseVolumeParams(req.GetParameters())
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "invalid StorageClass parameters: %v", err)
	}

	capacity := req.GetCapacityRange().GetRequiredBytes()
	if capacity <= 0 {
		capacity = defaultCapacityBytes
	}

	if params.nodeCount > 1 {
		return d.createMultiNodeVolume(ctx, req.GetName(), capacity, params)
	}

	v, err := d.volumes.create(req.GetName(), capacity, params)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to provision volume: %v", err)
	}

	// The volume's capio_server is a subprocess of this Controller, so the
	// volume is only usable on the node this Controller runs on: declaring it
	// here makes the external-provisioner put a matching nodeAffinity on the
	// PV, and pods using it are scheduled onto this node automatically.
	return &csi.CreateVolumeResponse{
		Volume: &csi.Volume{
			VolumeId:      v.id,
			CapacityBytes: v.capacity,
			VolumeContext: map[string]string{
				"capioDir":     v.capioDir,
				"workflowName": v.workflowName,
			},
			AccessibleTopology: []*csi.Topology{{
				Segments: map[string]string{TopologyNodeKey: d.nodeID},
			}},
		},
	}, nil
}

// createMultiNodeVolume is CreateVolume's path for nodeCount>1: instead of a
// local subprocess (volumeManager), it creates an MPIJob with one
// capio_server per node (see mpijob.go), each mounting its own directory at
// capioDir. VolumeContext reports capioDir so users can read from the PV
// which path clients must mount the volume at and use as CAPIO_DIR.
func (d *Driver) createMultiNodeVolume(ctx context.Context, name string, capacity int64, params *volumeParams) (*csi.CreateVolumeResponse, error) {
	clients, namespace, err := d.getK8sClients()
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to build Kubernetes clients: %v", err)
	}

	id := multiNodeIDPrefix + name
	if err := createMPIJobVolume(ctx, clients, namespace, id, d.mpiImage, d.mpiImagePullSecret, params); err != nil {
		return nil, status.Errorf(codes.Internal, "failed to provision multi-node volume: %v", err)
	}

	return &csi.CreateVolumeResponse{
		Volume: &csi.Volume{
			VolumeId:      id,
			CapacityBytes: capacity,
			VolumeContext: map[string]string{
				"capioDir":     params.capioDir,
				"workflowName": params.workflowName,
				"multiNode":    "true",
			},
		},
	}, nil
}

// DeleteVolume tears down what CreateVolume set up: stops the volume's
// capio_server (or MPIJob, for a multiNodeIDPrefix id - see that constant's
// comment for why the two paths are told apart this way) and removes its
// data.
func (d *Driver) DeleteVolume(ctx context.Context, req *csi.DeleteVolumeRequest) (*csi.DeleteVolumeResponse, error) {
	id := req.GetVolumeId()
	if id == "" {
		return nil, status.Error(codes.InvalidArgument, "volume id is required")
	}

	if strings.HasPrefix(id, multiNodeIDPrefix) {
		clients, namespace, err := d.getK8sClients()
		if err != nil {
			return nil, status.Errorf(codes.Internal, "failed to build Kubernetes clients: %v", err)
		}
		if err := deleteMPIJobVolume(ctx, clients, namespace, id); err != nil {
			return nil, status.Errorf(codes.Internal, "failed to delete multi-node volume: %v", err)
		}
		return &csi.DeleteVolumeResponse{}, nil
	}

	if err := d.volumes.delete(id); err != nil {
		return nil, status.Errorf(codes.Internal, "failed to delete volume: %v", err)
	}
	return &csi.DeleteVolumeResponse{}, nil
}

// validateReadWriteMany rejects any CreateVolume request that doesn't ask
// exclusively for the ReadWriteMany access mode.
func validateReadWriteMany(caps []*csi.VolumeCapability) error {
	if len(caps) == 0 {
		return status.Error(codes.InvalidArgument, "volume capabilities are required")
	}
	for _, c := range caps {
		if c.GetAccessMode().GetMode() != csi.VolumeCapability_AccessMode_MULTI_NODE_MULTI_WRITER {
			return status.Errorf(codes.InvalidArgument,
				"unsupported access mode %v: this driver only supports ReadWriteMany", c.GetAccessMode().GetMode())
		}
	}
	return nil
}
