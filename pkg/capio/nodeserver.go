package capio

import (
	"context"
	"os"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"k8s.io/klog/v2"
)

// NodeGetInfo tells Kubernetes the identity of the node this instance of
// the driver runs on. The Node service always runs as a DaemonSet, one pod
// per node, so each replica reports a different nodeID. The same name is
// reported as this node's topology segment, which is what lets a
// single-node volume's accessibility (see CreateVolume) be matched against
// nodes by the scheduler.
func (d *Driver) NodeGetInfo(ctx context.Context, req *csi.NodeGetInfoRequest) (*csi.NodeGetInfoResponse, error) {
	return &csi.NodeGetInfoResponse{
		NodeId: d.nodeID,
		AccessibleTopology: &csi.Topology{
			Segments: map[string]string{TopologyNodeKey: d.nodeID},
		},
	}, nil
}

// NodeGetCapabilities declares STAGE_UNSTAGE_VOLUME, although
// NodeStageVolume and NodeUnstageVolume do nothing: the driver needs no
// per-node preparation, but kubelet only cleans up its own per-volume staging
// directory (.../plugins/kubernetes.io/csi/<driver>/<hash>/globalmount) when
// the capability is declared. Since Kubernetes 1.34 kubelet no longer writes
// the metadata file it needs to remove that directory for drivers without
// it, so they leave one empty directory behind per volume and node.
//
// An earlier version used NodeUnstageVolume, called when the last pod using
// the volume on a node goes away, to clean up the workflow's shared memory.
// That was wrong: the segments belong to capio_server, which lives as long as
// the volume (single-node) or its MPIJob (multi-node), not as long as the pods
// using it. Cleaning them up there pulled the shared memory from under a
// still-running server, and every pod that used the volume afterwards hung.
// Cleanup now happens only where the server stops: DeleteVolume
// (single-node) and the worker pods' preStop hook (multi-node).
func (d *Driver) NodeGetCapabilities(ctx context.Context, req *csi.NodeGetCapabilitiesRequest) (*csi.NodeGetCapabilitiesResponse, error) {
	return &csi.NodeGetCapabilitiesResponse{
		Capabilities: []*csi.NodeServiceCapability{
			{
				Type: &csi.NodeServiceCapability_Rpc{
					Rpc: &csi.NodeServiceCapability_RPC{Type: csi.NodeServiceCapability_RPC_STAGE_UNSTAGE_VOLUME},
				},
			},
		},
	}, nil
}

// NodeStageVolume has nothing to prepare (see NodeGetCapabilities); kubelet
// creates the staging directory itself before calling it.
func (d *Driver) NodeStageVolume(ctx context.Context, req *csi.NodeStageVolumeRequest) (*csi.NodeStageVolumeResponse, error) {
	if req.GetVolumeId() == "" {
		return nil, status.Error(codes.InvalidArgument, "volume id is required")
	}
	if req.GetStagingTargetPath() == "" {
		return nil, status.Error(codes.InvalidArgument, "staging target path is required")
	}
	return &csi.NodeStageVolumeResponse{}, nil
}

// NodeUnstageVolume has nothing to undo (see NodeGetCapabilities); kubelet
// removes the staging directory itself afterwards. It must not touch the
// workflow's shared memory, which still belongs to a running capio_server.
func (d *Driver) NodeUnstageVolume(ctx context.Context, req *csi.NodeUnstageVolumeRequest) (*csi.NodeUnstageVolumeResponse, error) {
	if req.GetVolumeId() == "" {
		return nil, status.Error(codes.InvalidArgument, "volume id is required")
	}
	if req.GetStagingTargetPath() == "" {
		return nil, status.Error(codes.InvalidArgument, "staging target path is required")
	}
	return &csi.NodeUnstageVolumeResponse{}, nil
}

// NodePublishVolume gives the pod an empty directory at the path kubelet
// asks for. The directory doesn't need to hold or share any of CAPIO's data:
// clients reach capio_server over POSIX shared memory (hostIPC), and all
// that matters is that the pod sees the volume at the StorageClass capioDir
// and uses it as CAPIO_DIR - confirmed empirically: separate pods, each with
// its own unrelated empty directory there, wrote and read data through the
// same capio_server correctly. Injecting LD_PRELOAD/CAPIO_* into the
// workload container itself is left to the pod manifest.
//
// The directory is a small tmpfs private to this pod, not a bind mount of
// the staging directory: anything a process writes there without the CAPIO
// library is discarded on unmount, so NodeUnpublishVolume can always remove
// the target path afterwards.
func (d *Driver) NodePublishVolume(ctx context.Context, req *csi.NodePublishVolumeRequest) (*csi.NodePublishVolumeResponse, error) {
	if req.GetVolumeId() == "" {
		return nil, status.Error(codes.InvalidArgument, "volume id is required")
	}
	targetPath := req.GetTargetPath()
	if targetPath == "" {
		return nil, status.Error(codes.InvalidArgument, "target path is required")
	}

	mounts, err := mountCount(targetPath)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to check target path %q: %v", targetPath, err)
	}
	if mounts > 0 {
		// Already published: NodePublishVolume must be idempotent.
		return &csi.NodePublishVolumeResponse{}, nil
	}

	if err := tmpfsMount(targetPath, req.GetReadonly()); err != nil {
		return nil, status.Errorf(codes.Internal, "failed to mount %q: %v", targetPath, err)
	}

	klog.Infof("volume %q published: %s", req.GetVolumeId(), targetPath)
	return &csi.NodePublishVolumeResponse{}, nil
}

// NodeUnpublishVolume undoes what NodePublishVolume set up.
func (d *Driver) NodeUnpublishVolume(ctx context.Context, req *csi.NodeUnpublishVolumeRequest) (*csi.NodeUnpublishVolumeResponse, error) {
	targetPath := req.GetTargetPath()
	if targetPath == "" {
		return nil, status.Error(codes.InvalidArgument, "target path is required")
	}

	removed, err := unmountAll(targetPath)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to unmount %q: %v", targetPath, err)
	}

	// Reporting success while the directory is still there would make
	// kubelet stop retrying and leave the pod stuck in Terminating forever;
	// an error makes it call NodeUnpublishVolume again.
	if err := os.Remove(targetPath); err != nil && !os.IsNotExist(err) {
		return nil, status.Errorf(codes.Internal, "failed to remove target path %q after unmount: %v", targetPath, err)
	}

	klog.Infof("volume %q unpublished: %s (%d unmounts)", req.GetVolumeId(), targetPath, removed)
	return &csi.NodeUnpublishVolumeResponse{}, nil
}
