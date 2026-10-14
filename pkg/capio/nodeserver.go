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

// NodeGetCapabilities declares which Node RPCs this driver supports.
// STAGE_UNSTAGE_VOLUME makes kubelet call NodeStageVolume/NodeUnstageVolume
// once per (volume, node) - the first/last pod using the volume there -
// instead of only NodePublishVolume/NodeUnpublishVolume per pod. That's the
// hook used to clean up the workflow's leftover SHM segments on this node
// exactly once, instead of on every single pod's unpublish.
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

// NodeStageVolume creates this node's local anchor directory for the
// volume, at the staging path kubelet manages for us (one per node, shared
// across every pod on this node that mounts the volume).
//
// This directory does not need to hold or share any of CAPIO's actual data
// with wherever capio_server (local subprocess for a single-node volume, or
// an MPIJob worker pod for a multi-node one) keeps its own view of
// CAPIO_DIR - confirmed empirically: three separate pods on the same node,
// each with its own unrelated empty directory bind-mounted at /capio, wrote
// and read data through a fourth pod's capio_server correctly. All that
// matters is that CAPIO_DIR resolves to the same string ("/capio", see
// FixedMountPath) on every participant sharing the node's hostIPC/hostPID
// namespaces - the actual I/O goes over POSIX shared memory, never through
// this directory's real contents.
func (d *Driver) NodeStageVolume(ctx context.Context, req *csi.NodeStageVolumeRequest) (*csi.NodeStageVolumeResponse, error) {
	if req.GetVolumeId() == "" {
		return nil, status.Error(codes.InvalidArgument, "volume id is required")
	}
	stagingPath := req.GetStagingTargetPath()
	if stagingPath == "" {
		return nil, status.Error(codes.InvalidArgument, "staging target path is required")
	}

	if err := os.MkdirAll(stagingPath, 0o750); err != nil {
		return nil, status.Errorf(codes.Internal, "failed to create staging directory %q: %v", stagingPath, err)
	}

	// NodeUnstageVolumeRequest (unlike this request) carries no
	// VolumeContext, so workflowName - needed there to clean up this
	// workflow's SHM - is stashed in a sidecar file next to the staging
	// directory instead, the only piece of state that's guaranteed to still
	// be around by the time NodeUnstageVolume runs.
	if workflowName := req.GetVolumeContext()["workflowName"]; workflowName != "" {
		if err := os.WriteFile(workflowNameSidecarFile(stagingPath), []byte(workflowName), 0o640); err != nil {
			return nil, status.Errorf(codes.Internal, "failed to record workflow name for %q: %v", stagingPath, err)
		}
	}

	klog.Infof("volume %q staged at %s", req.GetVolumeId(), stagingPath)
	return &csi.NodeStageVolumeResponse{}, nil
}

// workflowNameSidecarFile is where NodeStageVolume records the volume's
// CAPIO workflow name, next to (not inside, to keep it out of what gets
// bind-mounted into pods) the staging directory itself, so NodeUnstageVolume
// can read it back despite NodeUnstageVolumeRequest not carrying
// VolumeContext.
func workflowNameSidecarFile(stagingPath string) string {
	return stagingPath + ".workflow-name"
}

// NodeUnstageVolume removes the staging directory NodeStageVolume created
// and, since this is called exactly once per node when the last pod using
// the volume there has gone (not once per pod), also cleans up the
// workflow's leftover SHM segments on this node (see cleanWorkflowSHM).
// Requires hostIPC on this DaemonSet so /dev/shm here is genuinely the
// node's, not this pod's own isolated one. DeleteVolume (single-node) and
// the worker pods' preStop hook (multi-node) run the same cleanup, for the
// nodes where no client pod ever mounted the volume.
func (d *Driver) NodeUnstageVolume(ctx context.Context, req *csi.NodeUnstageVolumeRequest) (*csi.NodeUnstageVolumeResponse, error) {
	stagingPath := req.GetStagingTargetPath()
	if stagingPath == "" {
		return nil, status.Error(codes.InvalidArgument, "staging target path is required")
	}

	sidecarFile := workflowNameSidecarFile(stagingPath)
	if workflowNameBytes, err := os.ReadFile(sidecarFile); err == nil {
		if workflowName := string(workflowNameBytes); workflowName != "" {
			if err := cleanWorkflowSHM(workflowName); err != nil {
				// Not fatal: a failed best-effort cleanup shouldn't block volume
				// teardown, it just means next time's CreateVolume for a workflow
				// of the same name might hit "SHM canary already exists" and need
				// a manual cleanup, same as before this existed.
				klog.Warningf("failed to clean up SHM for workflow %q: %v", workflowName, err)
			}
		}
	} else if !os.IsNotExist(err) {
		klog.Warningf("failed to read workflow name sidecar %q: %v", sidecarFile, err)
	}
	_ = os.Remove(sidecarFile)

	if err := os.RemoveAll(stagingPath); err != nil {
		return nil, status.Errorf(codes.Internal, "failed to remove staging directory %q: %v", stagingPath, err)
	}

	klog.Infof("volume %q unstaged: %s", req.GetVolumeId(), stagingPath)
	return &csi.NodeUnstageVolumeResponse{}, nil
}

// NodePublishVolume bind-mounts the node-local staging directory
// NodeStageVolume created onto the path kubelet gives us for this specific
// pod. Injecting LD_PRELOAD/CAPIO_* into the workload container itself is
// left to the pod manifest, as decided when scoping the project: this
// driver only takes care of exposing the directory.
func (d *Driver) NodePublishVolume(ctx context.Context, req *csi.NodePublishVolumeRequest) (*csi.NodePublishVolumeResponse, error) {
	if req.GetVolumeId() == "" {
		return nil, status.Error(codes.InvalidArgument, "volume id is required")
	}
	targetPath := req.GetTargetPath()
	if targetPath == "" {
		return nil, status.Error(codes.InvalidArgument, "target path is required")
	}
	stagingPath := req.GetStagingTargetPath()
	if stagingPath == "" {
		return nil, status.Error(codes.InvalidArgument, "staging target path is required")
	}

	mounts, err := mountCount(targetPath)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to check target path %q: %v", targetPath, err)
	}
	if mounts > 0 {
		// Already published: NodePublishVolume must be idempotent.
		return &csi.NodePublishVolumeResponse{}, nil
	}

	if err := bindMount(stagingPath, targetPath, req.GetReadonly()); err != nil {
		return nil, status.Errorf(codes.Internal, "failed to bind mount %q onto %q: %v", stagingPath, targetPath, err)
	}

	klog.Infof("volume %q published: %s -> %s", req.GetVolumeId(), stagingPath, targetPath)
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
