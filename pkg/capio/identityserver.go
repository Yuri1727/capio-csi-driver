package capio

import (
	"context"

	"github.com/container-storage-interface/spec/lib/go/csi"
)

// GetPluginInfo tells Kubernetes which driver this is and which version is
// running. It's the very first call made against any CSI driver.
func (d *Driver) GetPluginInfo(ctx context.Context, req *csi.GetPluginInfoRequest) (*csi.GetPluginInfoResponse, error) {
	return &csi.GetPluginInfoResponse{
		Name:          d.name,
		VendorVersion: d.version,
	}, nil
}

// GetPluginCapabilities declares which optional CSI features this driver
// supports. CONTROLLER_SERVICE tells Kubernetes it can talk to our
// Controller service at all; the other capabilities will be added as the
// corresponding RPCs are implemented.
func (d *Driver) GetPluginCapabilities(ctx context.Context, req *csi.GetPluginCapabilitiesRequest) (*csi.GetPluginCapabilitiesResponse, error) {
	return &csi.GetPluginCapabilitiesResponse{
		Capabilities: []*csi.PluginCapability{
			{
				Type: &csi.PluginCapability_Service_{
					Service: &csi.PluginCapability_Service{
						Type: csi.PluginCapability_Service_CONTROLLER_SERVICE,
					},
				},
			},
		},
	}, nil
}

// Probe is a liveness check: kubelet and the external sidecar containers
// call it repeatedly to know whether the driver is ready to serve requests.
func (d *Driver) Probe(ctx context.Context, req *csi.ProbeRequest) (*csi.ProbeResponse, error) {
	return &csi.ProbeResponse{}, nil
}
