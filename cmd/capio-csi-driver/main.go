// Command capio-csi-driver starts the CSI driver backed by CAPIO.
package main

import (
	"flag"
	"fmt"
	"os"

	"capio-csi-driver/pkg/capio"

	"k8s.io/klog/v2"
)

var (
	endpoint           = flag.String("endpoint", "unix:///csi/csi.sock", "CSI endpoint, the socket kubelet and sidecars connect to")
	nodeID             = flag.String("nodeid", "", "node this driver instance runs on (required)")
	capioRoot          = flag.String("capio-root", "/var/lib/capio/volumes", "directory where per-volume CAPIO data and config live")
	capioServerBin     = flag.String("capio-server-bin", "/usr/local/bin/capio_server", "path to the capio_server binary")
	mpiImage           = flag.String("mpi-image", "", "image for MPIJob launcher/worker pods on multi-node (nodeCount>1) volumes - normally the same image this driver itself runs as")
	mpiImagePullSecret = flag.String("mpi-image-pull-secret", "", "imagePullSecret name for MPIJob pods, if --mpi-image is private")
	showVer            = flag.Bool("version", false, "print the driver version and exit")
)

func main() {
	// Hidden re-exec entrypoint used internally to start capio_server in a
	// private mount namespace (see pkg/capio/server_wrapper.go). Checked
	// before flag.Parse() since its arguments aren't driver flags.
	if len(os.Args) > 1 && os.Args[1] == capio.ServerWrapperArg {
		if err := capio.RunServerInNamespace(os.Args[2:]); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		return
	}
	// Hidden entrypoint run as the preStop hook of multi-node worker pods
	// (see pkg/capio/shm.go).
	if len(os.Args) > 1 && os.Args[1] == capio.CleanSHMArg {
		if err := capio.RunCleanSHM(os.Args[2:]); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		return
	}

	klog.InitFlags(nil)
	flag.Parse()

	if *showVer {
		fmt.Println(capio.DriverName, capio.DriverVersion)
		return
	}

	if *nodeID == "" {
		klog.Error("--nodeid is required")
		os.Exit(1)
	}

	driver := capio.NewDriver(*nodeID, *endpoint, *capioRoot, *capioServerBin, *mpiImage, *mpiImagePullSecret)
	if err := driver.Run(); err != nil {
		klog.Errorf("driver stopped with error: %v", err)
		os.Exit(1)
	}
}
