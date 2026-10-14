package capio

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"syscall"
	"time"

	"k8s.io/klog/v2"
)

// volume tracks everything the driver needs to know about one CAPIO-backed
// volume: where its data lives on disk and the capio_server process
// currently serving it.
type volume struct {
	id           string
	dir          string // CAPIO_DIR for this volume, e.g. <root>/<id>/data
	workflowName string
	capacity     int64
	cmd          *exec.Cmd
}

// volumeManager owns the lifecycle of every capio_server instance the
// Controller service starts, one per volume. It's the piece of state that
// makes CreateVolume/DeleteVolume actually provision something, instead of
// just bookkeeping.
//
// Scope: single node only for now. Each capio_server runs with the "none"
// backend, directly as a child process of the driver (no mpirun involved,
// since MPI is only needed to coordinate servers across multiple nodes).
type volumeManager struct {
	mu           sync.Mutex
	volumes      map[string]*volume
	rootDir      string // e.g. /var/lib/capio/volumes
	serverBinary string // path to the capio_server binary bundled in the driver image
}

func newVolumeManager(rootDir, serverBinary string) *volumeManager {
	return &volumeManager{
		volumes:      make(map[string]*volume),
		rootDir:      rootDir,
		serverBinary: serverBinary,
	}
}

// create provisions a new volume: writes its CAPIO-CL config and starts a
// dedicated capio_server for it. It's idempotent on volume id, as CSI
// requires CreateVolume to be safe to retry.
func (m *volumeManager) create(id string, capacity int64, params *volumeParams) (*volume, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if v, ok := m.volumes[id]; ok {
		return v, nil
	}

	volDir := filepath.Join(m.rootDir, id)
	dataDir := filepath.Join(volDir, "data")
	if err := os.MkdirAll(dataDir, 0o750); err != nil {
		return nil, fmt.Errorf("failed to create volume directory %q: %w", dataDir, err)
	}

	cfg := buildWorkflowConfig(params)
	cfgBytes, err := cfg.marshalIndent()
	if err != nil {
		return nil, fmt.Errorf("failed to marshal CAPIO-CL config: %w", err)
	}
	cfgPath := filepath.Join(volDir, "workflow.json")
	if err := os.WriteFile(cfgPath, cfgBytes, 0o640); err != nil {
		return nil, fmt.Errorf("failed to write CAPIO-CL config %q: %w", cfgPath, err)
	}

	logDir := filepath.Join(volDir, "logs")
	if err := os.MkdirAll(logDir, 0o750); err != nil {
		return nil, fmt.Errorf("failed to create log directory %q: %w", logDir, err)
	}

	cmd, err := m.startServer(dataDir, cfgPath, logDir)
	if err != nil {
		return nil, err
	}

	v := &volume{
		id:           id,
		dir:          dataDir,
		workflowName: params.workflowName,
		capacity:     capacity,
		cmd:          cmd,
	}
	m.volumes[id] = v
	klog.Infof("volume %q provisioned: capio_server pid=%d dir=%s", id, cmd.Process.Pid, dataDir)
	return v, nil
}

func (m *volumeManager) startServer(dataDir, cfgPath, logDir string) (*exec.Cmd, error) {
	logFile, err := os.Create(filepath.Join(logDir, "capio_server.out"))
	if err != nil {
		return nil, fmt.Errorf("failed to create capio_server log file: %w", err)
	}

	// capio_server needs CAPIO_DIR to be the exact same string every client
	// uses; since clients mount the volume at a path of their own choosing,
	// the server is instead given a private view of dataDir at
	// FixedMountPath (see server_wrapper.go).
	// capio_server's own stdout/stderr go to capio_server.out above; the
	// --log-dir option it used to accept was removed upstream.
	cmd, err := execServerInNamespace(m.serverBinary, dataDir, []string{"-c", cfgPath})
	if err != nil {
		logFile.Close()
		return nil, err
	}
	cmd.Stdout = logFile
	cmd.Stderr = logFile

	if err := cmd.Start(); err != nil {
		logFile.Close()
		return nil, fmt.Errorf("failed to start capio_server: %w", err)
	}

	// capio_server takes a moment to finish parsing the workflow config and
	// open its request queue; NodePublishVolume can be called right after
	// CreateVolume returns, so we wait briefly instead of racing it.
	time.Sleep(1 * time.Second)
	if cmd.Process == nil {
		return nil, fmt.Errorf("capio_server did not start")
	}

	return cmd, nil
}

// get returns the volume for id, if the driver knows about it.
func (m *volumeManager) get(id string) (*volume, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	v, ok := m.volumes[id]
	return v, ok
}

// delete stops the capio_server for id and removes its data directory. Also
// safe to retry: deleting an unknown id is a no-op, as CSI requires.
// serverStopTimeout is how long stopServer waits for capio_server to exit
// on SIGTERM before killing it.
const serverStopTimeout = 10 * time.Second

// stopServer asks capio_server to shut down with SIGTERM, which its signal
// handler answers by removing its own shared memory objects before exiting,
// and falls back to SIGKILL if it is still running after serverStopTimeout.
// The cleanup that follows in delete still runs either way: it is what
// removes the segments when the server could not clean up after itself.
func stopServer(id string, cmd *exec.Cmd) {
	done := make(chan struct{})
	go func() {
		_ = cmd.Wait()
		close(done)
	}()

	pid := cmd.Process.Pid
	if err := cmd.Process.Signal(syscall.SIGTERM); err != nil {
		klog.Warningf("failed to send SIGTERM to capio_server for volume %q (pid=%d): %v", id, pid, err)
	}
	select {
	case <-done:
		klog.Infof("capio_server for volume %q (pid=%d) stopped on SIGTERM", id, pid)
		return
	case <-time.After(serverStopTimeout):
	}

	klog.Warningf("capio_server for volume %q (pid=%d) still running after %s, sending SIGKILL", id, pid, serverStopTimeout)
	if err := cmd.Process.Kill(); err != nil {
		klog.Warningf("failed to kill capio_server for volume %q (pid=%d): %v", id, pid, err)
	}
	<-done
}

func (m *volumeManager) delete(id string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	v, ok := m.volumes[id]
	if !ok {
		return nil
	}

	if v.cmd != nil && v.cmd.Process != nil {
		stopServer(id, v.cmd)
	}

	// NodeUnstageVolume only cleans up when a pod mounted the volume on this
	// node; a volume deleted without ever being mounted would otherwise leave
	// its segments behind, and a new volume with the same workflow name could
	// not start its capio_server. capio_server has exited above, so nothing
	// is using them any more; after a clean SIGTERM shutdown this normally
	// finds nothing left to remove.
	if err := cleanWorkflowSHM(v.workflowName); err != nil {
		klog.Warningf("failed to clean up SHM for workflow %q: %v", v.workflowName, err)
	}

	volDir := filepath.Join(m.rootDir, id)
	if err := os.RemoveAll(volDir); err != nil {
		return fmt.Errorf("failed to remove volume directory %q: %w", volDir, err)
	}

	delete(m.volumes, id)
	klog.Infof("volume %q deleted", id)
	return nil
}
