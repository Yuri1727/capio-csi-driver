package capio

import (
	"encoding/json"
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
// volume: its directory on disk and the capio_server process currently
// serving it.
type volume struct {
	id           string
	dir          string // <root>/<id>: config, logs, capio_server's working directory
	capioDir     string // CAPIO_DIR of the server and its clients (StorageClass capioDir)
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
	if err := os.MkdirAll(volDir, 0o750); err != nil {
		return nil, fmt.Errorf("failed to create volume directory %q: %w", volDir, err)
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

	cmd, err := m.startServer(volDir, params.capioDir, cfgPath, logDir)
	if err != nil {
		return nil, err
	}

	v := &volume{
		id:           id,
		dir:          volDir,
		capioDir:     params.capioDir,
		workflowName: params.workflowName,
		capacity:     capacity,
		cmd:          cmd,
	}
	m.volumes[id] = v
	klog.Infof("volume %q provisioned: capio_server pid=%d dir=%s CAPIO_DIR=%s", id, cmd.Process.Pid, volDir, params.capioDir)
	return v, nil
}

// startServer starts capio_server for a volume, with CAPIO_DIR set to
// capioDir, the same string every client of the volume uses.
//
// Several volumes may share a capioDir, all in this one container: that is
// fine because capio_server keeps the files it serves in memory and keeps
// workflows apart by name in /dev/shm, so CAPIO_DIR only has to exist, not to
// hold anything (confirmed empirically: two servers with different
// workflows and the same CAPIO_DIR and working directory served the right
// data to each client, and nothing was ever written under CAPIO_DIR). An
// earlier version gave every server a private bind mount at capioDir in its
// own mount namespace, which needed this container to run privileged. The
// working directory is volDir, so the files_location_<host>.txt file CAPIO
// writes there stays with the volume and goes away with it.
func (m *volumeManager) startServer(volDir, capioDir, cfgPath, logDir string) (*exec.Cmd, error) {
	if err := os.MkdirAll(capioDir, 0o750); err != nil {
		return nil, fmt.Errorf("failed to create CAPIO_DIR %q: %w", capioDir, err)
	}

	logFile, err := os.Create(filepath.Join(logDir, "capio_server.out"))
	if err != nil {
		return nil, fmt.Errorf("failed to create capio_server log file: %w", err)
	}

	// capio_server's own stdout/stderr go to capio_server.out above; the
	// --log-dir option it used to accept was removed upstream.
	cmd := exec.Command(m.serverBinary, "-c", cfgPath)
	cmd.Dir = volDir
	cmd.Env = append(os.Environ(), "CAPIO_DIR="+capioDir)
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
		return m.deleteOrphan(id)
	}

	if v.cmd != nil && v.cmd.Process != nil {
		stopServer(id, v.cmd)
	}

	// capio_server has exited above, so nothing is using the workflow's
	// segments any more. After a clean SIGTERM shutdown this normally finds
	// nothing left to remove; it catches what a killed server, or clients
	// that exited without cleaning up, left behind, which would otherwise
	// stop a new volume with the same workflow name from starting.
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

// deleteOrphan cleans up after a volume this Controller doesn't know about:
// one created before the Controller restarted, whose capio_server died with
// the previous Controller process without cleaning up. If the volume's
// directory is on this node (the Controller restarted on the same node), the
// workflow name is read back from its workflow.json, the workflow's shared
// memory is removed and so is the directory. The shared memory is left alone
// if a running volume uses the same workflow name, since it then belongs to
// that volume's server. If the directory isn't here (the Controller moved to
// another node), there is nothing this instance can reach, and deleting the
// volume still succeeds, as CSI requires for an unknown id.
//
// Called with m.mu held.
func (m *volumeManager) deleteOrphan(id string) error {
	volDir := filepath.Join(m.rootDir, id)
	cfgBytes, err := os.ReadFile(filepath.Join(volDir, "workflow.json"))
	if err != nil {
		if !os.IsNotExist(err) {
			klog.Warningf("unknown volume %q: failed to read its workflow config: %v", id, err)
		}
		return nil
	}

	var cfg workflowConfig
	if err := json.Unmarshal(cfgBytes, &cfg); err != nil {
		klog.Warningf("unknown volume %q: failed to parse its workflow config: %v", id, err)
	} else if m.workflowInUse(cfg.Name) {
		klog.Infof("unknown volume %q: workflow %q belongs to a running volume, leaving its shared memory alone", id, cfg.Name)
	} else if err := cleanWorkflowSHM(cfg.Name); err != nil {
		klog.Warningf("failed to clean up SHM for workflow %q: %v", cfg.Name, err)
	}

	if err := os.RemoveAll(volDir); err != nil {
		return fmt.Errorf("failed to remove volume directory %q: %w", volDir, err)
	}
	klog.Infof("unknown volume %q: cleaned up what the previous Controller left behind", id)
	return nil
}

// workflowInUse reports whether a volume this Controller is running uses
// workflowName. Called with m.mu held.
func (m *volumeManager) workflowInUse(workflowName string) bool {
	for _, v := range m.volumes {
		if v.workflowName == workflowName {
			return true
		}
	}
	return false
}
