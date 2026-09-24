package capio

import (
	"fmt"
	"os"
	"os/exec"
	"syscall"

	"golang.org/x/sys/unix"
)

// FixedMountPath is the path every capio_server instance run by this driver
// uses as CAPIO_DIR, and the path every pod using a CAPIO-backed volume is
// required to mount it at. CAPIO requires the exact same CAPIO_DIR string on
// both sides of a client/server pair (confirmed against the project's own
// Docker example, where every participant is launched with an identical
// CAPIO_DIR); since a pod's mount path is chosen freely in its own manifest
// and can't be made to match wherever the Controller happens to keep the
// volume's real data, the server side is instead given a private view of
// that data at this fixed path.
const FixedMountPath = "/capio"

// ServerWrapperArg is the hidden argv[1] that tells main() to run
// RunServerInNamespace instead of the normal driver.
const ServerWrapperArg = "__run_capio_server"

// execServerInNamespace re-executes the current binary in a private mount
// namespace, where it bind-mounts dataDir onto FixedMountPath before
// exec-ing the real capio_server. The calling process must have
// CAP_SYS_ADMIN (the Controller runs privileged for this reason).
func execServerInNamespace(serverBinary, dataDir string, serverArgs []string) (*exec.Cmd, error) {
	// /proc is mounted noexec in the container, so the /proc/self/exe
	// symlink itself can't be the exec target; os.Executable() resolves it
	// to the real path of the binary on the (executable) root filesystem.
	self, err := os.Executable()
	if err != nil {
		return nil, fmt.Errorf("failed to resolve own executable path: %w", err)
	}

	args := append([]string{ServerWrapperArg, serverBinary, dataDir}, serverArgs...)

	cmd := exec.Command(self, args...)
	cmd.SysProcAttr = &syscall.SysProcAttr{
		Unshareflags: syscall.CLONE_NEWNS,
	}
	return cmd, nil
}

// RunServerInNamespace is the body executed by the re-exec above: it runs
// inside the freshly unshared mount namespace, sets up the bind mount, and
// then replaces itself with capio_server. Called from main() when argv[1]
// is ServerWrapperArg.
func RunServerInNamespace(args []string) error {
	if len(args) < 2 {
		return fmt.Errorf("usage: %s <capio_server binary> <data dir> [capio_server args...]", ServerWrapperArg)
	}
	serverBinary, dataDir, serverArgs := args[0], args[1], args[2:]

	// The new mount namespace starts as a copy of the parent's, with mounts
	// still marked shared: without making them private first, our bind
	// mount below would propagate out to every other namespace sharing the
	// same peer group, defeating the whole point.
	if err := unix.Mount("", "/", "", unix.MS_PRIVATE|unix.MS_REC, ""); err != nil {
		return fmt.Errorf("failed to make mounts private: %w", err)
	}

	if err := os.MkdirAll(FixedMountPath, 0o750); err != nil {
		return fmt.Errorf("failed to create %q: %w", FixedMountPath, err)
	}
	if err := unix.Mount(dataDir, FixedMountPath, "", unix.MS_BIND, ""); err != nil {
		return fmt.Errorf("failed to bind mount %q onto %q: %w", dataDir, FixedMountPath, err)
	}

	env := append(os.Environ(), "CAPIO_DIR="+FixedMountPath)
	argv := append([]string{serverBinary}, serverArgs...)
	return syscall.Exec(serverBinary, argv, env)
}
