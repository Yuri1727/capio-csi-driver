package capio

import (
	"os"
	"path/filepath"

	"golang.org/x/sys/unix"
)

// isMountPoint reports whether path is the root of a separate mount, by
// comparing its device id with its parent's: a bind mount changes the
// device id seen at that path. Returns false (not an error) if path
// doesn't exist yet, since that's the normal state before the first
// NodePublishVolume call.
func isMountPoint(path string) (bool, error) {
	pathStat, err := os.Stat(path)
	if os.IsNotExist(err) {
		return false, nil
	}
	if err != nil {
		return false, err
	}

	parentStat, err := os.Stat(filepath.Dir(path))
	if err != nil {
		return false, err
	}

	pathSys, ok1 := pathStat.Sys().(*unix.Stat_t)
	parentSys, ok2 := parentStat.Sys().(*unix.Stat_t)
	if !ok1 || !ok2 {
		return false, nil
	}

	return pathSys.Dev != parentSys.Dev, nil
}

// bindMount bind-mounts source onto target, creating target first if
// needed. If readonly is set, the mount is remounted read-only right after
// (the kernel doesn't support read-only bind mounts in a single syscall).
func bindMount(source, target string, readonly bool) error {
	if err := os.MkdirAll(target, 0o750); err != nil {
		return err
	}

	if err := unix.Mount(source, target, "", unix.MS_BIND, ""); err != nil {
		return err
	}

	if readonly {
		if err := unix.Mount("", target, "", unix.MS_BIND|unix.MS_REMOUNT|unix.MS_RDONLY, ""); err != nil {
			_ = unix.Unmount(target, 0)
			return err
		}
	}

	return nil
}

// unmount detaches the bind mount at target.
func unmount(target string) error {
	return unix.Unmount(target, 0)
}
