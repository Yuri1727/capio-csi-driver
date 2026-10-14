package capio

import (
	"fmt"
	"os"
	"strconv"
	"strings"

	"golang.org/x/sys/unix"
)

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

// mountCount returns how many mounts are stacked at exactly target, read
// from /proc/self/mountinfo (field 5 is the mount point, with spaces and
// other special characters octal-escaped by the kernel).
//
// This replaces an earlier check that compared the device id of target with
// its parent's. That check type-asserted os.FileInfo.Sys() to *unix.Stat_t,
// but on Linux it is a *syscall.Stat_t: the assertion always failed, so the
// check always answered "not mounted". NodeUnpublishVolume therefore never
// unmounted anything, every pod left its bind mount behind and stayed in
// Terminating. Comparing device ids would also be wrong on its own if the
// source were ever on the same filesystem as the target; mountinfo is not.
func mountCount(target string) (int, error) {
	data, err := os.ReadFile("/proc/self/mountinfo")
	if err != nil {
		return 0, err
	}
	n := 0
	for _, line := range strings.Split(string(data), "\n") {
		fields := strings.Fields(line)
		if len(fields) > 4 && unescapeMountinfo(fields[4]) == target {
			n++
		}
	}
	return n, nil
}

func unescapeMountinfo(s string) string {
	if !strings.Contains(s, `\`) {
		return s
	}
	var b strings.Builder
	for i := 0; i < len(s); i++ {
		if s[i] == '\\' && i+3 < len(s) {
			if v, err := strconv.ParseUint(s[i+1:i+4], 8, 8); err == nil {
				b.WriteByte(byte(v))
				i += 3
				continue
			}
		}
		b.WriteByte(s[i])
	}
	return b.String()
}

// maxUnmountAttempts bounds unmountAll: a target never legitimately holds
// more than a couple of stacked mounts, so hitting the limit means
// something else keeps re-mounting it and looping further won't help.
const maxUnmountAttempts = 10

// unmountAll detaches every mount stacked at target and returns how many
// it removed. Normally there is exactly one; looping until none is left
// also cleans up targets that were mounted more than once while
// NodePublishVolume's idempotency check was broken (see mountCount).
func unmountAll(target string) (int, error) {
	removed := 0
	for removed < maxUnmountAttempts {
		n, err := mountCount(target)
		if err != nil {
			return removed, err
		}
		if n == 0 {
			return removed, nil
		}
		if err := unix.Unmount(target, 0); err != nil {
			return removed, err
		}
		removed++
	}
	return removed, fmt.Errorf("%q still mounted after %d unmounts", target, removed)
}
