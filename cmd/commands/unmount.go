//go:build linux || darwin

package commands

import (
	"context"
	"fmt"
	"io"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/spf13/cobra"
)

// signalMount asks a gMountie mount process to unmount and exit. SIGTERM hits
// the same handler as Ctrl-C, so the process unmounts cleanly via its deferred
// teardown. A var so tests can substitute it.
var signalMount = func(pid int) error { return syscall.Kill(pid, syscall.SIGTERM) }

// unmountWaitTimeout / unmountPollInterval bound waitMountExit's polling.
// Package vars so tests can shorten them.
var (
	unmountWaitTimeout  = 10 * time.Second
	unmountPollInterval = 50 * time.Millisecond
)

// waitMountExit polls until the signalled mount process has exited — the
// process removes its FUSE mount in its deferred teardown before exiting, so
// process-gone is the "mount actually detached" signal. Without this wait,
// `gmountie unmount` reported success the instant the signal was delivered,
// while the unmount was still in flight (or stuck).
func waitMountExit(pid int) error {
	deadline := time.Now().Add(unmountWaitTimeout)
	for time.Now().Before(deadline) {
		if !processAlive(pid) {
			return nil
		}
		time.Sleep(unmountPollInterval)
	}
	return fmt.Errorf("mount process %d is still running after %s; the mount may still be attached (is the mountpoint busy?)",
		pid, unmountWaitTimeout)
}

// fuseUnmount tears down a FUSE mount we don't own a process for, trying the
// FUSE-aware tools first and falling back to umount. A var so tests can
// substitute it.
var fuseUnmount = func(path string) error {
	var errs []string
	for _, c := range [][]string{
		{"fusermount3", "-u", path},
		{"fusermount", "-u", path},
		{"umount", path},
	} {
		if _, err := exec.LookPath(c[0]); err != nil {
			continue
		}
		// CommandContext per the project's noctx lint; this is a short-lived,
		// fire-and-wait invocation so a background context is appropriate.
		out, err := exec.CommandContext(context.Background(), c[0], c[1:]...).CombinedOutput()
		if err == nil {
			return nil
		}
		errs = append(errs, fmt.Sprintf("%s: %v: %s", c[0], err, strings.TrimSpace(string(out))))
	}
	if len(errs) == 0 {
		return fmt.Errorf("no unmount tool found (install fuse3 for fusermount3)")
	}
	return fmt.Errorf("%s", strings.Join(errs, "; "))
}

var unmountCmd = &cobra.Command{
	Use:   "unmount <mountpoint>",
	Short: "Unmount a gMountie volume",
	Long: "Unmounts a gMountie volume. For a mount this machine started (including\n" +
		"--daemon mounts), it signals the mount process to unmount cleanly. For any\n" +
		"other mount it falls back to fusermount3 -u / umount.\n\n" +
		"  gmountie unmount /mnt/shared",
	Args:    cobra.ExactArgs(1),
	Aliases: []string{"umount"},
	RunE: func(cmd *cobra.Command, args []string) error {
		return unmountTarget(cmd.OutOrStdout(), args[0])
	},
}

func init() {
	rootCmd.AddCommand(unmountCmd)
}

// unmountTarget resolves a mountpoint and tears it down: a live gMountie-managed
// mount is signalled to unmount itself; anything else (no record, or a dead
// process whose kernel mount may linger) goes through fuseUnmount. The state
// record is cleared either way.
func unmountTarget(out io.Writer, path string) error {
	abs := path
	if a, err := filepath.Abs(path); err == nil {
		abs = a
	}

	if st, ok, err := findMountState(abs); err != nil {
		return err
	} else if ok && processAlive(st.PID) {
		_, _ = fmt.Fprintf(out, "Stopping gMountie mount at %s (pid %d)…\n", abs, st.PID)
		if err := signalMount(st.PID); err != nil {
			return fmt.Errorf("signal mount process %d: %w", st.PID, err)
		}
		// Only report success once the mount process is actually gone — it
		// removes the FUSE mount in its teardown before exiting.
		if err := waitMountExit(st.PID); err != nil {
			return err
		}
		_ = removeMountState(abs)
		_, _ = fmt.Fprintf(out, "Unmounted %s\n", abs)
		return nil
	}

	if err := fuseUnmount(abs); err != nil {
		return fmt.Errorf("unmount %s: %w", abs, err)
	}
	_ = removeMountState(abs)
	_, _ = fmt.Fprintf(out, "Unmounted %s\n", abs)
	return nil
}
