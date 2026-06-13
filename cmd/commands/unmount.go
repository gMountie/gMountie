//go:build linux || darwin

package commands

import (
	"context"
	"fmt"
	"io"
	"os"
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

// unmountOutcome is how a signalled mount finished tearing down.
type unmountOutcome int

const (
	// outcomeProcessGone: the mount process exited — fully torn down and clean.
	outcomeProcessGone unmountOutcome = iota
	// outcomeDetached: the mountpoint is no longer mounted (lazy MNT_DETACH took
	// effect) but the process is still draining open fds in the background. The
	// path is already gone from the user's view; the process exits when the fds
	// close and removes its own state record then.
	outcomeDetached
	// outcomeBusy: still mounted after the timeout — genuinely stuck.
	outcomeBusy
)

// waitMountExit polls a signalled mount until it has torn down. It returns as
// soon as either the process exits (clean) or the mountpoint is *confidently*
// detached from the namespace (lazy unmount of a busy mount — the path is gone
// even though the process lingers to drain open fds). An indeterminate stat
// (a hung mount) is not treated as detached; only a mount still attached after
// the timeout is reported busy. Polling the mountpoint (not just the process)
// is what makes a busy unmount succeed promptly instead of a 10 s false error.
func waitMountExit(pid int, mountpoint string) unmountOutcome {
	deadline := time.Now().Add(unmountWaitTimeout)
	for {
		if !processAlive(pid) {
			return outcomeProcessGone
		}
		if checkMounted(mountpoint) == mountNo {
			return outcomeDetached
		}
		if !time.Now().Before(deadline) {
			return outcomeBusy
		}
		time.Sleep(unmountPollInterval)
	}
}

// mountCheck is the tri-state result of checkMounted.
type mountCheck int

const (
	mountUnknown mountCheck = iota // stat failed — can't tell (e.g. a hung mount)
	mountYes                       // confidently a mount point
	mountNo                        // confidently not a mount point
)

// checkMounted reports whether path is a mount point by comparing its device
// number to its parent's — a mount point sits on a different device than the
// directory it covers. Returns mountUnknown on any stat error so callers treat
// "can't tell" distinctly from "confidently not mounted": the idempotent
// fast-path only triggers on mountNo, while mountUnknown falls through to an
// explicit fuseUnmount (which treats a "not mounted" result as success). A var
// so tests can drive the mount-point check deterministically.
var checkMounted = func(path string) mountCheck {
	fi, err := os.Lstat(path)
	if err != nil {
		return mountUnknown
	}
	parent, err := os.Lstat(filepath.Dir(path))
	if err != nil {
		return mountUnknown
	}
	st, ok := fi.Sys().(*syscall.Stat_t)
	pst, ok2 := parent.Sys().(*syscall.Stat_t)
	if !ok || !ok2 {
		return mountUnknown
	}
	if st.Dev != pst.Dev {
		return mountYes
	}
	return mountNo
}

// notMountedHints mark a fusermount/umount failure that actually means the path
// was already unmounted — which makes unmount idempotent (a no-op success)
// rather than a hard error on a second invocation.
var notMountedHints = []string{
	"not mounted",
	"not currently mounted",
	"not found in /etc/mtab",
	"no mount point specified",
}

func looksNotMounted(s string) bool {
	s = strings.ToLower(s)
	for _, h := range notMountedHints {
		if strings.Contains(s, h) {
			return true
		}
	}
	return false
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
		// Already unmounted is success — keeps unmount idempotent rather than
		// failing a second invocation after a busy mount finally detached.
		if looksNotMounted(string(out)) {
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
// process whose kernel mount may linger) goes through fuseUnmount. It is
// idempotent — an already-unmounted path is a success. The state record is
// cleared once the path is gone, except when the owning process is still
// draining open files (it removes its own record on exit, so we don't race it).
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
		switch waitMountExit(st.PID, abs) {
		case outcomeProcessGone:
			_ = removeMountState(abs)
			_, _ = fmt.Fprintf(out, "Unmounted %s\n", abs)
			return nil
		case outcomeDetached:
			// The mountpoint is already detached; the process is draining open
			// files and exits when they close, removing its own record then —
			// so don't delete it here (a heartbeat from the still-live process
			// could otherwise race a re-create). End-state is correct: rc=0.
			_, _ = fmt.Fprintf(out, "Unmounted %s (a background process is releasing open files and will exit when they close)\n", abs)
			return nil
		default: // outcomeBusy
			return fmt.Errorf("mount process %d is still attached at %s after %s; the mountpoint may be stuck",
				st.PID, abs, unmountWaitTimeout)
		}
	}

	// No live gMountie process owns this path. If it is confidently no longer a
	// mount point, unmount is idempotent — report success instead of erroring.
	// An indeterminate stat falls through to fuseUnmount below.
	if checkMounted(abs) == mountNo {
		_ = removeMountState(abs)
		_, _ = fmt.Fprintf(out, "%s is not mounted\n", abs)
		return nil
	}

	if err := fuseUnmount(abs); err != nil {
		return fmt.Errorf("unmount %s: %w", abs, err)
	}
	_ = removeMountState(abs)
	_, _ = fmt.Fprintf(out, "Unmounted %s\n", abs)
	return nil
}
