//go:build linux || darwin

package commands

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/adrg/xdg"
)

// daemonChildEnv marks the re-exec'd child so it runs the foreground mount loop
// instead of forking again.
const daemonChildEnv = "GMOUNTIE_DAEMON_CHILD"

// readyFD is the inherited pipe fd (after the standard three) the child writes
// to once the mount is up.
const readyFD = 3

// Ready-pipe protocol. The child writes exactly one of these to readyFD and
// closes it: daemonReadyMsg on success, or daemonErrPrefix + message on a
// pre-ready failure, so the parent can report the real cause instead of a
// generic timeout.
const (
	daemonReadyMsg  = "ready"
	daemonErrPrefix = "err:"
)

var errReady = errors.New("daemon child exited before signalling mount ready")

// daemonizer is the seam: the parent side of --daemon. Faked in tests.
type daemonizer interface {
	spawnAndAwaitReady(childArgs []string) error
}

// buildDaemonChildArgs returns args for the child with any --daemon / --daemon=...
// flag removed (so the child runs in the foreground).
func buildDaemonChildArgs(args []string) []string {
	out := make([]string, 0, len(args))
	for _, a := range args {
		if a == "--daemon" || strings.HasPrefix(a, "--daemon=") {
			continue
		}
		out = append(out, a)
	}
	return out
}

// daemonize runs the parent side: spawn the child, wait until it signals the
// mount is ready, then return (the caller exits 0). Errors if the child dies
// first. fullArgs is os.Args (argv[0] is dropped).
func daemonize(d daemonizer, fullArgs []string) error {
	return d.spawnAndAwaitReady(buildDaemonChildArgs(fullArgs[1:]))
}

// isDaemonChild reports whether this process is the re-exec'd child.
func isDaemonChild() bool { return os.Getenv(daemonChildEnv) == "1" }

// daemonLogPath is where the detached child's stdout/stderr go.
func daemonLogPath() string {
	return filepath.Join(xdg.StateHome, "gmountie", "mount-daemon.log")
}

// execDaemonizer is the production seam: re-execs the current binary detached
// (new session), redirecting output to a log file, and waits for the readyFD
// pipe to report success.
type execDaemonizer struct{}

func (execDaemonizer) spawnAndAwaitReady(childArgs []string) error {
	logPath := daemonLogPath()
	if err := os.MkdirAll(filepath.Dir(logPath), 0o755); err != nil {
		return fmt.Errorf("create daemon log dir: %w", err)
	}
	logFile, err := os.OpenFile(logPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o600)
	if err != nil {
		return fmt.Errorf("open daemon log %s: %w", logPath, err)
	}
	defer func() { _ = logFile.Close() }()

	pr, pw, err := os.Pipe()
	if err != nil {
		return fmt.Errorf("create ready pipe: %w", err)
	}
	defer func() { _ = pr.Close() }()

	self, err := os.Executable()
	if err != nil {
		return fmt.Errorf("resolve executable: %w", err)
	}
	// CommandContext (not Command) per the project's noctx lint; the daemon
	// parent is short-lived, so a background context is appropriate.
	cmd := exec.CommandContext(context.Background(), self, childArgs...)
	cmd.Env = append(os.Environ(), daemonChildEnv+"=1")
	cmd.Stdout = logFile
	cmd.Stderr = logFile
	cmd.ExtraFiles = []*os.File{pw} // becomes fd 3 in the child
	cmd.SysProcAttr = &syscall.SysProcAttr{Setsid: true}

	if err := cmd.Start(); err != nil {
		_ = pw.Close()
		return fmt.Errorf("start daemon child: %w", err)
	}
	_ = pw.Close() // parent keeps only the read end

	// Bound the wait so --daemon detaches promptly even if the child's first
	// mount RPC hangs; the detached child keeps running in the background
	// regardless of whether the parent is still listening.
	const daemonReadyTimeout = 30 * time.Second
	_ = pr.SetReadDeadline(time.Now().Add(daemonReadyTimeout))

	// The child writes a short status to the pipe and closes it: "ready" on a
	// successful mount, or "err:<message>" when the mount fails before it comes
	// up (e.g. the cache lock — "volume already mounted"). Reading to EOF lets
	// us surface the real reason instead of a generic timeout. An empty read
	// (the child crashed or hung past the deadline) falls back to the log.
	data, _ := io.ReadAll(pr)
	if err := interpretReadyMsg(string(data), logPath); err != nil {
		return err
	}
	fmt.Fprintf(os.Stderr, "gMountie: mounted in background (pid %d, logs: %s)\n", cmd.Process.Pid, logPath)
	return nil
}

// interpretReadyMsg maps the child's ready-pipe status to a result: nil on
// "ready", the child's propagated reason on "err:<message>", and the generic
// timed-out/crashed error otherwise (empty read = child died before signalling
// or blew past the deadline). Pure, so it's unit-testable without a re-exec.
func interpretReadyMsg(msg, logPath string) error {
	switch {
	case msg == daemonReadyMsg:
		return nil
	case strings.HasPrefix(msg, daemonErrPrefix):
		return fmt.Errorf("mount failed: %s (see %s)", strings.TrimPrefix(msg, daemonErrPrefix), logPath)
	default:
		return fmt.Errorf("%w (timed out or child exited; see %s)", errReady, logPath)
	}
}

// signalDaemonReady is called by the child after a successful mount to release
// the waiting parent. No-op when not a daemon child.
func signalDaemonReady() {
	writeReadyPipe(daemonReadyMsg)
}

// signalDaemonError is called by the child when the mount fails before it comes
// up, so the parent can report the real cause (e.g. "volume already mounted")
// instead of a generic "child exited" timeout. No-op when not a daemon child,
// or after signalDaemonReady already closed the pipe (the success path blocks
// until teardown, so this only fires on a pre-ready failure).
func signalDaemonError(err error) {
	if err == nil {
		return
	}
	writeReadyPipe(daemonErrPrefix + err.Error())
}

// writeReadyPipe writes one status string to readyFD and closes it. No-op when
// not a daemon child or the fd is unavailable.
func writeReadyPipe(msg string) {
	if !isDaemonChild() {
		return
	}
	f := os.NewFile(readyFD, "ready-pipe")
	if f == nil {
		return
	}
	_, _ = f.WriteString(msg)
	_ = f.Close()
}
