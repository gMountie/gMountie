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

// passwordFD is the inherited pipe fd the parent uses to hand the resolved
// basic-auth password to the detached child OUT OF BAND. The secret must not
// ride the child's environment: anything in the execve env block stays
// readable in /proc/<pid>/environ for the whole mount lifetime (CQ-L2). The
// parent writes the secret and closes the pipe (EOF); the child reads it once,
// early, before resolving auth.
const passwordFD = 4

// Ready-pipe protocol. The child writes exactly one of these to readyFD and
// closes it: daemonReadyMsg on success, or daemonErrPrefix + message on a
// pre-ready failure, so the parent can report the real cause instead of a
// generic timeout.
const (
	daemonReadyMsg  = "ready"
	daemonErrPrefix = "err:"
)

var errReady = errors.New("daemon child exited before signalling mount ready")

// daemonizer is the seam: the parent side of --daemon. Faked in tests. password
// is the resolved basic-auth secret to hand the child over the password pipe
// (empty for mtls / no-password mounts — nothing is written then).
type daemonizer interface {
	spawnAndAwaitReady(childArgs []string, password string) error
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
// first. fullArgs is os.Args (argv[0] is dropped). password is handed to the
// child over the password pipe rather than the environment (CQ-L2).
func daemonize(d daemonizer, fullArgs []string, password string) error {
	return d.spawnAndAwaitReady(buildDaemonChildArgs(fullArgs[1:]), password)
}

// isDaemonChild reports whether this process is the re-exec'd child.
func isDaemonChild() bool { return os.Getenv(daemonChildEnv) == "1" }

// readDaemonPassword reads the password the parent wrote to passwordFD and
// returns it. Pure on its input fd so it's unit-testable with a plain pipe. An
// unavailable fd (not a daemon child, or no secret sent) yields "".
func readDaemonPassword(f *os.File) string {
	if f == nil {
		return ""
	}
	defer func() { _ = f.Close() }()
	b, err := io.ReadAll(f)
	if err != nil {
		return ""
	}
	return string(b)
}

// applyDaemonPassword, in the detached child only, reads the secret the parent
// handed over passwordFD and exposes it to resolveAuth via GMOUNTIE_AUTH_PASSWORD.
//
// Setting it with os.Setenv in the CHILD is safe w.r.t. the leak this fixes:
// /proc/<pid>/environ is the kernel's snapshot of the execve env block and is
// NOT rewritten by a post-start os.Setenv — os.Getenv sees it, but it never
// appears in /proc/environ. The secret therefore reaches resolvePassword
// without the long-lived /proc exposure the env hand-off caused (CQ-L2).
//
// Caveat: a later auth.password_command subprocess (sh -c) would inherit this
// env var — but password_command, when set, is resolved BEFORE the env is read,
// so it never reaches that branch in practice. Accepted; left out of band of
// this fix.
func applyDaemonPassword() {
	if !isDaemonChild() {
		return
	}
	f := os.NewFile(passwordFD, "password-pipe")
	if pw := readDaemonPassword(f); pw != "" {
		_ = os.Setenv(passwordEnvVar, pw)
	}
}

// daemonLogPath is where the detached child's stdout/stderr go.
func daemonLogPath() string {
	return filepath.Join(xdg.StateHome, "gmountie", "mount-daemon.log")
}

// execDaemonizer is the production seam: re-execs the current binary detached
// (new session), redirecting output to a log file, and waits for the readyFD
// pipe to report success.
type execDaemonizer struct{}

func (execDaemonizer) spawnAndAwaitReady(childArgs []string, password string) error {
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

	// Password pipe (parent→child). The secret is written here and closed
	// rather than placed in cmd.Env, so it never lands in the child's
	// /proc/<pid>/environ for the mount lifetime (CQ-L2).
	pwR, pwW, err := os.Pipe()
	if err != nil {
		return fmt.Errorf("create password pipe: %w", err)
	}
	defer func() { _ = pwR.Close() }()

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
	// Order is load-bearing: index 0 → fd 3 (ready, child→parent), index 1 →
	// fd 4 (password, parent→child). See readyFD / passwordFD.
	cmd.ExtraFiles = []*os.File{pw, pwR}
	cmd.SysProcAttr = &syscall.SysProcAttr{Setsid: true}

	if err := cmd.Start(); err != nil {
		_ = pw.Close()
		_ = pwW.Close()
		return fmt.Errorf("start daemon child: %w", err)
	}
	_ = pw.Close()  // parent keeps only the ready read end
	_ = pwR.Close() // child owns the password read end now

	// Hand the secret to the child and close so its read sees EOF. A write
	// failure (child already gone) is non-fatal here — the ready-pipe wait
	// below surfaces the real outcome.
	if password != "" {
		_, _ = pwW.WriteString(password)
	}
	_ = pwW.Close()

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
