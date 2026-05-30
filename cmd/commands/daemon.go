//go:build linux

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

	"github.com/adrg/xdg"
)

// daemonChildEnv marks the re-exec'd child so it runs the foreground mount loop
// instead of forking again.
const daemonChildEnv = "GMOUNTIE_DAEMON_CHILD"

// readyFD is the inherited pipe fd (after the standard three) the child writes
// to once the mount is up.
const readyFD = 3

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

	buf := make([]byte, 5)
	n, _ := io.ReadFull(pr, buf) // child writes "ready"
	if n < 5 {
		return fmt.Errorf("%w (see %s)", errReady, logPath)
	}
	fmt.Fprintf(os.Stderr, "gMountie: mounted in background (pid %d, logs: %s)\n", cmd.Process.Pid, logPath)
	return nil
}

// signalDaemonReady is called by the child after a successful mount to release
// the waiting parent. No-op when not a daemon child.
func signalDaemonReady() {
	if !isDaemonChild() {
		return
	}
	f := os.NewFile(readyFD, "ready-pipe")
	if f == nil {
		return
	}
	_, _ = f.WriteString("ready")
	_ = f.Close()
}
