//go:build linux || darwin

package commands

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"syscall"
	"time"

	"github.com/adrg/xdg"
)

// mountState is the on-disk record of one active gMountie mount. One JSON file
// per mount lives under mountStateDir(); the foreground mount process (which is
// also the --daemon child) writes it on a successful mount and removes it on
// exit. `gmountie status` lists them; `gmountie unmount` removes them.
type mountState struct {
	Mountpoint string    `json:"mountpoint"`
	Server     string    `json:"server"`
	Volume     string    `json:"volume"`
	PID        int       `json:"pid"`
	StartedAt  time.Time `json:"started_at"`
	// Healthy and HeartbeatAt are refreshed by the running mount process every
	// mountHeartbeatInterval so `gmountie status` (a separate process that can't
	// see the daemon's in-memory session state) can tell a working mount from a
	// zombie whose session is locked out. A zero HeartbeatAt means the record
	// predates heartbeating (treated as healthy/unknown). See mountStatus.
	Healthy     bool      `json:"healthy,omitempty"`
	HeartbeatAt time.Time `json:"heartbeat_at,omitempty"`
}

// mountHeartbeatInterval is how often the running mount refreshes its record's
// health. A var so tests can shorten it.
var mountHeartbeatInterval = 5 * time.Second

// mountHeartbeatStaleWindow is how long a mount whose process is alive but no
// longer refreshing its record can go before it's reported "stale" (the daemon
// is wedged). A multiple of the interval so an ordinary missed tick (scheduling
// jitter, a slow rewrite) doesn't flip the label.
func mountHeartbeatStaleWindow() time.Duration { return 3 * mountHeartbeatInterval }

// mountStatus is the health label `gmountie status` shows for a mount.
const (
	mountStatusActive   = "active"   // session healthy (or pre-heartbeat record)
	mountStatusDegraded = "degraded" // process alive but session not healthy (e.g. revoked/reconnecting)
	mountStatusStale    = "stale"    // process alive but heartbeat stopped (daemon wedged)
)

// mountStatusOf classifies a record's health at time now. A zero HeartbeatAt
// (record written before this feature, or before the first tick) is reported
// active rather than guessed-degraded.
func mountStatusOf(st mountState, now time.Time) string {
	if st.HeartbeatAt.IsZero() {
		return mountStatusActive
	}
	if now.Sub(st.HeartbeatAt) > mountHeartbeatStaleWindow() {
		return mountStatusStale
	}
	if !st.Healthy {
		return mountStatusDegraded
	}
	return mountStatusActive
}

// processAlive reports whether a process with the given pid exists. It's a var
// so tests can substitute a deterministic checker. signal 0 performs error
// checking without delivering a signal — ESRCH means the process is gone.
var processAlive = func(pid int) bool {
	if pid <= 0 {
		return false
	}
	err := syscall.Kill(pid, 0)
	return err == nil || errors.Is(err, syscall.EPERM)
}

// mountStateDir is $XDG_STATE_HOME/gmountie/mounts. Reload picks up an
// $XDG_STATE_HOME set after package init (mirrors defaultVolumeDir / tests).
func mountStateDir() string {
	xdg.Reload()
	return filepath.Join(xdg.StateHome, "gmountie", "mounts")
}

// stateFilePath maps a mountpoint to its state file. The abs path is hashed so
// the filename is fixed-length and filesystem-safe regardless of the path.
func stateFilePath(mountpoint string) string {
	abs := mountpoint
	if a, err := filepath.Abs(mountpoint); err == nil {
		abs = a
	}
	sum := sha256.Sum256([]byte(abs))
	return filepath.Join(mountStateDir(), hex.EncodeToString(sum[:])+".json")
}

// writeMountState persists a mount record, keyed by its mountpoint.
func writeMountState(st mountState) error {
	if abs, err := filepath.Abs(st.Mountpoint); err == nil {
		st.Mountpoint = abs
	}
	if st.StartedAt.IsZero() {
		st.StartedAt = time.Now()
	}
	if err := os.MkdirAll(mountStateDir(), 0o755); err != nil {
		return err
	}
	data, err := json.Marshal(st)
	if err != nil {
		return err
	}
	// Atomic write (temp + rename): the heartbeat rewrites this file every few
	// seconds while `status`/`unmount` read it concurrently. A plain truncate
	// + write could be observed half-written, making listMountStates skip the
	// record (its json.Unmarshal `continue`) and the mount flicker out of the
	// listing. Rename is atomic on the same filesystem.
	final := stateFilePath(st.Mountpoint)
	tmp, err := os.CreateTemp(mountStateDir(), ".mount-*.json.tmp")
	if err != nil {
		return err
	}
	tmpName := tmp.Name()
	if _, err := tmp.Write(data); err != nil {
		_ = tmp.Close()
		_ = os.Remove(tmpName)
		return err
	}
	if err := tmp.Close(); err != nil {
		_ = os.Remove(tmpName)
		return err
	}
	if err := os.Rename(tmpName, final); err != nil {
		_ = os.Remove(tmpName)
		return err
	}
	return nil
}

// removeMountState deletes the record for a mountpoint. A missing file is not
// an error — the mount may already be gone.
func removeMountState(mountpoint string) error {
	if err := os.Remove(stateFilePath(mountpoint)); err != nil && !errors.Is(err, fs.ErrNotExist) {
		return err
	}
	return nil
}

// findMountState returns the live mount record for a mountpoint, if any.
func findMountState(mountpoint string) (mountState, bool, error) {
	states, err := listMountStates()
	if err != nil {
		return mountState{}, false, err
	}
	target := mountpoint
	if abs, err := filepath.Abs(mountpoint); err == nil {
		target = abs
	}
	for _, st := range states {
		if st.Mountpoint == target {
			return st, true, nil
		}
	}
	return mountState{}, false, nil
}

// listMountStates returns the live mounts, sorted by mountpoint. Records whose
// process is no longer alive are pruned from disk (a mount killed with SIGKILL
// leaves a stale file) so callers never see ghosts.
func listMountStates() ([]mountState, error) {
	dir := mountStateDir()
	entries, err := os.ReadDir(dir)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return nil, nil
		}
		return nil, err
	}

	var states []mountState
	for _, e := range entries {
		if e.IsDir() || filepath.Ext(e.Name()) != ".json" {
			continue
		}
		path := filepath.Join(dir, e.Name())
		data, err := os.ReadFile(path)
		if err != nil {
			continue
		}
		var st mountState
		if err := json.Unmarshal(data, &st); err != nil {
			continue // skip unreadable record rather than fail the whole listing
		}
		if !processAlive(st.PID) {
			_ = os.Remove(path) // prune stale entry
			continue
		}
		states = append(states, st)
	}
	sort.Slice(states, func(i, j int) bool { return states[i].Mountpoint < states[j].Mountpoint })
	return states, nil
}
