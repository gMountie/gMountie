package tls

import (
	"bufio"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"github.com/adrg/xdg"
)

// KnownHosts is a simple "endpoint fingerprint" line-oriented file
// modeled on SSH known_hosts. Each line is "<endpoint> <fingerprint>".
// Intra-process access is serialized via mu; inter-process concurrent
// first-pins on the same endpoint may both succeed (O_APPEND on Linux is
// atomic for lines ≤ PIPE_BUF), resulting in two identical lines —
// Lookup returns the first match so the duplicate is harmless.
type KnownHosts struct {
	path string
	mu   sync.Mutex
}

func openKnownHosts(override string) (*KnownHosts, error) {
	p := override
	if p == "" {
		p = filepath.Join(xdg.StateHome, "gmountie", "known_hosts")
	}
	if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
		return nil, fmt.Errorf("ensure known_hosts dir: %w", err)
	}
	return &KnownHosts{path: p}, nil
}

// Lookup returns the pinned fingerprint for endpoint, or ("", false) if not found.
func (k *KnownHosts) Lookup(endpoint string) (string, bool) {
	f, err := os.Open(k.path)
	if err != nil {
		return "", false
	}
	defer func() { _ = f.Close() }()
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		line := strings.TrimSpace(sc.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		fields := strings.Fields(line)
		if len(fields) != 2 {
			continue
		}
		if fields[0] == endpoint {
			return fields[1], true
		}
	}
	return "", false
}

// Pin records endpoint → fingerprint in the file. Returns nil if the
// endpoint is already pinned to the same fingerprint (idempotent). Returns
// an error if the endpoint is already pinned to a different fingerprint.
func (k *KnownHosts) Pin(endpoint, fingerprint string) error {
	k.mu.Lock()
	defer k.mu.Unlock()

	if existing, ok := k.Lookup(endpoint); ok {
		if existing == fingerprint {
			return nil
		}
		return fmt.Errorf("refusing to overwrite pinned fingerprint for %s", endpoint)
	}
	f, err := os.OpenFile(k.path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o600)
	if err != nil {
		return err
	}
	defer func() { _ = f.Close() }()
	_, err = fmt.Fprintf(f, "%s %s\n", endpoint, fingerprint)
	return err
}
