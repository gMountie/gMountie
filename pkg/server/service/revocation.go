package service

import (
	"math/big"
	"strings"
	"sync/atomic"

	"go.gmountie.dev/gmountie/pkg/utils/log"

	"go.uber.org/zap"
)

// SerialKey is the canonical key for a certificate serial: lowercase hex, no
// separators. Used identically for blocklist entries and presented certs so
// formatting can never cause a miss.
func SerialKey(n *big.Int) string {
	if n == nil {
		return ""
	}
	return n.Text(16)
}

// ParseSerialKey normalizes a config-supplied serial in any common hex format
// ("abcd", "AB:CD", "0xABCD") to a SerialKey. Returns ("", false) when the
// value is not valid hex.
func ParseSerialKey(s string) (string, bool) {
	clean := strings.ToLower(strings.TrimSpace(s))
	clean = strings.TrimPrefix(clean, "0x")
	clean = strings.ReplaceAll(clean, ":", "")
	if clean == "" {
		return "", false
	}
	n, ok := new(big.Int).SetString(clean, 16)
	if !ok {
		return "", false
	}
	return SerialKey(n), true
}

// RevocationStore holds the cert-serial blocklist as an atomically-swappable
// snapshot. Writers: the ops reload handler. Readers: the TLS handshake hook,
// the gRPC auth interceptor, and the session reaper. A reader always sees a
// fully-consistent map, never a half-updated one.
type RevocationStore struct {
	blocked atomic.Pointer[map[string]struct{}]
}

// NewRevocationStore returns a store with an empty blocklist.
func NewRevocationStore() *RevocationStore {
	r := &RevocationStore{}
	empty := make(map[string]struct{})
	r.blocked.Store(&empty)
	return r
}

// Set replaces the blocklist with the normalized serials. Unparseable entries
// are dropped. A nil/empty slice clears the list.
func (r *RevocationStore) Set(serials []string) {
	m := make(map[string]struct{}, len(serials))
	for _, s := range serials {
		if key, ok := ParseSerialKey(s); ok {
			m[key] = struct{}{}
		} else {
			// A typo'd serial in a security blocklist silently fails to block
			// that cert — surface it so the operator can fix the config.
			log.Log.Warn("revocation: dropping unparseable revoked_serials entry", zap.String("entry", s))
		}
	}
	r.blocked.Store(&m)
}

// IsBlocked reports whether the given SerialKey is in the current blocklist.
// An empty key (no client cert) is never blocked.
func (r *RevocationStore) IsBlocked(key string) bool {
	if key == "" {
		return false
	}
	m := r.blocked.Load()
	if m == nil { // zero-value RevocationStore{} built without the constructor
		return false
	}
	_, ok := (*m)[key]
	return ok
}
