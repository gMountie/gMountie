package delegation

import (
	"context"
	"strings"
	"sync"

	"go.gmountie.dev/gmountie/pkg/proto"
)

// CacheInvalidator is the outward signal sent on recall: the cache must evict
// every entry whose path falls under the recalled subtree.
type CacheInvalidator interface {
	InvalidateSubtree(path string)
}

// grantState holds one active delegation grant.
type grantState struct {
	grantedRoot   string
	excludedPaths []string
}

// Manager holds active delegation grants, exposes the IsDelegated oracle used
// by the cache layer, and handles recall events. It is the client-side counterpart
// to the server's DelegationController.
//
// Concurrency: all exported methods are safe for concurrent use.
type Manager struct {
	inv    CacheInvalidator
	ws     *writeSet
	mu     sync.RWMutex
	grants map[string]grantState // keyed by grantedRoot
	cancel context.CancelFunc   // cancels the recall goroutine; set via SetCancel
	stop   chan struct{}
	once   sync.Once
}

// NewManager constructs a Manager. inv is called with the recalled subtree root
// when a server-issued recall arrives (via OnRecall).
func NewManager(inv CacheInvalidator) *Manager {
	return &Manager{
		inv:    inv,
		ws:     newWriteSet(64),
		grants: make(map[string]grantState),
		stop:   make(chan struct{}),
	}
}

// contains reports whether path b is equal to or under path a.
// An empty a matches everything (mount root).
func contains(a, b string) bool {
	return a == "" || a == b || strings.HasPrefix(b, a+"/")
}

// IsDelegated returns true iff at least one held grant covers path and no
// excluded sub-path covers path.
func (m *Manager) IsDelegated(path string) bool {
	m.mu.RLock()
	defer m.mu.RUnlock()
	for _, g := range m.grants {
		if !contains(g.grantedRoot, path) {
			continue
		}
		excluded := false
		for _, ex := range g.excludedPaths {
			if contains(ex, path) {
				excluded = true
				break
			}
		}
		if !excluded {
			return true
		}
	}
	return false
}

// Apply records a grant returned by the server. A grant with an empty
// GrantedRoot is a denial and is silently ignored.
func (m *Manager) Apply(grant *proto.DelegationGrant) {
	if grant.GetGrantedRoot() == "" {
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.grants[grant.GrantedRoot] = grantState{
		grantedRoot:   grant.GrantedRoot,
		excludedPaths: grant.ExcludedPaths,
	}
}

// RequestedRoot returns the write-set LCA to piggyback as a delegation request
// on the next mutating RPC. Returns "" when there is nothing to request or when
// the LCA is already fully delegated (no need to ask again).
func (m *Manager) RequestedRoot() string {
	r := m.ws.root()
	if r == "" {
		return ""
	}
	if m.IsDelegated(r) {
		return ""
	}
	return r
}

// Record feeds a written path into the write-set so the LCA can be computed.
func (m *Manager) Record(path string) {
	m.ws.record(path)
}

// OnRecall drops all grants that cover or are covered by root and then
// signals the cache to evict the subtree. It is the unit-testable core of
// the server-driven recall flow (the stream pump is wired in Task 10).
func (m *Manager) OnRecall(root string) {
	m.mu.Lock()
	for key, g := range m.grants {
		if contains(root, g.grantedRoot) || contains(g.grantedRoot, root) {
			delete(m.grants, key)
		}
	}
	m.mu.Unlock()
	// Call the invalidator outside the lock to avoid holding it during an
	// external callback (prevents deadlock if the invalidator re-enters Manager).
	m.inv.InvalidateSubtree(root)
}

// SetCancel registers the context cancel function for the recall goroutine
// started in single.go. It is called once, right after the goroutine is
// launched. Close invokes it (under the once guard) so the goroutine exits
// cleanly on unmount. The cancel func is guarded by m.mu to give -race a
// well-defined happens-before edge.
func (m *Manager) SetCancel(cancel context.CancelFunc) {
	m.mu.Lock()
	m.cancel = cancel
	m.mu.Unlock()
}

// Close stops the recall goroutine by cancelling its context and closing the
// stop channel. Safe to call multiple times.
func (m *Manager) Close() {
	m.once.Do(func() {
		m.mu.RLock()
		cancel := m.cancel
		m.mu.RUnlock()
		if cancel != nil {
			cancel()
		}
		close(m.stop)
	})
}
