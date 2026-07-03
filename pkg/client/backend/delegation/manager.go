package delegation

import (
	"context"
	"strings"
	"sync"

	"go.gmountie.dev/gmountie/pkg/proto"
	"go.gmountie.dev/gmountie/pkg/utils/log"
	"go.uber.org/zap"
)

// CacheInvalidator is the outward signal sent on recall: the cache must evict
// every entry whose path falls under the recalled subtree.
type CacheInvalidator interface {
	InvalidateSubtree(path string)
}

// RecallFlusher is implemented by the WAL Coordinator (wired in Task 14). It
// flushes a barrier snapshot of the pending prefix (a bounded superset of the
// recalled region's ops) to the server before the recall handoff completes, so
// the contender always sees the holder's deferred writes. The root parameter
// names the recalled subtree (for logging/tracing); the current implementation
// flushes the entire pending prefix, which is a correct superset of root.
//
// Implementations must block until the ops are durably acked by the server (or
// until ctx is cancelled). On permanent failure (ENOSPC, EIO) the WAL onLoss
// hook has already fired; the error returned here prevents the caller from
// completing the clean handoff.
type RecallFlusher interface {
	FlushForRecall(ctx context.Context, root string) error
}

// grantState holds one active delegation grant.
type grantState struct {
	grantedRoot   string
	excludedPaths []string
	// gen is the server-issued generation for this grant. Every WAL op deferred
	// under the grant is stamped with it (Coordinator.RecordOp) so the server's
	// revoked-gen fence can reject a stale replay after a handoff.
	gen uint64
}

// Manager holds active delegation grants, exposes the IsDelegated oracle used
// by the cache layer, and handles recall events. It is the client-side counterpart
// to the server's DelegationController.
//
// Concurrency: all exported methods are safe for concurrent use.
type Manager struct {
	inv      CacheInvalidator
	ws       *writeSet
	mu       sync.RWMutex
	grants   map[string]grantState // keyed by grantedRoot
	cancel   context.CancelFunc    // cancels the recall goroutine; set via SetCancel
	flusher  RecallFlusher         // optional; wired in Task 14 via SetRecallFlusher
	once     sync.Once
	walEpoch string // client wal.db epoch (mu-guarded); stamped on DelegationRequests
	// draining maps a recalled root → the refcounted entry tracking every
	// in-flight recall for it. While a root is draining, IsWriteDelegated is
	// false for paths under it (new mutations must not defer — the recall
	// flush snapshot would miss them) while IsDelegated stays true (the
	// overlay is still authoritative for reads until it is flushed+cleared).
	// The entry stays in the map — and the root stays draining — until the
	// LAST in-flight recall for the root finishes, regardless of the order in
	// which concurrent recalls for the same root complete.
	draining map[string]*drainEntry
}

// drainEntry tracks the in-flight recalls draining one root. n counts them;
// ch closes when the last finishes. Shared by every concurrent OnRecall for
// the same root so no ordering of completions can un-drain the root while any
// recall's flush is still in flight.
type drainEntry struct {
	n  int
	ch chan struct{}
}

// SetWalEpoch records the client wal.db's stable epoch. Called once at mount
// (after the WAL log opens, before any mutating RPC). Stamped on every
// DelegationRequest via WalEpoch() so the server keys the delegation gen +
// dedup watermark per (identity, volume, wal-epoch).
func (m *Manager) SetWalEpoch(epoch string) {
	m.mu.Lock()
	m.walEpoch = epoch
	m.mu.Unlock()
}

// WalEpoch returns the client wal.db epoch to piggyback on DelegationRequests.
func (m *Manager) WalEpoch() string {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.walEpoch
}

// NewManager constructs a Manager. inv is called with the recalled subtree root
// when a server-issued recall arrives (via OnRecall).
func NewManager(inv CacheInvalidator) *Manager {
	return &Manager{
		inv:      inv,
		ws:       newWriteSet(64),
		grants:   make(map[string]grantState),
		draining: make(map[string]*drainEntry),
	}
}

// contains reports whether path b is equal to or under path a.
// An empty a matches everything (mount root).
func contains(a, b string) bool {
	return a == "" || a == b || strings.HasPrefix(b, a+"/")
}

// coveringGrantLocked returns the grant covering path (containment minus
// exclusions), or nil. Caller must hold m.mu (read or write).
func (m *Manager) coveringGrantLocked(path string) *grantState {
	for k := range m.grants {
		g := m.grants[k]
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
			return &g
		}
	}
	return nil
}

// IsDelegated returns true iff at least one held grant covers path and no
// excluded sub-path covers path.
func (m *Manager) IsDelegated(path string) bool {
	// Nil-receiver safe: a nil *Manager (WAL disabled → no delegation) means
	// "nothing is delegated". Defends against a typed-nil oracle reaching the
	// cache's delegation-aware fast path without panicking.
	if m == nil {
		return false
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.coveringGrantLocked(path) != nil
}

// drainingLocked reports whether any draining (recall or admission barrier in
// flight) root covers path. Caller must hold m.mu (read or write).
func (m *Manager) drainingLocked(path string) bool {
	for root := range m.draining {
		if contains(root, path) {
			return true
		}
	}
	return false
}

// IsWriteDelegated reports whether path may admit NEW deferred ops: covered by
// a grant AND not under a draining (recall in progress) root. Read paths keep
// using IsDelegated during a drain; write admission stops the moment a recall
// begins so the recall flush is a complete snapshot of deferred state.
func (m *Manager) IsWriteDelegated(path string) bool {
	if m == nil {
		return false
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	if m.drainingLocked(path) {
		return false
	}
	return m.coveringGrantLocked(path) != nil
}

// IsDraining reports whether any draining (recall or admission barrier in
// flight) root covers path. Nil-receiver safe.
//
// NOTE: IsDraining alone cannot decide whether a synchronous (wire) fallback
// is safe after an admission refusal — that inference needs the grant state
// and the drain state from ONE lock snapshot. Use AdmissionState for that.
func (m *Manager) IsDraining(path string) bool {
	if m == nil {
		return false
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.drainingLocked(path)
}

// AdmissionState reports, under ONE lock snapshot, whether path is covered by
// a grant (delegated) and whether a draining root covers it (draining). The
// pair answers the wire/sync-fallback safety question atomically: both false
// ⇒ no pending WAL op for path can exist or be racing a recall Apply — the
// grant can only be gone after a SUCCESSFUL recall flush (every op-retaining
// failure path retains the grant), and no flush is in flight. Two separate
// IsWriteDelegated/IsDraining calls cannot make that inference (a failed
// recall flush can end the drain grant-retained between them). Nil-safe.
func (m *Manager) AdmissionState(path string) (delegated, draining bool) {
	if m == nil {
		return false, false
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.coveringGrantLocked(path) != nil, m.drainingLocked(path)
}

// GenFor returns the server-issued generation of the grant covering path, or 0
// when no grant covers it. The Coordinator stamps this on every deferred op so
// the server's revoked-gen fence can reject the op if the grant is later
// revoked (machine-death handoff). Nil-receiver safe.
func (m *Manager) GenFor(path string) uint64 {
	if m == nil {
		return 0
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	if g := m.coveringGrantLocked(path); g != nil {
		return g.gen
	}
	return 0
}

// WaitDrained blocks while any draining root covers path (or until ctx is
// done). Callers race a recall: an op refused admission because its region is
// draining waits here, then retries — after a completed handoff the retry is
// refused again (grant gone) and the caller goes synchronous; after a FAILED
// recall flush the grant is retained and the retry defers as before.
func (m *Manager) WaitDrained(ctx context.Context, path string) {
	if m == nil {
		return
	}
	for {
		m.mu.RLock()
		var ch chan struct{}
		for root, e := range m.draining {
			if contains(root, path) {
				ch = e.ch
				break
			}
		}
		m.mu.RUnlock()
		if ch == nil {
			return
		}
		select {
		case <-ch:
		case <-ctx.Done():
			return
		}
	}
}

// markDraining registers one in-flight drain for root and returns the shared
// refcounted entry. While ANY drain for a covering root is registered,
// IsWriteDelegated is false (and IsDraining true) for paths under it, so new
// deferrals park in WaitDrained instead of being admitted. Shared by OnRecall
// and BeginDrain so the two use literally the same refcount logic.
func (m *Manager) markDraining(root string) *drainEntry {
	m.mu.Lock()
	entry := m.draining[root]
	if entry == nil {
		entry = &drainEntry{ch: make(chan struct{})}
		m.draining[root] = entry
	}
	entry.n++
	m.mu.Unlock()
	return entry
}

// endDraining releases one in-flight drain for root. When the LAST drain for
// the root ends, the root leaves the draining set and entry.ch closes, waking
// every WaitDrained parker (the n-write happens-before the close via m.mu).
func (m *Manager) endDraining(root string, entry *drainEntry) {
	m.mu.Lock()
	entry.n--
	last := entry.n == 0
	if last {
		delete(m.draining, root)
	}
	m.mu.Unlock()
	if last {
		close(entry.ch)
	}
}

// BeginDrain marks root draining (refcounted, same entries as OnRecall) and
// returns a release func. Used by the WAL layer to hold an admission barrier
// over compound operations (flush-then-rename): racing deferrals park in
// WaitDrained instead of being admitted against a path about to move.
// Nil-receiver safe (returns a no-op release). release must be called exactly
// once.
func (m *Manager) BeginDrain(root string) (release func()) {
	if m == nil {
		return func() {}
	}
	entry := m.markDraining(root)
	return func() { m.endDraining(root, entry) }
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
		gen:           grant.GetGen(),
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

// OnRecall is the handoff barrier for a server-driven recall. It:
//
//  1. Flushes the WAL (if a RecallFlusher is wired) BEFORE touching the grant
//     table or the cache — so the contender always sees the holder's deferred
//     writes durably on the server before the ack is sent.
//  2. Drops all grants covering or covered by root.
//  3. Signals the cache to evict the subtree.
//
// The ack is sent by the recall loop in single.go AFTER OnRecall returns. By
// flushing inside OnRecall the ack is gated on durable write propagation.
//
// Flush failure (ENOSPC / EIO): OnRecall returns the error WITHOUT dropping the
// grant or invalidating the cache. The recall loop must NOT send a RecallAck on
// error — it skips the Send so the server-side RecallRegistry times out and the
// contender does not observe a clean handoff. The WAL's onLoss hook (Task 13)
// has already fired for the lost ops when the error is returned here.
//
// nil flusher or no pending WAL ops: flush is skipped, and Phase-1 behaviour
// (drop + invalidate) is preserved unchanged.
func (m *Manager) OnRecall(ctx context.Context, root string) error {
	// Step 1: flush the entire pending WAL prefix (a correct superset of root).
	// The full-pending-prefix approach is correct because every deferred write
	// belongs to at least one grant that is being recalled or superseded; the
	// optimization of flushing only ops touching root is deferred (Task 14 note).
	m.mu.RLock()
	flusher := m.flusher
	m.mu.RUnlock()

	// Stop write admission for the recalled region for the duration of the
	// flush. New mutating ops go synchronous (or wait via WaitDrained); reads
	// keep merging the overlay until the flush clears it. Cleared on BOTH the
	// success and failure paths: on failure the grant is retained and deferral
	// resumes (fail-closed handoff — the server times out the recall).
	//
	// Refcounted: concurrent OnRecall calls for the same root share one
	// drainEntry. The root stays in m.draining — and IsWriteDelegated stays
	// false for it — until the LAST in-flight recall for the root finishes,
	// no matter which of several concurrent recalls happens to complete (or
	// fail) first.
	entry := m.markDraining(root)
	defer m.endDraining(root, entry)

	if flusher != nil {
		if err := flusher.FlushForRecall(ctx, root); err != nil {
			// Flush failed: on a PERMANENT halt onLoss has already fired for the
			// lost ops; on a TRANSIENT halt (FS_EAGAIN contention) the tail is
			// retained in the WAL — no loss either way that this path must
			// handle. Do NOT drop the grant or invalidate — the handoff is
			// aborted. The recall loop will skip the RecallAck, letting the
			// server timeout the recall instead of accepting a false clean
			// handoff; the next recall retries the flush from where the log
			// now stands.
			log.Log.Error("WAL flush failed before recall handoff; aborting clean ack",
				zap.String("root", root),
				zap.Error(err),
			)
			return err
		}
	}

	// Step 2: drop grants (only after flush succeeds).
	m.mu.Lock()
	for key, g := range m.grants {
		if contains(root, g.grantedRoot) || contains(g.grantedRoot, root) {
			delete(m.grants, key)
		}
	}
	m.mu.Unlock()

	// Step 3: signal the cache outside the lock to avoid deadlock if the
	// invalidator re-enters Manager.
	m.inv.InvalidateSubtree(root)
	return nil
}

// SetRecallFlusher wires the WAL Coordinator's flush capability into the Manager.
// It must be called before the recall goroutine starts (typically right after
// NewCoordinator returns in the mount wiring path). Thread-safe: protected by m.mu.
// Task 14 calls this. Until it is called, OnRecall skips the flush (Phase-1 behavior).
func (m *Manager) SetRecallFlusher(f RecallFlusher) {
	m.mu.Lock()
	m.flusher = f
	m.mu.Unlock()
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

// Close stops the recall goroutine by cancelling its context. Safe to call
// multiple times.
func (m *Manager) Close() {
	m.once.Do(func() {
		m.mu.RLock()
		cancel := m.cancel
		m.mu.RUnlock()
		if cancel != nil {
			cancel()
		}
	})
}
