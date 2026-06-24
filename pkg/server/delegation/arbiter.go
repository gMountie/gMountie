package delegation

import (
	"sync"
	"time"

	"go.uber.org/zap"

	"go.gmountie.dev/gmountie/pkg/proto"
	"go.gmountie.dev/gmountie/pkg/server/watermark"
	"go.gmountie.dev/gmountie/pkg/utils/log"
)

// Metrics is the arbiter's optional observability sink. Nil = no-op.
// The interface is defined here (not in pkg/server/metrics) so the
// delegation package stays free of a metrics import; the adapter at the
// wiring site satisfies it structurally.
type Metrics interface {
	GrantsActiveSet(n int) // current count of held delegations
	RecallInc()            // a successful recall RTT
	CooldownTripInc()      // a root entered cooldown
}

type Config struct {
	Cooldown cooldownConfig
	Metrics  Metrics // nil = no observability
}

// errCoalescedRecallFailed is returned by a coalesced OnMutation waiter when
// the in-flight leader recall failed and the root is still foreign-owned.
// arbitrateContention maps any non-nil error to FS_EAGAIN.
type errCoalescedStr string

func (e errCoalescedStr) Error() string { return string(e) }

const errCoalescedRecallFailed errCoalescedStr = "delegation: coalesced recall failed; root still foreign-owned"

// regionState tracks an in-flight recall so concurrent contenders coalesce
// onto one recall instead of stampeding the holder.
type regionState struct {
	done chan struct{} // closed when the in-flight recall finishes
}

type Arbiter struct {
	recaller Recaller
	now      func() time.Time
	metrics  Metrics // nil = no-op
	store    watermark.Store

	mu       sync.Mutex
	table    *delegationTable
	cooldown *cooldownTable
	regions  map[string]*regionState // keyed by delegated root being recalled
	// pendingRevoke holds delegation entries drained from the table when a
	// session's recall stream closed (DeferRevokeOnStreamClose) but whose gens
	// have NOT yet been revoked. The gen-revoke is deferred to the grace-period
	// reap (ReleaseSession) so a transient recall-stream blip cannot fence a
	// still-live holder's un-flushed WAL. Keyed by session ID.
	pendingRevoke map[string][]entry
}

// NewArbiter creates a new Arbiter. store is required and must not be nil;
// it is used to durably record revoked delegation generations on handoff.
func NewArbiter(r Recaller, cfg Config, now func() time.Time, store watermark.Store) *Arbiter {
	return &Arbiter{
		recaller: r,
		now:      now,
		metrics:  cfg.Metrics,
		store:    store,
		table:         newDelegationTable(),
		cooldown:      newCooldownTable(cfg.Cooldown),
		regions:       make(map[string]*regionState),
		pendingRevoke: make(map[string][]entry),
	}
}

// DeferRevokeOnStreamClose releases a session's delegations from the table the
// instant its recall stream closes, so a contender is never blocked by a holder
// that can no longer be recalled ("recall: no stream for session …"). This is
// the lifecycle fix for the orphaned-delegation window between a recall-stream
// close and the grace-period reap.
//
// The gen-revoke (WAL fence) is DEFERRED to the reap (ReleaseSession), NOT done
// here: a transient recall-stream blip (the client's recall goroutine
// reconnects within ~1s) must not fence the holder's still-valid un-flushed
// WAL. A resumed session simply re-acquires its delegation via the normal
// piggyback path; its deferred gens are revoked harmlessly at its eventual reap
// (which only fires for a truly-dead session, after its final flush).
func (a *Arbiter) DeferRevokeOnStreamClose(sessionID string) {
	a.mu.Lock()
	drained := a.table.drainOwner(sessionID)
	if len(drained) > 0 {
		a.pendingRevoke[sessionID] = append(a.pendingRevoke[sessionID], drained...)
	}
	a.mGrantsActive()
	a.mu.Unlock()
}

// mGrantsActive emits the current delegation count; nil-guarded.
func (a *Arbiter) mGrantsActive() {
	if a.metrics != nil {
		a.metrics.GrantsActiveSet(a.table.size())
	}
}

// mRecall emits a successful recall; nil-guarded.
func (a *Arbiter) mRecall() {
	if a.metrics != nil {
		a.metrics.RecallInc()
	}
}

// mCooldownTrip emits a cooldown trip; nil-guarded.
func (a *Arbiter) mCooldownTrip() {
	if a.metrics != nil {
		a.metrics.CooldownTripInc()
	}
}

// Request grants owner a delegation rooted at root, carving around foreign
// subtrees and refusing cooling roots. Returns a grant (empty GrantedRoot =
// denied). root=="" (no piggyback) returns an empty grant without touching the
// table.
//
// principal and volume are stored in the entry to construct the fence key
// ({principal,volume}) when this delegation is later revoked on handoff.
// They must match the watermark.Key used by Apply for the same operation
// stream (Apply derives its key as {id.Principal, volume}).
func (a *Arbiter) Request(owner, root, principal, volume string) *proto.DelegationGrant {
	if root == "" {
		return &proto.DelegationGrant{}
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	now := a.now()
	a.cooldown.sweep(now)
	if a.cooldown.cooling(root, now) {
		return &proto.DelegationGrant{RetryAfterMs: uint64(a.cfgRetryMs())}
	}
	// Allocate a durable per-(identity,volume) gen BEFORE inserting into the
	// table.  NextGen is a durable fsync under the lock, which is acceptable
	// because grants already serialize on a.mu.  On NextGen failure we deny
	// (return RetryAfterMs) — never grant with gen=0, which is the "untagged /
	// never fenced" sentinel and would reopen the false-fence hole.
	fenceKey := watermark.Key{Identity: principal, Volume: volume}
	gen, genErr := a.store.NextGen(fenceKey)
	if genErr != nil {
		return &proto.DelegationGrant{RetryAfterMs: uint64(a.cfgRetryMs())}
	}
	granted, excluded, ok := a.table.grant(owner, root, principal, volume, gen)
	if !ok {
		// Denial: the gen slot was consumed but no entry was created — gen gaps
		// are harmless; do NOT decrement (no in-memory counter to roll back).
		return &proto.DelegationGrant{RetryAfterMs: uint64(a.cfgRetryMs())}
	}
	a.mGrantsActive()
	return &proto.DelegationGrant{GrantedRoot: granted, ExcludedPaths: excluded, Gen: gen}
}

func (a *Arbiter) cfgRetryMs() int64 { return a.cooldown.cfg.Base.Milliseconds() }

// OnMutation enforces recall-on-contention. If a FOREIGN delegation covers
// path, recall it (releasing the lock across the RPC), trip its cooldown, and
// drop it. Self-owned coverage is a no-op. Returns the recall error so the
// caller can fail the contender's op closed (map to FS_EAGAIN).
func (a *Arbiter) OnMutation(contender, path string) error {
	a.mu.Lock()
	owner, root, ok := a.table.ownerOf(path)
	if !ok || owner == contender {
		a.mu.Unlock()
		return nil // free, or self-access -> never recall
	}
	// Coalesce: if this root is already being recalled, wait for that recall.
	if rs := a.regions[root]; rs != nil {
		done := rs.done
		a.mu.Unlock()
		<-done
		// Re-check: if the leader's recall FAILED the root is still foreign-owned.
		// Return an error so the contender maps it to FS_EAGAIN, consistent with
		// what the leader returns on failure.
		a.mu.Lock()
		stillOwner, _, stillOwned := a.table.ownerOf(path)
		a.mu.Unlock()
		if stillOwned && stillOwner != contender {
			return errCoalescedRecallFailed
		}
		return nil
	}
	// Capture the entry's fence key BEFORE releasing the lock. A concurrent
	// ReleaseSession (session reap) could drop the entry during the recall RTT;
	// we must fence the revoked gen even if the entry is gone by then.
	revokedEntry, hasEntry := a.table.entryForRoot(root)

	rs := &regionState{done: make(chan struct{})}
	a.regions[root] = rs
	a.mu.Unlock()

	// ---- recall RTT happens with NO lock held (barrier = this handoff) ----
	err := a.recaller.Recall(owner, root)

	if err == nil && hasEntry && revokedEntry.gen > 0 {
		// Persist-before-handoff: durably record the revoked gen BEFORE closing
		// rs.done so that coalesced waiters (the contenders now unblocked) can
		// never proceed without the fence being durable. If RevokeGen fails, treat
		// the handoff as failed (same as a recall error): don't release the entry,
		// don't serve the contender.
		fenceKey := watermark.Key{Identity: revokedEntry.principal, Volume: revokedEntry.volume}
		if rErr := a.store.RevokeGen(fenceKey, revokedEntry.gen); rErr != nil {
			// RevokeGen is durable-required; failing here is corrupt-safe: the
			// contender will get errCoalescedRecallFailed and back off.
			err = rErr
		}
	}

	a.mu.Lock()
	if err == nil {
		a.table.release(root)
		a.cooldown.trip(root, a.now())
		a.mRecall()
		a.mCooldownTrip()
		a.mGrantsActive()
	}
	delete(a.regions, root)
	close(rs.done)
	a.mu.Unlock()
	return err
}

// ReleaseSession drops all delegations owned by a reaped session and durably
// revokes their generation numbers so that a dead holder's replayed WAL cannot
// clobber the new owner after machine-death + handoff (Task 6 death path).
//
// Revocation is performed OUTSIDE the lock (after draining the table) so that
// the durable store write does not hold the arbiter mutex during I/O. Entries
// are captured under the lock and revoked outside it; any revoke error is
// logged (best-effort — a failed revoke is still safer than no revoke, because
// a successful revoke prevents corruption and a failed one leaves the prior
// behaviour: no fence).
func (a *Arbiter) ReleaseSession(sessionID string) {
	a.mu.Lock()
	drained := a.table.drainOwner(sessionID)
	// Include any gens deferred earlier by DeferRevokeOnStreamClose: the recall
	// stream closed (entry already removed from the table) but the gen-revoke
	// was held until this reap confirmed the session is truly dead.
	if pending := a.pendingRevoke[sessionID]; len(pending) > 0 {
		drained = append(drained, pending...)
		delete(a.pendingRevoke, sessionID)
	}
	a.mGrantsActive()
	a.mu.Unlock()

	// Revoke each drained gen outside the lock.  gen=0 is the untagged sentinel
	// and is never passed to RevokeGen.
	for _, e := range drained {
		if e.gen == 0 {
			continue
		}
		fenceKey := watermark.Key{Identity: e.principal, Volume: e.volume}
		// Best-effort: a failed revoke means the fence won't fire for this gen
		// (same risk as before Task 6).  Log loudly so the operator can
		// investigate — a silent missed revoke is invisible data-loss risk.
		if rErr := a.store.RevokeGen(fenceKey, e.gen); rErr != nil {
			log.Log.Error("delegation gen revoke failed on session reap; fence may not fire",
				zap.Error(rErr),
				zap.String("identity", e.principal),
				zap.String("volume", e.volume),
				zap.Uint64("gen", e.gen),
				zap.String("session", sessionID),
			)
		}
	}
}
