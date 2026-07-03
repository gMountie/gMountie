package delegation

import (
	"errors"
	"sync"
	"sync/atomic"
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
	grantCount atomic.Int64            // maintained at every table mutation (under mu)
}

// NewArbiter creates a new Arbiter. store is required and must not be nil;
// it is used to durably record revoked delegation generations on handoff.
func NewArbiter(r Recaller, cfg Config, now func() time.Time, store watermark.Store) *Arbiter {
	return &Arbiter{
		recaller: r,
		now:      now,
		metrics:  cfg.Metrics,
		store:    store,
		table:    newDelegationTable(),
		cooldown: newCooldownTable(cfg.Cooldown),
		regions:  make(map[string]*regionState),
	}
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
func (a *Arbiter) Request(owner, root, principal, volume, epoch string) *proto.DelegationGrant {
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
	fenceKey := watermark.Key{Identity: principal, Volume: volume, Epoch: epoch}
	gen, genErr := a.store.NextGen(fenceKey)
	if genErr != nil {
		return &proto.DelegationGrant{RetryAfterMs: uint64(a.cfgRetryMs())}
	}
	granted, excluded, ok := a.table.grant(owner, root, principal, volume, epoch, gen)
	if !ok {
		// Denial: the gen slot was consumed but no entry was created — gen gaps
		// are harmless; do NOT decrement (no in-memory counter to roll back).
		return &proto.DelegationGrant{RetryAfterMs: uint64(a.cfgRetryMs())}
	}
	a.grantCount.Store(int64(a.table.size()))
	a.mGrantsActive()
	return &proto.DelegationGrant{GrantedRoot: granted, ExcludedPaths: excluded, Gen: gen}
}

func (a *Arbiter) cfgRetryMs() int64 { return a.cooldown.cfg.Base.Milliseconds() }

// OnMutation enforces recall-on-contention. If a FOREIGN delegation covers
// path, recall it (releasing the lock across the RPC), trip its cooldown, and
// drop it. Self-owned coverage is a no-op. Returns the recall error so the
// caller can fail the contender's op closed (map to FS_EAGAIN).
func (a *Arbiter) OnMutation(contender, path string) error {
	// Hot-path exit: with zero grants anywhere there is nothing to arbitrate.
	// Read handlers call this on every RPC; do not touch a.mu for the common
	// no-delegations case (WAL-off clients, idle volumes).
	if a.grantCount.Load() == 0 {
		return nil
	}
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

	// A no-stream recall means the holder's recall channel is gone (it
	// disconnected / never opened one). It cannot honour the recall, so we treat
	// contention against it as a CONTENDED HANDOFF: fence the holder's gen —
	// exactly as a delivered recall does on success — so the unreachable holder's
	// un-flushed WAL can never clobber the contender if it resurrects and
	// replays. Skipping the undeliverable RPC is the only difference from a
	// normal handoff. Any OTHER recall error (timeout, send failure) stays
	// fail-closed: no release, no revoke, the contender backs off (EAGAIN).
	//
	// This is the corruption-safe replacement for releasing the entry on
	// recall-stream close without revoking: there, a crashed holder could
	// resurrect within grace and replay its un-revoked gen over the contender.
	// Here the gen is fenced at the moment of handoff, before the contender
	// proceeds. A no-CONTENTION blip never reaches here, so it never revokes (no
	// spurious loss); only an actual contended handoff fences the holder.
	handoff := err == nil || errors.Is(err, ErrNoStream)
	if errors.Is(err, ErrNoStream) {
		log.Log.Warn("delegation: holder unreachable on contention; fencing its gen and handing off (its un-flushed WAL is discarded on replay)",
			zap.String("owner", owner),
			zap.String("root", root),
			zap.String("contender", contender),
		)
		err = nil // a successful contended handoff: the contender proceeds
	}

	if handoff && hasEntry && revokedEntry.gen > 0 {
		// Persist-before-handoff: durably record the revoked gen BEFORE releasing
		// the table + closing rs.done, so coalesced waiters (the contenders now
		// unblocked) can never proceed before the fence is durable. If RevokeGen
		// fails, fail the handoff closed: keep the entry, return the error.
		fenceKey := watermark.Key{Identity: revokedEntry.principal, Volume: revokedEntry.volume, Epoch: revokedEntry.epoch}
		if rErr := a.store.RevokeGen(fenceKey, revokedEntry.gen); rErr != nil {
			err = rErr
			handoff = false
		}
	}

	a.mu.Lock()
	if handoff {
		a.table.release(root)
		a.grantCount.Store(int64(a.table.size()))
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
	a.grantCount.Store(int64(a.table.size()))
	a.mGrantsActive()
	a.mu.Unlock()

	// Revoke each drained gen outside the lock.  gen=0 is the untagged sentinel
	// and is never passed to RevokeGen.
	for _, e := range drained {
		if e.gen == 0 {
			continue
		}
		fenceKey := watermark.Key{Identity: e.principal, Volume: e.volume, Epoch: e.epoch}
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
