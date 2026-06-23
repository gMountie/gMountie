package delegation

import (
	"sync"
	"time"

	"go.gmountie.dev/gmountie/pkg/proto"
)

type Config struct {
	Cooldown cooldownConfig
}

// regionState tracks an in-flight recall so concurrent contenders coalesce
// onto one recall instead of stampeding the holder.
type regionState struct {
	done chan struct{} // closed when the in-flight recall finishes
}

type Arbiter struct {
	recaller Recaller
	now      func() time.Time

	mu       sync.Mutex
	table    *delegationTable
	cooldown *cooldownTable
	regions  map[string]*regionState // keyed by delegated root being recalled
}

func NewArbiter(r Recaller, cfg Config, now func() time.Time) *Arbiter {
	return &Arbiter{
		recaller: r,
		now:      now,
		table:    newDelegationTable(),
		cooldown: newCooldownTable(cfg.Cooldown),
		regions:  make(map[string]*regionState),
	}
}

// Request grants owner a delegation rooted at root, carving around foreign
// subtrees and refusing cooling roots. Returns a grant (empty GrantedRoot =
// denied). root=="" (no piggyback) returns an empty grant without touching the
// table.
func (a *Arbiter) Request(owner, root string) *proto.DelegationGrant {
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
	granted, excluded, ok := a.table.grant(owner, root)
	if !ok {
		return &proto.DelegationGrant{RetryAfterMs: uint64(a.cfgRetryMs())}
	}
	return &proto.DelegationGrant{GrantedRoot: granted, ExcludedPaths: excluded}
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
		return nil // the in-flight recall already freed (or cooled) the root
	}
	rs := &regionState{done: make(chan struct{})}
	a.regions[root] = rs
	a.mu.Unlock()

	// ---- recall RTT happens with NO lock held (barrier = this handoff) ----
	err := a.recaller.Recall(owner, root)

	a.mu.Lock()
	if err == nil {
		a.table.release(root)
		a.cooldown.trip(root, a.now())
	}
	delete(a.regions, root)
	close(rs.done)
	a.mu.Unlock()
	return err
}

// ReleaseSession drops all delegations owned by a reaped session.
func (a *Arbiter) ReleaseSession(sessionID string) {
	a.mu.Lock()
	a.table.releaseOwner(sessionID)
	a.mu.Unlock()
}
