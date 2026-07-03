package delegation

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/suite"

	"go.gmountie.dev/gmountie/pkg/server/watermark"
)

type fakeRecaller struct {
	mu         sync.Mutex
	calls      []string // "owner:root"
	failOn     map[string]bool
	noStreamOn map[string]bool // owners whose recall returns ErrNoStream (unreachable holder)
	block      chan struct{}   // if non-nil, Recall blocks on this channel after recording the call
}

func (f *fakeRecaller) Recall(owner, root string) error {
	f.mu.Lock()
	f.calls = append(f.calls, owner+":"+root)
	fail := f.failOn[owner]
	noStream := f.noStreamOn[owner]
	ch := f.block
	f.mu.Unlock()
	if ch != nil {
		<-ch // wait without holding the mutex
	}
	if noStream {
		return fmt.Errorf("%w %s", ErrNoStream, owner)
	}
	if fail {
		return assertErr
	}
	return nil
}

func (f *fakeRecaller) callCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.calls)
}

var assertErr = errInfo("recall failed")

type errInfo string

func (e errInfo) Error() string { return string(e) }

// recallerFunc is a Recaller adapter for inline callback functions.
type recallerFunc func(owner, root string) error

func (f recallerFunc) Recall(owner, root string) error {
	return f(owner, root)
}

// fakeStore is a minimal watermark.Store for arbiter tests. It records
// RevokeGen calls so tests can assert handoff ordering. NextGen is per-key
// so that per-(identity,volume) gen isolation is faithfully represented.
type fakeStore struct {
	mu          sync.Mutex
	revokedKeys []watermark.Key
	revokedGens []uint64
	genHi       map[watermark.Key]uint64 // per-key gen counter
}

func newFakeStore() *fakeStore { return &fakeStore{genHi: make(map[watermark.Key]uint64)} }

func (f *fakeStore) Get(k watermark.Key) (watermark.Record, error) { return watermark.Record{}, nil }
func (f *fakeStore) Advance(_ watermark.Key, _ uint64) error       { return nil }
func (f *fakeStore) Close() error                                  { return nil }

func (f *fakeStore) RevokeGen(k watermark.Key, gen uint64) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.revokedKeys = append(f.revokedKeys, k)
	f.revokedGens = append(f.revokedGens, gen)
	return nil
}

func (f *fakeStore) NextGen(k watermark.Key) (uint64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.genHi[k]++
	return f.genHi[k], nil
}

func (f *fakeStore) revokedGensCopy() []uint64 {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]uint64, len(f.revokedGens))
	copy(out, f.revokedGens)
	return out
}

func (f *fakeStore) revokedKeysCopy() []watermark.Key {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]watermark.Key, len(f.revokedKeys))
	copy(out, f.revokedKeys)
	return out
}

type ArbiterSuite struct {
	suite.Suite
	clock time.Time
}

func TestArbiterSuite(t *testing.T) { suite.Run(t, new(ArbiterSuite)) }

func (s *ArbiterSuite) now() time.Time { return s.clock }

func (s *ArbiterSuite) newArbiter(r Recaller) *Arbiter {
	return s.newArbiterWithStore(r, newFakeStore())
}

func (s *ArbiterSuite) newArbiterWithStore(r Recaller, st watermark.Store) *Arbiter {
	s.clock = time.Unix(1000, 0)
	return NewArbiter(r, Config{
		Cooldown: cooldownConfig{Base: time.Second, Max: time.Minute, Cap: 256},
	}, s.now, st)
}

func (s *ArbiterSuite) TestGrantThenForeignMutationRecalls() {
	fr := &fakeRecaller{}
	a := s.newArbiter(fr)
	g := a.Request("sessA", "proj", "userA", "vol", "")
	s.Equal("proj", g.GrantedRoot)

	// B mutates inside A's subtree -> A recalled, A's grant dropped.
	s.Require().NoError(a.OnMutation("sessB", "proj/file"))
	s.Equal([]string{"sessA:proj"}, fr.calls)

	// A's delegation is gone now; B mutating again must NOT recall (no owner).
	fr.calls = nil
	s.Require().NoError(a.OnMutation("sessB", "proj/file"))
	s.Empty(fr.calls)
}

// TestNoStreamRecallFencesAndHandsOff pins the corruption-safe contended
// handoff: when the holder's recall stream is gone (ErrNoStream), a contender
// takes over — the holder's gen is fenced (so its un-flushed WAL can never
// clobber the contender if it resurrects and replays) and the contender
// proceeds. This replaces the unsafe "release on stream close, defer revoke"
// approach, where a crashed holder could resurrect within grace and clobber.
func (s *ArbiterSuite) TestNoStreamRecallFencesAndHandsOff() {
	fr := &fakeRecaller{noStreamOn: map[string]bool{"sessA": true}}
	st := newFakeStore()
	a := s.newArbiterWithStore(fr, st)

	g := a.Request("sessA", "proj", "alice", "vol", "ep-A")
	s.Require().NotZero(g.Gen)

	// Contender B mutates A's subtree; A is unreachable (no recall stream).
	s.Require().NoError(a.OnMutation("sessB", "proj/file"),
		"contender must proceed when the holder is unreachable")

	// Corruption prevention: A's gen MUST be fenced, under A's epoch fence key.
	s.Require().Contains(st.revokedGensCopy(), g.Gen,
		"unreachable holder's gen must be revoked on contended handoff")
	s.Require().Contains(st.revokedKeysCopy(),
		watermark.Key{Identity: "alice", Volume: "vol", Epoch: "ep-A"},
		"revoke must use the holder's epoch fence key")

	// The root is now free for the contender.
	_, _, owned := a.table.ownerOf("proj/file")
	s.False(owned, "root must be free after handoff")
}

// TestNonNoStreamRecallErrorStaysFailClosed: a generic recall error (timeout,
// send failure) is NOT a handoff — the entry stays, the gen is NOT revoked, and
// the contender backs off (EAGAIN). Only an unreachable holder (ErrNoStream)
// triggers the fence-and-hand-off path.
func (s *ArbiterSuite) TestNonNoStreamRecallErrorStaysFailClosed() {
	fr := &fakeRecaller{failOn: map[string]bool{"sessA": true}}
	st := newFakeStore()
	a := s.newArbiterWithStore(fr, st)

	g := a.Request("sessA", "proj", "alice", "vol", "ep-A")
	s.Require().NotZero(g.Gen)

	s.Require().Error(a.OnMutation("sessB", "proj/file"),
		"a generic recall failure must fail closed")
	s.Empty(st.revokedGensCopy(), "no revoke on a non-ErrNoStream recall failure")
	owner, _, owned := a.table.ownerOf("proj/file")
	s.True(owned, "entry must remain after a fail-closed recall")
	s.Equal("sessA", owner)
}

// TestNoStreamHandoffCoalescedContendersProceed: contenders coalescing on a
// no-stream handoff all proceed without error (the leader fences + releases;
// waiters re-check ownerOf → free).
func (s *ArbiterSuite) TestNoStreamHandoffCoalescedContendersProceed() {
	block := make(chan struct{})
	fr := &fakeRecaller{noStreamOn: map[string]bool{"sessA": true}, block: block}
	st := newFakeStore()
	a := s.newArbiterWithStore(fr, st)
	a.Request("sessA", "proj", "alice", "vol", "ep-A")

	leaderErr := make(chan error, 1)
	go func() { leaderErr <- a.OnMutation("sessB", "proj/file") }()
	// Wait until the leader is in-flight inside Recall (region registered).
	s.Require().Eventually(func() bool { return fr.callCount() == 1 }, time.Second, 2*time.Millisecond)

	coalescedErr := make(chan error, 1)
	go func() { coalescedErr <- a.OnMutation("sessC", "proj/other") }()

	close(block) // let the no-stream recall complete → fence + handoff
	s.Require().NoError(<-leaderErr, "leader must proceed on no-stream handoff")
	s.Require().NoError(<-coalescedErr, "coalesced contender must proceed after handoff")
}

// TestRequestEpochKeysFence pins that the delegation gen + revoke fence are
// keyed by the client's wal-epoch (bug #2): a grant requested under epoch
// "ep-A" must revoke under (identity, volume, ep-A), so the fence lands in the
// same namespace the client's Apply stream uses.
func (s *ArbiterSuite) TestRequestEpochKeysFence() {
	fr := &fakeRecaller{}
	st := newFakeStore()
	a := s.newArbiterWithStore(fr, st)

	a.Request("sessA", "proj", "alice", "vol", "ep-A")

	// Foreign contender forces a handoff -> RevokeGen with the entry's epoch.
	s.Require().NoError(a.OnMutation("sessB", "proj/file"))
	keys := st.revokedKeysCopy()
	s.Require().Len(keys, 1)
	s.Equal(watermark.Key{Identity: "alice", Volume: "vol", Epoch: "ep-A"}, keys[0],
		"revoke fence key must carry the requesting client's wal-epoch")
}

func (s *ArbiterSuite) TestSelfMutationNeverRecalls() {
	fr := &fakeRecaller{}
	a := s.newArbiter(fr)
	a.Request("sessA", "proj", "userA", "vol", "")
	s.Require().NoError(a.OnMutation("sessA", "proj/file")) // own subtree
	s.Empty(fr.calls)
}

func (s *ArbiterSuite) TestCooldownBlocksImmediateRegrant() {
	fr := &fakeRecaller{}
	a := s.newArbiter(fr)
	a.Request("sessA", "proj", "userA", "vol", "")
	s.Require().NoError(a.OnMutation("sessB", "proj/file")) // recall + trip cooldown on "proj"
	// A re-requests immediately -> denied (cooling).
	g := a.Request("sessA", "proj", "userA", "vol", "")
	s.Empty(g.GrantedRoot)
	s.Positive(g.RetryAfterMs)
}

func (s *ArbiterSuite) TestReleaseSessionFreesSubtree() {
	fr := &fakeRecaller{}
	a := s.newArbiter(fr)
	a.Request("sessA", "proj", "userA", "vol", "")
	a.ReleaseSession("sessA")
	// No owner now -> B's mutation recalls nothing; B can take it.
	s.Require().NoError(a.OnMutation("sessB", "proj/x"))
	s.Empty(fr.calls)
	g := a.Request("sessB", "proj", "userB", "vol", "")
	s.Equal("proj", g.GrantedRoot)
}

func (s *ArbiterSuite) TestRecallFailurePropagates() {
	fr := &fakeRecaller{failOn: map[string]bool{"sessA": true}}
	a := s.newArbiter(fr)
	a.Request("sessA", "proj", "userA", "vol", "")
	s.Error(a.OnMutation("sessB", "proj/file")) // handler maps to FS_EAGAIN
}

// fakeMetrics records delegation.Metrics calls for assertions.
type fakeMetrics struct {
	grantsActive []int
	recalls      int
	cooldowns    int
}

func (f *fakeMetrics) GrantsActiveSet(n int) { f.grantsActive = append(f.grantsActive, n) }
func (f *fakeMetrics) RecallInc()            { f.recalls++ }
func (f *fakeMetrics) CooldownTripInc()      { f.cooldowns++ }

func (s *ArbiterSuite) TestMetricsWiredOnGrantAndRecall() {
	fr := &fakeRecaller{}
	fm := &fakeMetrics{}
	s.clock = time.Unix(1000, 0)
	a := NewArbiter(fr, Config{
		Cooldown: cooldownConfig{Base: time.Second, Max: time.Minute, Cap: 256},
		Metrics:  fm,
	}, s.now, newFakeStore())

	// Grant to sessA: should emit GrantsActiveSet(1).
	g := a.Request("sessA", "proj", "userA", "vol", "")
	s.Equal("proj", g.GrantedRoot)
	s.Equal([]int{1}, fm.grantsActive, "grant must emit GrantsActiveSet(1)")

	// Foreign mutation from sessB: recall fires, then RecallInc, CooldownTripInc,
	// and GrantsActiveSet(0) (table is now empty after release).
	s.Require().NoError(a.OnMutation("sessB", "proj/file"))
	s.Equal(1, fm.recalls, "exactly one recall")
	s.Equal(1, fm.cooldowns, "exactly one cooldown trip")
	s.Equal([]int{1, 0}, fm.grantsActive, "GrantsActiveSet must end at 0 after recall")
}

func (s *ArbiterSuite) TestMetricsReleaseSession() {
	fr := &fakeRecaller{}
	fm := &fakeMetrics{}
	s.clock = time.Unix(1000, 0)
	a := NewArbiter(fr, Config{
		Cooldown: cooldownConfig{Base: time.Second, Max: time.Minute, Cap: 256},
		Metrics:  fm,
	}, s.now, newFakeStore())

	a.Request("sessA", "proj", "userA", "vol", "")
	fm.grantsActive = nil // reset after grant observation
	a.ReleaseSession("sessA")
	s.Equal([]int{0}, fm.grantsActive, "ReleaseSession must emit GrantsActiveSet(0)")
}

func (s *ArbiterSuite) TestOnMutationFastPathWithoutGrants() {
	// A recaller that fails the test if invoked, plus a mutex-poisoning probe:
	// hold a.mu from another goroutine and assert OnMutation still returns
	// (i.e. it never touched the mutex).
	a := NewArbiter(recallerFunc(func(owner, root string) error {
		s.Fail("no recall expected")
		return nil
	}), Config{}, time.Now, newFakeStore())

	locked := make(chan struct{})
	release := make(chan struct{})
	go func() {
		a.mu.Lock()
		close(locked)
		<-release
		a.mu.Unlock()
	}()
	<-locked

	done := make(chan error, 1)
	go func() { done <- a.OnMutation("sess-b", "some/path") }()
	select {
	case err := <-done:
		s.Require().NoError(err)
	case <-time.After(200 * time.Millisecond):
		s.Fail("OnMutation blocked on a.mu despite zero grants")
	}
	close(release)
}


func (s *ArbiterSuite) TestConcurrentContendersCoalesce() {
	fr := &fakeRecaller{block: make(chan struct{})}
	a := s.newArbiter(fr)
	a.Request("sessA", "proj", "userA", "vol", "")

	var wg sync.WaitGroup
	errs := make([]error, 2)

	// Goroutine #1: triggers the recall; blocks inside Recall on fr.block.
	wg.Add(1)
	go func() {
		defer wg.Done()
		errs[0] = a.OnMutation("sessB", "proj/x")
	}()

	// Wait until the recall is in-flight (exactly one call recorded).
	s.Eventually(func() bool {
		return fr.callCount() == 1
	}, time.Second, time.Millisecond)

	// Goroutine #2: contends on the same root while recall #1 is still blocked.
	// It must coalesce onto the in-flight recall (wait on done) and NOT fire a second recall.
	wg.Add(1)
	go func() {
		defer wg.Done()
		errs[1] = a.OnMutation("sessB", "proj/x")
	}()

	// Give goroutine #2 a moment to reach the coalesce branch, then unblock recall #1.
	time.Sleep(10 * time.Millisecond)
	close(fr.block)

	wg.Wait()

	s.NoError(errs[0])
	s.NoError(errs[1])
	s.Equal(1, fr.callCount(), "exactly one recall must fire despite two concurrent contenders")
}

// TestConcurrentContendersCoalesceFailure verifies that when the leader's recall
// FAILS, every coalesced waiter also returns a non-nil error — they must NOT
// return nil (safe to proceed) while the root is still foreign-owned.
// This is the C3 coherence fix: prevent coalesced waiters from mutating a
// still-delegated subtree.
func (s *ArbiterSuite) TestConcurrentContendersCoalesceFailure() {
	block := make(chan struct{})
	fr := &fakeRecaller{
		failOn: map[string]bool{"sessA": true},
		block:  block,
	}
	a := s.newArbiter(fr)
	a.Request("sessA", "proj", "userA", "vol", "")

	var wg sync.WaitGroup
	errs := make([]error, 2)

	// Goroutine #1: triggers the recall; the recaller blocks then fails.
	wg.Add(1)
	go func() {
		defer wg.Done()
		errs[0] = a.OnMutation("sessB", "proj/x")
	}()

	// Wait until the recall is in-flight.
	s.Eventually(func() bool {
		return fr.callCount() == 1
	}, time.Second, time.Millisecond)

	// Goroutine #2: coalesces onto the in-flight (failing) recall.
	wg.Add(1)
	go func() {
		defer wg.Done()
		errs[1] = a.OnMutation("sessB", "proj/x")
	}()

	// Give goroutine #2 a moment to enter the coalesce branch, then let the
	// recall fail.
	time.Sleep(10 * time.Millisecond)
	close(block)

	wg.Wait()

	// BOTH goroutines must return non-nil: leader because recall failed;
	// coalesced waiter because the root is still foreign-owned after the failure.
	s.Require().Error(errs[0], "leader must return error on recall failure")
	s.Require().Error(errs[1], "coalesced waiter must also return error when recall failed")
	s.Equal(1, fr.callCount(), "exactly one recall must fire despite two concurrent contenders")
}

// ---------------------------------------------------------------------------
// Task 6: gen-fencing tests
// ---------------------------------------------------------------------------

// TestReleaseSessionRevokesHeldGens is the death-path test: when a session is
// reaped (machine death + grace expiry), every held delegation gen must be
// durably revoked so that a replayed WAL from the dead holder (PR #119 reclaim)
// is fenced in Apply and cannot clobber the new owner.
func (s *ArbiterSuite) TestReleaseSessionRevokesHeldGens() {
	fr := &fakeRecaller{}
	st := newFakeStore()
	a := s.newArbiterWithStore(fr, st)

	// sessA holds two delegations with distinct gens on distinct volumes.
	g1 := a.Request("sessA", "dir1", "alice", "vol1", "")
	s.Require().NotEmpty(g1.GrantedRoot)
	gen1 := g1.Gen

	g2 := a.Request("sessA", "dir2", "alice", "vol2", "")
	s.Require().NotEmpty(g2.GrantedRoot)
	gen2 := g2.Gen

	// Simulate session reap (machine death + grace expiry path).
	a.ReleaseSession("sessA")

	// Both gens must have been revoked.
	revs := st.revokedGensCopy()
	s.Require().Len(revs, 2, "both gens must be revoked on session reap")

	// Order is not guaranteed — check by set membership.
	revokedSet := make(map[uint64]bool, len(revs))
	for _, g := range revs {
		revokedSet[g] = true
	}
	s.True(revokedSet[gen1], "gen1 must be revoked")
	s.True(revokedSet[gen2], "gen2 must be revoked")

	// Fence keys must be {principal, volume}, not session IDs.
	keySet := make(map[watermark.Key]bool, len(st.revokedKeys))
	for _, k := range st.revokedKeys {
		keySet[k] = true
	}
	s.True(keySet[watermark.Key{Identity: "alice", Volume: "vol1"}])
	s.True(keySet[watermark.Key{Identity: "alice", Volume: "vol2"}])

	// After reap, B can freely take the delegations (no recall needed).
	s.Require().NoError(a.OnMutation("sessB", "dir1/x"))
	s.Empty(fr.calls, "no recall should fire — sessA no longer owns dir1")
}

// TestGenMonotoneAndReturnedInGrant verifies that each grant receives a
// distinct, strictly increasing gen and that it is reflected in the returned
// DelegationGrant.Gen field.
func (s *ArbiterSuite) TestGenMonotoneAndReturnedInGrant() {
	fr := &fakeRecaller{}
	a := s.newArbiter(fr)

	g1 := a.Request("sessA", "dir1", "userA", "vol", "")
	s.Require().NotEmpty(g1.GrantedRoot)
	s.Positive(g1.Gen, "gen must be > 0 (0 is reserved for untagged)")

	g2 := a.Request("sessA", "dir2", "userA", "vol", "")
	s.Require().NotEmpty(g2.GrantedRoot)
	s.Greater(g2.Gen, g1.Gen, "gen must be strictly monotone per grant")
}

// TestGenDeniedGrantDoesNotAdvanceCounter verifies that a cooldown-denied
// Request does not consume a gen slot for the (identity,volume) key.
// Gens are per-(identity,volume): the same principal+volume is used throughout
// so the counter isolation is correctly exercised.
func (s *ArbiterSuite) TestGenDeniedGrantDoesNotAdvanceCounter() {
	fr := &fakeRecaller{}
	a := s.newArbiter(fr)

	// Grant and recall to trip the cooldown on "proj".
	a.Request("sessA", "proj", "userA", "vol", "")
	s.Require().NoError(a.OnMutation("sessB", "proj/x")) // recall + trip cooldown

	// Request while cooling — must be denied, gen must not advance (cooldown
	// check fires before NextGen is called, so GenHi for userA/vol stays at 1).
	denied := a.Request("sessA", "proj", "userA", "vol", "")
	s.Empty(denied.GrantedRoot, "must be denied while cooling")
	s.Equal(uint64(0), denied.Gen, "denied grant must carry gen 0")

	// Advance clock past cooldown and verify the next granted gen is
	// sequential with the first (no gap from the denied attempt).
	// Use the same principal+volume so we stay on the same per-key counter.
	s.clock = s.clock.Add(time.Hour)
	g2 := a.Request("sessB", "proj", "userA", "vol", "")
	s.Require().NotEmpty(g2.GrantedRoot)
	s.Equal(uint64(2), g2.Gen, "second successful grant must be gen 2 (denied didn't consume a slot)")
}

// TestHandoffRevokesGenBeforeContenderProceeds is the critical ordering test:
// after a recall, the revoked gen is durable (stored in the fake store) BEFORE
// the coalesced contender unblocks from rs.done.
func (s *ArbiterSuite) TestHandoffRevokesGenBeforeContenderProceeds() {
	block := make(chan struct{})
	fr := &fakeRecaller{block: block}
	st := newFakeStore()
	a := s.newArbiterWithStore(fr, st)

	g := a.Request("sessA", "proj", "userA", "myvol", "")
	s.Require().NotEmpty(g.GrantedRoot)
	revokedGen := g.Gen

	// Run the recall in a goroutine; it blocks until we close block.
	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = a.OnMutation("sessB", "proj/file")
	}()

	// Wait until the recall is in-flight.
	s.Eventually(func() bool { return fr.callCount() == 1 }, time.Second, time.Millisecond)

	// Store must NOT have seen RevokeGen yet (recall RTT still in progress).
	s.Empty(st.revokedGensCopy(), "RevokeGen must not fire before recall completes")

	// Unblock the recall — OnMutation will call RevokeGen then close rs.done.
	close(block)
	<-done

	// After OnMutation returns, RevokeGen must have been called with the
	// correct gen for the correct (identity, volume) key.
	revs := st.revokedGensCopy()
	s.Require().Len(revs, 1, "exactly one RevokeGen call expected")
	s.Equal(revokedGen, revs[0], "revoked gen must match the holder's grant gen")
	s.Equal(watermark.Key{Identity: "userA", Volume: "myvol"}, st.revokedKeys[0],
		"fence key must be {principal, volume} of the recalled holder")
}

// TestHandoffRevokesGenWithCorrectFenceKey verifies that the fence key used in
// RevokeGen matches the watermark.Key that Apply uses ({principal, volume}),
// not the session ID.
func (s *ArbiterSuite) TestHandoffRevokesGenWithCorrectFenceKey() {
	fr := &fakeRecaller{}
	st := newFakeStore()
	a := s.newArbiterWithStore(fr, st)

	const principal = "alice"
	const volume = "data"

	a.Request("sess-alice", "subtree", principal, volume, "")
	s.Require().NoError(a.OnMutation("sess-bob", "subtree/x"))

	s.Require().Len(st.revokedKeys, 1)
	s.Equal(watermark.Key{Identity: principal, Volume: volume}, st.revokedKeys[0],
		"fence key must be {principal, volume}, not {sessionID, root}")
}

// TestHandoffGenZeroNotRevoked verifies that if for some reason an entry has
// gen=0 (untagged, shouldn't happen via Request but defensive), RevokeGen is
// NOT called (gen 0 is the "no delegation gen" sentinel).
func (s *ArbiterSuite) TestHandoffGenZeroNotRevoked() {
	fr := &fakeRecaller{}
	st := newFakeStore()
	a := s.newArbiterWithStore(fr, st)

	// Manually inject a gen-0 entry (simulate a pre-fencing delegation).
	a.mu.Lock()
	a.table.entries = append(a.table.entries, entry{
		owner: "sessA", root: "proj", principal: "userA", volume: "vol", gen: 0,
	})
	a.mu.Unlock()

	s.Require().NoError(a.OnMutation("sessB", "proj/x"))

	s.Empty(st.revokedGensCopy(), "gen=0 must never be passed to RevokeGen")
}

// ---------------------------------------------------------------------------
// Task 6b: durable per-(identity,volume) gen tests
// ---------------------------------------------------------------------------

// TestArbiterUsesDurableGens verifies that the Arbiter calls store.NextGen
// (not an in-memory counter) when stamping grants. A fakeStore that already
// has GenHi advanced (simulating a prior server run) causes the Arbiter to
// stamp grants ABOVE the prior max — i.e., the Arbiter never resets to 1.
func (s *ArbiterSuite) TestArbiterUsesDurableGens() {
	fr := &fakeRecaller{}
	st := newFakeStore()

	// Pre-advance GenHi for userA/vol to 5 (simulating a prior server run).
	k := watermark.Key{Identity: "userA", Volume: "vol"}
	for i := 0; i < 5; i++ {
		_, err := st.NextGen(k)
		s.Require().NoError(err)
	}

	a := s.newArbiterWithStore(fr, st)

	// First grant after "restart" must be gen 6 (> 5), not gen 1.
	g := a.Request("sessA", "dir1", "userA", "vol", "")
	s.Require().NotEmpty(g.GrantedRoot)
	s.Equal(uint64(6), g.Gen, "post-restart gen must continue above prior GenHi (no reset to 1)")

	// Second grant on the same key must be gen 7.
	g2 := a.Request("sessA", "dir2", "userA", "vol", "")
	s.Require().NotEmpty(g2.GrantedRoot)
	s.Equal(uint64(7), g2.Gen, "subsequent grants must be strictly increasing")
}

// TestDeathPathRevokeGenErrorIsLogged verifies that a RevokeGen failure on
// the ReleaseSession path is not silently swallowed — the error path must
// reach the logger. We test this by injecting a store that returns an error
// from RevokeGen and asserting the arbiter does not panic and completes (the
// log call itself is a best-effort side-effect; correctness = no silent drop).
// The session cleanup must still complete despite the error (best-effort).
func (s *ArbiterSuite) TestDeathPathRevokeGenErrorIsLogged() {
	fr := &fakeRecaller{}
	st := &erroringRevokeStore{inner: newFakeStore()}
	a := s.newArbiterWithStore(fr, st)

	g := a.Request("sessA", "dir1", "userA", "vol", "")
	s.Require().NotEmpty(g.GrantedRoot)

	// ReleaseSession must not panic and must complete; the gen will fail to
	// revoke but cleanup proceeds (best-effort, logged).
	s.NotPanics(func() { a.ReleaseSession("sessA") })
}

// erroringRevokeStore wraps a fakeStore and returns an error from RevokeGen.
type erroringRevokeStore struct {
	inner *fakeStore
}

func (e *erroringRevokeStore) Get(k watermark.Key) (watermark.Record, error) {
	return e.inner.Get(k)
}
func (e *erroringRevokeStore) Advance(k watermark.Key, wm uint64) error {
	return e.inner.Advance(k, wm)
}
func (e *erroringRevokeStore) NextGen(k watermark.Key) (uint64, error) {
	return e.inner.NextGen(k)
}
func (e *erroringRevokeStore) RevokeGen(_ watermark.Key, _ uint64) error {
	return errInfo("injected revoke failure")
}
func (e *erroringRevokeStore) Close() error { return nil }
