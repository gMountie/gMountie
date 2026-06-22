package service

import (
	"context"
	"errors"
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/hanwen/go-fuse/v2/fuse/nodefs"
	"github.com/stretchr/testify/suite"
)

type SessionManagerTestSuite struct {
	suite.Suite
	mgr SessionManager
}

func (s *SessionManagerTestSuite) SetupTest() {
	s.mgr = NewSessionManager(SessionManagerOptions{
		GracePeriod: 100 * time.Millisecond,
	})
}

func (s *SessionManagerTestSuite) TestCreateReturnsUniqueIDs() {
	id1, err := s.mgr.Create("test-user", "")
	s.Require().NoError(err)
	s.Require().NotEmpty(id1)

	id2, err := s.mgr.Create("test-user", "")
	s.Require().NoError(err)
	s.Assert().NotEqual(id1, id2)
}

func (s *SessionManagerTestSuite) TestGetReturnsTheSession() {
	id, err := s.mgr.Create("test-user", "")
	s.Require().NoError(err)

	sess, err := s.mgr.Get(id)
	s.Require().NoError(err)
	s.Assert().Equal(id, sess.ID())
}

func (s *SessionManagerTestSuite) TestGetUnknownSessionErrors() {
	_, err := s.mgr.Get("does-not-exist")
	s.Assert().Error(err)
}

func (s *SessionManagerTestSuite) TestSessionFdTableRegisterAndLookup() {
	id, err := s.mgr.Create("test-user", "")
	s.Require().NoError(err)
	sess, err := s.mgr.Get(id)
	s.Require().NoError(err)

	fd := sess.RegisterFile("vol", "/some/path", nodefs.NewDefaultFile())
	s.Assert().NotZero(fd)

	entry, ok := sess.GetFile(fd)
	s.Assert().True(ok)
	s.Assert().Equal("/some/path", entry.Path)
}

func (s *SessionManagerTestSuite) TestSessionReleaseFile() {
	id, err := s.mgr.Create("test-user", "")
	s.Require().NoError(err)
	sess, _ := s.mgr.Get(id)
	fd := sess.RegisterFile("vol", "/p", nodefs.NewDefaultFile())

	sess.ReleaseFile(fd)
	_, ok := sess.GetFile(fd)
	s.Assert().False(ok)
}

func (s *SessionManagerTestSuite) TestDisconnectThenGraceExpiryReapsFds() {
	id, err := s.mgr.Create("test-user", "")
	s.Require().NoError(err)
	sess, _ := s.mgr.Get(id)
	fd := sess.RegisterFile("vol", "/p", nodefs.NewDefaultFile())

	s.mgr.MarkDisconnected(id)

	// Wait well past the 100ms grace period configured in SetupTest.
	s.Require().Eventually(func() bool {
		_, err := s.mgr.Get(id)
		return err != nil
	}, 500*time.Millisecond, 10*time.Millisecond, "session should be reaped after grace period")

	_ = fd // fd entry is now unreachable through Get; that is the assertion.
}

func (s *SessionManagerTestSuite) TestResumeBeforeGraceCancelsReap() {
	id, err := s.mgr.Create("test-user", "")
	s.Require().NoError(err)
	sess, _ := s.mgr.Get(id)
	fd := sess.RegisterFile("vol", "/p", nodefs.NewDefaultFile())

	s.mgr.MarkDisconnected(id)

	// Resume immediately — well within the 100ms grace period.
	resumed, err := s.mgr.Resume(id)
	s.Require().NoError(err)
	s.Require().True(resumed)

	// Wait long enough that the original timer *would* have fired.
	time.Sleep(200 * time.Millisecond)

	sess2, err := s.mgr.Get(id)
	s.Require().NoError(err)
	entry, ok := sess2.GetFile(fd)
	s.Require().True(ok)
	s.Assert().Equal("/p", entry.Path)
}

func (s *SessionManagerTestSuite) TestResumeUnknownSessionReturnsFalse() {
	resumed, err := s.mgr.Resume("nope")
	s.Require().NoError(err)
	s.Assert().False(resumed)
}

func (s *SessionManagerTestSuite) TestStopReleasesAllFds() {
	id, err := s.mgr.Create("test-user", "")
	s.Require().NoError(err)
	sess, _ := s.mgr.Get(id)
	_ = sess.RegisterFile("vol", "/p", nodefs.NewDefaultFile())

	err = s.mgr.Stop(context.Background())
	s.Require().NoError(err)

	_, err = s.mgr.Get(id)
	s.Assert().Error(err)
}

func (s *SessionManagerTestSuite) TestDoOnceCachesSuccessfulReply() {
	id, _ := s.mgr.Create("test-user", "")
	sess, _ := s.mgr.Get(id)

	calls := 0
	fn := func() (any, error) {
		calls++
		return "reply-1", nil
	}

	r1, err := sess.DoOnce("req-A", fn)
	s.Require().NoError(err)
	s.Assert().Equal("reply-1", r1)

	r2, err := sess.DoOnce("req-A", fn)
	s.Require().NoError(err)
	s.Assert().Equal("reply-1", r2)

	s.Assert().Equal(1, calls, "fn must execute only once for the same request_id")
}

func (s *SessionManagerTestSuite) TestDoOnceDoesNotCacheErrors() {
	id, _ := s.mgr.Create("test-user", "")
	sess, _ := s.mgr.Get(id)

	calls := 0
	fn := func() (any, error) {
		calls++
		if calls == 1 {
			return nil, errors.New("first fails")
		}
		return "reply-ok", nil
	}

	_, err := sess.DoOnce("req-B", fn)
	s.Require().Error(err)

	r, err := sess.DoOnce("req-B", fn)
	s.Require().NoError(err)
	s.Assert().Equal("reply-ok", r)
	s.Assert().Equal(2, calls, "errored fn must re-execute on retry with same request_id")
}

func (s *SessionManagerTestSuite) TestDoOnceCollapsesConcurrentDuplicates() {
	id, _ := s.mgr.Create("test-user", "")
	sess, _ := s.mgr.Get(id)

	var mu sync.Mutex
	calls := 0
	block := make(chan struct{})
	fn := func() (any, error) {
		mu.Lock()
		calls++
		mu.Unlock()
		<-block
		return "reply", nil
	}

	type result struct {
		v   any
		err error
	}
	results := make(chan result, 5)
	for i := 0; i < 5; i++ {
		go func() {
			v, err := sess.DoOnce("req-C", fn)
			results <- result{v, err}
		}()
	}

	// Give the goroutines a moment to all enter DoOnce and queue on singleflight.
	time.Sleep(50 * time.Millisecond)
	close(block)

	for i := 0; i < 5; i++ {
		r := <-results
		s.Require().NoError(r.err)
		s.Assert().Equal("reply", r.v)
	}

	mu.Lock()
	defer mu.Unlock()
	s.Assert().Equal(1, calls, "fn must run exactly once even with 5 concurrent callers using the same request_id")
}

func (s *SessionManagerTestSuite) TestDoOnceLRUEvictsOldEntries() {
	// Use an explicit small cache so the test doesn't need to fill 4096 entries.
	m := NewSessionManager(SessionManagerOptions{IdempotencyCacheSize: 256})
	defer func() { _ = m.Stop(context.Background()) }()
	id, _ := m.Create("test-user", "")
	sess, _ := m.Get(id)

	// Saturate the LRU (256 entries) and verify the first one is gone.
	for i := 0; i < 300; i++ {
		reqID := fmt.Sprintf("req-%d", i)
		_, err := sess.DoOnce(reqID, func() (any, error) { return i, nil })
		s.Require().NoError(err)
	}

	// req-0 should be evicted by now; calling DoOnce with it re-executes.
	calls := 0
	_, err := sess.DoOnce("req-0", func() (any, error) {
		calls++
		return 999, nil
	})
	s.Require().NoError(err)
	s.Assert().Equal(1, calls, "evicted request_id must re-execute")
}

// releaseRecorder is a nodefs.File stub that records Release calls. inFlight,
// when set, lets Release detect overlap with an op that still holds a ref —
// the exact race the FileEntry refcount exists to prevent.
type releaseRecorder struct {
	nodefs.File
	releases             atomic.Int32
	inFlight             *atomic.Int32
	releasedWithInFlight atomic.Int32
}

func (r *releaseRecorder) Release() {
	if r.inFlight != nil && r.inFlight.Load() != 0 {
		r.releasedWithInFlight.Add(1)
	}
	r.releases.Add(1)
}

// TestReleaseFileWaitsForInFlightRef: ReleaseFile must only drop the table
// ref — the underlying File closes when the last in-flight op releases its
// ref, never under it.
func (s *SessionManagerTestSuite) TestReleaseFileWaitsForInFlightRef() {
	id, err := s.mgr.Create("test-user", "")
	s.Require().NoError(err)
	sess, _ := s.mgr.Get(id)

	rec := &releaseRecorder{File: nodefs.NewDefaultFile()}
	fd := sess.RegisterFile("vol", "/p", rec)
	entry, ok := sess.GetFile(fd)
	s.Require().True(ok)
	s.Require().True(entry.Acquire(), "live entry must grant a ref")

	sess.ReleaseFile(fd) // table ref dropped; the op above still holds one
	s.Equal(int32(0), rec.releases.Load(), "must not close under an in-flight op")
	_, ok = sess.GetFile(fd)
	s.False(ok, "fd must leave the table immediately on ReleaseFile")

	entry.ReleaseRef()
	s.Equal(int32(1), rec.releases.Load(), "last ref out closes the file")
	s.False(entry.Acquire(), "fully released entry must refuse new refs")
}

// TestReleaseAllWaitsForInFlightRef: a reap (grace expiry / revocation /
// shutdown all funnel through ReleaseAll) must likewise defer the close to
// the in-flight op.
func (s *SessionManagerTestSuite) TestReleaseAllWaitsForInFlightRef() {
	id, err := s.mgr.Create("test-user", "")
	s.Require().NoError(err)
	sess, _ := s.mgr.Get(id)

	rec := &releaseRecorder{File: nodefs.NewDefaultFile()}
	fd := sess.RegisterFile("vol", "/p", rec)
	entry, ok := sess.GetFile(fd)
	s.Require().True(ok)
	s.Require().True(entry.Acquire())

	s.Equal(1, sess.ReleaseAll(), "ReleaseAll must still count the fd as released")
	s.Equal(int32(0), rec.releases.Load(), "reap must not close under an in-flight op")

	entry.ReleaseRef()
	s.Equal(int32(1), rec.releases.Load(), "last ref out closes the file")
}

// TestConcurrentOpsNeverRaceWithRelease hammers Acquire/ReleaseRef from many
// goroutines while ReleaseFile runs. Under -race: File.Release must be
// observed exactly once and never while any op still holds a ref.
func (s *SessionManagerTestSuite) TestConcurrentOpsNeverRaceWithRelease() {
	id, err := s.mgr.Create("test-user", "")
	s.Require().NoError(err)
	sess, _ := s.mgr.Get(id)

	var inFlight atomic.Int32
	rec := &releaseRecorder{File: nodefs.NewDefaultFile(), inFlight: &inFlight}
	fd := sess.RegisterFile("vol", "/p", rec)
	entry, ok := sess.GetFile(fd)
	s.Require().True(ok)

	const workers = 16
	start := make(chan struct{})
	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			for j := 0; j < 1000; j++ {
				if !entry.Acquire() {
					return // entry fully released; no new ops may begin
				}
				// Hold the in-flight window open across a scheduling point so
				// a close-under-live-ref has a real chance to be observed.
				// The detector is still best-effort — the load-bearing
				// assertion is releases == 1.
				inFlight.Add(1)
				runtime.Gosched()
				inFlight.Add(-1)
				entry.ReleaseRef()
			}
		}()
	}
	wg.Add(1)
	go func() {
		defer wg.Done()
		<-start
		sess.ReleaseFile(fd)
	}()
	close(start)
	wg.Wait()

	s.Equal(int32(1), rec.releases.Load(), "File.Release must run exactly once")
	s.Equal(int32(0), rec.releasedWithInFlight.Load(), "File.Release must never overlap an op holding a ref")
}

func (s *SessionManagerTestSuite) TestIdempotencyCacheSizeConfigurable() {
	m := NewSessionManager(SessionManagerOptions{IdempotencyCacheSize: 2})
	id, err := m.Create("p", "")
	s.Require().NoError(err)
	sess, err := m.Get(id)
	s.Require().NoError(err)
	defer func() { _ = m.Stop(context.Background()) }()

	for i := 0; i < 3; i++ {
		_, _ = sess.DoOnce(fmt.Sprintf("r%d", i), func() (any, error) { return i, nil })
	}
	// r0 was evicted when i=2 was inserted into a size-2 cache: fn must re-execute.
	calls := 0
	_, err = sess.DoOnce("r0", func() (any, error) { calls++; return 0, nil })
	s.Require().NoError(err)
	s.Equal(1, calls, "r0 evicted at size 2 — fn re-executes")
}

func (s *SessionManagerTestSuite) TearDownTest() {
	_ = s.mgr.Stop(context.Background())
}

func TestSessionManagerTestSuite(t *testing.T) {
	suite.Run(t, new(SessionManagerTestSuite))
}

// fakeSessionMetrics records the open-files gauge per volume so tests can pin
// the session-layer accounting (OBS-6 / CQ-12).
type fakeSessionMetrics struct {
	mu     sync.Mutex
	open   map[string]int
	reaped map[string]int
	active int
}

func newFakeSessionMetrics() *fakeSessionMetrics {
	return &fakeSessionMetrics{open: map[string]int{}, reaped: map[string]int{}}
}
func (f *fakeSessionMetrics) SessionsActiveInc() { f.mu.Lock(); f.active++; f.mu.Unlock() }
func (f *fakeSessionMetrics) SessionsActiveDec() { f.mu.Lock(); f.active--; f.mu.Unlock() }
func (f *fakeSessionMetrics) SessionsReapedInc(reason string) {
	f.mu.Lock()
	f.reaped[reason]++
	f.mu.Unlock()
}
func (f *fakeSessionMetrics) reapedFor(reason string) int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.reaped[reason]
}
func (f *fakeSessionMetrics) OpenFilesInc(volume string) {
	f.mu.Lock()
	f.open[volume]++
	f.mu.Unlock()
}
func (f *fakeSessionMetrics) OpenFilesDec(volume string) {
	f.mu.Lock()
	f.open[volume]--
	f.mu.Unlock()
}
func (f *fakeSessionMetrics) openFor(volume string) int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.open[volume]
}

// TestOpenFilesGaugeFollowsRegisterAndRelease: the gauge moves on actual fd
// table changes only — a retried ReleaseFile of an already-released fd must
// NOT double-decrement.
func (s *SessionManagerTestSuite) TestOpenFilesGaugeFollowsRegisterAndRelease() {
	fm := newFakeSessionMetrics()
	mgr := NewSessionManager(SessionManagerOptions{Metrics: fm})
	defer func() { _ = mgr.Stop(context.Background()) }()

	id, err := mgr.Create("test-user", "")
	s.Require().NoError(err)
	sess, _ := mgr.Get(id)

	fd := sess.RegisterFile("photos", "/p", nodefs.NewDefaultFile())
	s.Equal(1, fm.openFor("photos"))

	sess.ReleaseFile(fd)
	s.Equal(0, fm.openFor("photos"))

	// Retried Release: fd already gone — gauge must not go negative.
	sess.ReleaseFile(fd)
	s.Equal(0, fm.openFor("photos"), "retried Release must not double-decrement")
}

// TestOpenFilesGaugeDecrementsOnReap: a session reaped by grace expiry (or
// ReapIf/Stop, which share ReleaseAll) must return its fds to the gauge —
// previously only the explicit Release RPC decremented and the gauge stayed
// permanently elevated after every reap.
func (s *SessionManagerTestSuite) TestOpenFilesGaugeDecrementsOnReap() {
	fm := newFakeSessionMetrics()
	mgr := NewSessionManager(SessionManagerOptions{Metrics: fm, GracePeriod: 50 * time.Millisecond})
	defer func() { _ = mgr.Stop(context.Background()) }()

	id, err := mgr.Create("test-user", "")
	s.Require().NoError(err)
	sess, _ := mgr.Get(id)
	sess.RegisterFile("photos", "/a", nodefs.NewDefaultFile())
	sess.RegisterFile("docs", "/b", nodefs.NewDefaultFile())
	s.Equal(1, fm.openFor("photos"))
	s.Equal(1, fm.openFor("docs"))

	mgr.MarkDisconnected(id)
	s.Require().Eventually(func() bool {
		return fm.openFor("photos") == 0 && fm.openFor("docs") == 0
	}, time.Second, 10*time.Millisecond, "reap must decrement the per-volume open-files gauge")
}

// TestRegisterFileDuringReapNeverLeaks is the regression lock for the
// RegisterFile-vs-reap race (CQ-H/CQ-M1). A RegisterFile that publishes its
// entry just as a concurrent reap drains the fd table must not leave an orphaned
// entry behind (a leaked nodefs.File plus a permanently elevated open-files
// gauge). The publish-then-recheck in RegisterFile must net every registration:
// each fd is either drained by ReleaseAll or self-removed on the recheck. We
// hammer RegisterFile against a reap over many rounds and assert the per-volume
// gauge nets to zero and no fd entry survives. Run under -race. Against the old
// check-then-act guard a late Store could survive the drain and this fails.
func (s *SessionManagerTestSuite) TestRegisterFileDuringReapNeverLeaks() {
	const rounds, perRound = 200, 40
	for round := 0; round < rounds; round++ {
		fm := newFakeSessionMetrics()
		mgr := NewSessionManager(SessionManagerOptions{Metrics: fm})
		id, err := mgr.Create("u", "")
		s.Require().NoError(err)
		sess, err := mgr.Get(id)
		s.Require().NoError(err)
		impl := sess.(*sessionImpl)

		var wg sync.WaitGroup
		wg.Add(2)
		// Registrar: spam RegisterFile straddling the reap.
		go func() {
			defer wg.Done()
			for i := 0; i < perRound; i++ {
				sess.RegisterFile("vol", "/p", nodefs.NewDefaultFile())
			}
		}()
		// Reaper: win the claim and drain concurrently (same sequence the manager
		// uses: set reaped BEFORE draining the fd table).
		go func() {
			defer wg.Done()
			impl.reaped.Store(true)
			impl.ReleaseAll()
		}()
		wg.Wait()

		s.Require().Equalf(0, fm.openFor("vol"),
			"round %d: open-files gauge must net to zero after the reap race", round)
		live := 0
		impl.files.Range(func(uint64, *FileEntry) bool { live++; return true })
		s.Require().Equalf(0, live, "round %d: no fd entry may survive a reap", round)
		_ = mgr.Stop(context.Background())
	}
}

// TestSessionsReapedCounterByReason: a grace-expiry reap bumps the reaped
// counter under the "grace_expired" reason so the reap rate is observable per
// cause (OB-L1).
func (s *SessionManagerTestSuite) TestSessionsReapedCounterByReason() {
	fm := newFakeSessionMetrics()
	mgr := NewSessionManager(SessionManagerOptions{Metrics: fm, GracePeriod: 50 * time.Millisecond})
	defer func() { _ = mgr.Stop(context.Background()) }()

	id, err := mgr.Create("test-user", "")
	s.Require().NoError(err)
	mgr.MarkDisconnected(id)

	s.Require().Eventually(func() bool {
		return fm.reapedFor("grace_expired") == 1
	}, time.Second, 10*time.Millisecond, "a grace-expiry reap must bump the reaped counter for grace_expired")
}
