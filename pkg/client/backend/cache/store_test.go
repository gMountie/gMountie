package cache

import (
	"sync/atomic"
	"testing"
	"time"

	"go.gmountie.dev/gmountie/pkg/client/metrics"

	"github.com/stretchr/testify/suite"
)

type StoreTestSuite struct {
	suite.Suite
	acct *accountant
	s    *store
}

func (s *StoreTestSuite) SetupTest() {
	s.acct = newAccountant(0, 0) // unlimited for the basic suite
	s.s = newStore(s.acct, "attr", metrics.NopRecorder{})
}

func (s *StoreTestSuite) TestPutGet() {
	s.s.put("k1", "v1", 10)
	e := s.s.get("k1")
	s.Require().NotNil(e)
	s.Assert().Equal("v1", e.value)
	s.Assert().Equal(10, e.size)
}

func (s *StoreTestSuite) TestGetMiss() {
	s.Assert().Nil(s.s.get("nope"))
}

func (s *StoreTestSuite) TestPutReplacesAndRefundsBytes() {
	s.s.put("k", "v", 100)
	s.Require().Equal(100, s.acct.Used())
	s.s.put("k", "v2", 30)
	s.Assert().Equal(30, s.acct.Used())
	s.Assert().Equal("v2", s.s.get("k").value)
}

func (s *StoreTestSuite) TestRemove() {
	s.s.put("k", "v", 50)
	s.s.remove("k")
	s.Assert().Nil(s.s.get("k"))
	s.Assert().Equal(0, s.acct.Used())
}

func (s *StoreTestSuite) TestRemoveMatching() {
	s.s.put("/a/x", "v1", 10)
	s.s.put("/a/y", "v2", 10)
	s.s.put("/b/z", "v3", 10)
	s.s.removeMatching(func(k string) bool { return len(k) >= 2 && k[:2] == "/a" })
	s.Assert().Nil(s.s.get("/a/x"))
	s.Assert().Nil(s.s.get("/a/y"))
	s.Assert().NotNil(s.s.get("/b/z"))
	s.Assert().Equal(10, s.acct.Used())
}

func (s *StoreTestSuite) TestConcurrentReadsRaceClean() {
	s.s.put("k", "v", 10)
	done := make(chan struct{})
	for i := 0; i < 8; i++ {
		go func() {
			for j := 0; j < 1000; j++ {
				_ = s.s.get("k")
			}
			done <- struct{}{}
		}()
	}
	for i := 0; i < 8; i++ {
		<-done
	}
	s.Assert().Equal(10, s.acct.Used())
}

func TestStoreTestSuite(t *testing.T) {
	suite.Run(t, new(StoreTestSuite))
}

// PersistedStoreSuite exercises the memory-tier-above-disk fallthrough
// added by Sub-spec C. Memory hit returns immediately. Memory miss
// falls through to disk via the configured Loader/Putter pair; a disk
// hit promotes the value back into the memory tier so subsequent gets
// short-circuit.
type PersistedStoreSuite struct {
	suite.Suite
}

func (s *PersistedStoreSuite) TestMemoryMissFallsThroughToLoader() {
	loaderCalls := 0
	loader := func(key string) (any, int, bool) {
		loaderCalls++
		if key == "k1" {
			return "from-disk", 9, true
		}
		return nil, 0, false
	}
	acct := newAccountant(0, 0)
	st := newStoreWithPersist(acct, loader, func(string, any, int) {}, nil, "attr", metrics.NopRecorder{})

	e := st.get("k1")
	s.Require().NotNil(e)
	s.Assert().Equal("from-disk", e.value)
	s.Assert().Equal(1, loaderCalls)

	e2 := st.get("k1")
	s.Require().NotNil(e2)
	s.Assert().Equal(1, loaderCalls, "loader must not be called for memory hit")
}

// TestPromoteDoesNotWriteThrough pins the variant-A half of the async-persist
// stale-read fix: a loader (disk) hit promotes into the MEMORY tier only and
// must NOT write back through the putter. Re-persisting disk-sourced bytes is
// pointless (they are already on disk) and, under a racing invalidation, would
// resurrect a chunk the cleaner just removed.
func (s *PersistedStoreSuite) TestPromoteDoesNotWriteThrough() {
	var putCalls int
	loader := func(key string) (any, int, bool) {
		if key == "k1" {
			return "from-disk", 9, true
		}
		return nil, 0, false
	}
	putter := func(string, any, int) { putCalls++ }
	st := newStoreWithPersist(newAccountant(0, 0), loader, putter, nil, "attr", metrics.NopRecorder{})

	e := st.get("k1") // memory miss -> loader hit -> promote
	s.Require().NotNil(e)
	s.Assert().Equal("from-disk", e.value)
	s.Assert().Equal(0, putCalls, "loader-promote must be memory-only (no write-through)")

	e2 := st.get("k1") // now a memory hit (promote populated the memory tier)
	s.Require().NotNil(e2)
	s.Assert().Equal("from-disk", e2.value)
}

func (s *PersistedStoreSuite) TestPutAlsoWritesThrough() {
	var putCalls int
	loader := func(string) (any, int, bool) { return nil, 0, false }
	putter := func(_ string, _ any, _ int) { putCalls++ }
	st := newStoreWithPersist(newAccountant(0, 0), loader, putter, nil, "attr", metrics.NopRecorder{})
	st.put("k", "v", 1)
	s.Assert().Equal(1, putCalls, "write-through must call putter")
}

func (s *PersistedStoreSuite) TestRemoveForwardsToRemover() {
	var removerCalls int
	loader := func(string) (any, int, bool) { return nil, 0, false }
	putter := func(string, any, int) {}
	remover := func(string) { removerCalls++ }
	st := newStoreWithPersist(newAccountant(0, 0), loader, putter, remover, "attr", metrics.NopRecorder{})
	st.put("k", "v", 1)
	st.remove("k")
	s.Assert().Equal(1, removerCalls)
}

func TestPersistedStoreSuite(t *testing.T) { suite.Run(t, new(PersistedStoreSuite)) }

// AsyncPersistStoreSuite exercises the async write-back persist path: when a
// store has startAsyncPersist enabled, put() must populate the memory tier
// synchronously but dispatch the (slow, fsync-heavy) putter to a background
// worker so the read path never blocks on disk. Close() flushes pending jobs.
type AsyncPersistStoreSuite struct {
	suite.Suite
}

func newAsyncStore(putter Putter, depth int) *store {
	st := newStoreWithPersist(newAccountant(0, 0), func(string) (any, int, bool) { return nil, 0, false }, putter, nil, "data", metrics.NopRecorder{})
	st.startAsyncPersist(depth)
	return st
}

// TestPutDoesNotBlockOnSlowPutter: a put must return even while the putter is
// still running (the whole point — the cold-read regression was put() blocking
// on a per-chunk fsync).
func (s *AsyncPersistStoreSuite) TestPutDoesNotBlockOnSlowPutter() {
	release := make(chan struct{})
	var calls int32
	putter := func(string, any, int) {
		<-release
		atomic.AddInt32(&calls, 1)
	}
	st := newAsyncStore(putter, 8)

	done := make(chan struct{})
	go func() { st.put("k", "v", 1); close(done) }()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		s.FailNow("put blocked on the slow putter")
	}
	s.Equal(int32(0), atomic.LoadInt32(&calls), "putter must not have completed before put returned")

	close(release)
	st.Close() // drains the one in-flight job
	s.Equal(int32(1), atomic.LoadInt32(&calls))
}

// TestMemoryTierPopulatedSynchronously: hits must stay fast — the value is in
// the memory tier immediately, before the async persist runs.
func (s *AsyncPersistStoreSuite) TestMemoryTierPopulatedSynchronously() {
	release := make(chan struct{})
	st := newAsyncStore(func(string, any, int) { <-release }, 8)
	st.put("k", "v", 1)
	e := st.get("k")
	s.Require().NotNil(e, "memory tier must hold the entry synchronously")
	s.Equal("v", e.value)
	close(release)
	st.Close()
}

// TestCloseFlushesPending: Close must drain buffered jobs so a clean unmount
// still persists what was cached (cross-mount cache effectiveness).
func (s *AsyncPersistStoreSuite) TestCloseFlushesPending() {
	var calls int32
	st := newAsyncStore(func(string, any, int) { atomic.AddInt32(&calls, 1) }, 64)
	for i := 0; i < 20; i++ {
		st.put(string(rune('a'+i)), "v", 1)
	}
	st.Close()
	s.Equal(int32(20), atomic.LoadInt32(&calls), "Close must flush all buffered persists")
}

// TestDropOnFullNeverBlocks: when the worker can't keep up (streaming overload),
// puts must drop the persist rather than block the read path.
func (s *AsyncPersistStoreSuite) TestDropOnFullNeverBlocks() {
	release := make(chan struct{})
	st := newAsyncStore(func(string, any, int) { <-release }, 2) // tiny buffer; worker wedged
	for i := 0; i < 50; i++ {
		done := make(chan struct{})
		go func() { st.put("k", "v", 1); close(done) }()
		select {
		case <-done:
		case <-time.After(time.Second):
			s.FailNow("put blocked when the persist queue was full")
		}
	}
	close(release)
	st.Close()
}

// TestGenCapturedAtEnqueue pins the core of the async-persist stale-read fix:
// the persist job must carry the path generation seen at ENQUEUE time, so that
// an invalidation which advances the generation before the worker runs is
// detectable (the data cache drops/undoes such a job in onPersist). We hold the
// worker, advance the generation after the put, then release: the job's gen must
// still be the pre-advance value, not the current one.
func (s *AsyncPersistStoreSuite) TestGenCapturedAtEnqueue() {
	var gen atomic.Uint64
	release := make(chan struct{})
	gotGen := make(chan uint64, 1)
	st := newStoreWithPersist(newAccountant(0, 0),
		func(string) (any, int, bool) { return nil, 0, false }, nil, nil, "data", metrics.NopRecorder{})
	st.genOf = func(string) uint64 { return gen.Load() }
	st.onPersist = func(job persistJob) {
		<-release
		gotGen <- job.gen
	}
	st.startAsyncPersist(8)

	st.put("k", "v", 1) // enqueued while gen == 0
	gen.Add(1)          // an invalidation advances the generation before the worker runs
	close(release)
	st.Close() // drains the in-flight job

	select {
	case g := <-gotGen:
		s.Equal(uint64(0), g,
			"job must carry the generation captured at enqueue (0), not the post-invalidation value (1)")
	default:
		s.FailNow("onPersist was never called")
	}
}

// TestOnPersistReplacesPutter: when onPersist is set, the worker must route jobs
// through it (not the plain putter), so the data cache's generation-aware
// write-through is actually used.
func (s *AsyncPersistStoreSuite) TestOnPersistReplacesPutter() {
	var putterCalls, onPersistCalls int32
	st := newStoreWithPersist(newAccountant(0, 0),
		func(string) (any, int, bool) { return nil, 0, false },
		func(string, any, int) { atomic.AddInt32(&putterCalls, 1) }, nil, "data", metrics.NopRecorder{})
	st.onPersist = func(persistJob) { atomic.AddInt32(&onPersistCalls, 1) }
	st.startAsyncPersist(8)
	st.put("k", "v", 1)
	st.Close()
	s.Equal(int32(1), atomic.LoadInt32(&onPersistCalls), "onPersist must handle the job")
	s.Equal(int32(0), atomic.LoadInt32(&putterCalls), "putter must not be called when onPersist is set")
}

func TestAsyncPersistStoreSuite(t *testing.T) { suite.Run(t, new(AsyncPersistStoreSuite)) }
