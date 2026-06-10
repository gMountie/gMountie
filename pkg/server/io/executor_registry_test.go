package io

import (
	"sync"
	"sync/atomic"
	"testing"

	"github.com/pkg/errors"
	"github.com/stretchr/testify/suite"
)

// ExecutorRegistrySuite tests the process-wide executor registry through the
// build seam — no real workers are started, so no credential seams and no
// permanently pinned threads are involved. The executor's own behaviour is
// covered by IdentityExecutorSuite.
type ExecutorRegistrySuite struct {
	suite.Suite

	reg    *executorRegistry
	builds atomic.Int64
}

func TestExecutorRegistrySuite(t *testing.T) { suite.Run(t, new(ExecutorRegistrySuite)) }

func (s *ExecutorRegistrySuite) SetupTest() {
	s.builds.Store(0)
	s.reg = newExecutorRegistry()
	// Fake build: a distinct executor value per construction, no goroutines.
	s.reg.build = func(Identity, int) (*identityExecutor, error) {
		s.builds.Add(1)
		return &identityExecutor{}, nil
	}
}

// TestSameTupleSharesExecutor: the registry's whole point — every caller with
// the same identity tuple (across volumes/principals) gets ONE executor.
func (s *ExecutorRegistrySuite) TestSameTupleSharesExecutor() {
	id := Identity{Uid: 1000, Gid: 1000, Gids: []uint32{1000, 2000}}
	e1 := s.reg.executorFor(id, 2)
	e2 := s.reg.executorFor(id, 2)
	s.Require().NotNil(e1)
	s.Same(e1, e2)
	s.EqualValues(1, s.builds.Load(), "one construction per identity tuple")
}

// TestDifferentGidsDistinctExecutors: the supplementary-group set is part of
// the worker thread state, so it MUST be part of the key.
func (s *ExecutorRegistrySuite) TestDifferentGidsDistinctExecutors() {
	e1 := s.reg.executorFor(Identity{Uid: 1000, Gid: 1000, Gids: []uint32{1000}}, 2)
	e2 := s.reg.executorFor(Identity{Uid: 1000, Gid: 1000, Gids: []uint32{1000, 2000}}, 2)
	s.Require().NotNil(e1)
	s.Require().NotNil(e2)
	s.NotSame(e1, e2)
	s.EqualValues(2, s.builds.Load())
}

// TestGidsOrderNormalized: the same group SET in a different order describes
// identical thread state and must share an executor.
func (s *ExecutorRegistrySuite) TestGidsOrderNormalized() {
	e1 := s.reg.executorFor(Identity{Uid: 1000, Gid: 1000, Gids: []uint32{2000, 1000}}, 2)
	e2 := s.reg.executorFor(Identity{Uid: 1000, Gid: 1000, Gids: []uint32{1000, 2000}}, 2)
	s.Same(e1, e2)
	s.EqualValues(1, s.builds.Load())
}

// TestCapModePartOfKey: dac modes leave fsuid 0 (and dac_read_search reduces
// effective caps) — different thread state than the unprivileged switch, so
// each applyCreds dispatch mode gets its own executor.
func (s *ExecutorRegistrySuite) TestCapModePartOfKey() {
	base := Identity{Uid: 1000, Gid: 1000, Gids: []uint32{1000}}
	override := base
	override.Caps = []string{CapDacOverride}
	readSearch := base
	readSearch.Caps = []string{CapDacReadSearch}

	e1 := s.reg.executorFor(base, 2)
	e2 := s.reg.executorFor(override, 2)
	e3 := s.reg.executorFor(readSearch, 2)
	s.NotSame(e1, e2)
	s.NotSame(e1, e3)
	s.NotSame(e2, e3)
	s.EqualValues(3, s.builds.Load())
}

// TestConcurrentFirstUseBuildsOnce: 50 goroutines race the first lookup of
// one tuple — exactly one construction, everyone gets the same instance.
func (s *ExecutorRegistrySuite) TestConcurrentFirstUseBuildsOnce() {
	id := Identity{Uid: 3000, Gid: 3000, Gids: []uint32{3000}}
	var wg sync.WaitGroup
	results := make([]*identityExecutor, 50)
	for i := range results {
		wg.Add(1)
		go func() {
			defer wg.Done()
			results[i] = s.reg.executorFor(id, 2)
		}()
	}
	wg.Wait()
	s.EqualValues(1, s.builds.Load(), "concurrent first use must collapse to one construction")
	for _, e := range results {
		s.Same(results[0], e)
	}
}

// TestStartupFailureCachesNil: a failed credential switch is deterministic
// (privilege), so the nil result is cached — no rebuild storm, callers keep
// falling back to per-op switching.
func (s *ExecutorRegistrySuite) TestStartupFailureCachesNil() {
	s.reg.build = func(Identity, int) (*identityExecutor, error) {
		s.builds.Add(1)
		return nil, errors.New("boom")
	}
	id := Identity{Uid: 1000, Gid: 1000, Gids: []uint32{1000}}
	s.Nil(s.reg.executorFor(id, 2))
	s.Nil(s.reg.executorFor(id, 2))
	s.EqualValues(1, s.builds.Load(), "failure must be cached, not retried (and warned once)")
}

// TestZeroWorkersDisabled: workers == 0 means the executor is disabled —
// executorFor returns nil immediately without touching the registry or build.
func (s *ExecutorRegistrySuite) TestZeroWorkersDisabled() {
	e := s.reg.executorFor(Identity{Uid: 1, Gid: 1, Gids: []uint32{1}}, 0)
	s.Nil(e, "workers=0 must disable the executor")
	s.EqualValues(0, s.builds.Load(), "no build must occur when executor is disabled")
}

// TestExplicitWorkerCountPassesThrough: a positive worker count flows to build.
func (s *ExecutorRegistrySuite) TestExplicitWorkerCountPassesThrough() {
	var got int
	s.reg.build = func(_ Identity, workers int) (*identityExecutor, error) {
		got = workers
		return &identityExecutor{}, nil
	}
	s.reg.executorFor(Identity{Uid: 2, Gid: 2, Gids: []uint32{2}}, 7)
	s.Equal(7, got, "an explicit worker count must pass through to build")
}
