package io

import (
	"testing"

	"github.com/stretchr/testify/suite"
)

type ReadaheadTestSuite struct {
	suite.Suite
}

func (s *ReadaheadTestSuite) TestObserve_SequentialOffsetsTriggerPrefetch() {
	r := NewReadahead(4096, 3)

	// First two sequential reads do not arm yet.
	_, ok := r.Observe(0, 4096)
	s.Require().False(ok)
	_, ok = r.Observe(4096, 4096)
	s.Require().False(ok)

	// Third sequential read meets the threshold.
	prefetchOff, ok := r.Observe(8192, 4096)
	s.Require().True(ok, "third sequential observation should arm a prefetch")
	s.Assert().Equal(int64(12288), prefetchOff)
}

func (s *ReadaheadTestSuite) TestObserve_BackwardSeekResetsState() {
	r := NewReadahead(4096, 3)

	_, _ = r.Observe(0, 4096)
	_, _ = r.Observe(4096, 4096)
	_, _ = r.Observe(8192, 4096)

	// Backward seek to 0 — must reset seqHits.
	_, ok := r.Observe(0, 4096)
	s.Require().False(ok, "backward seek must not immediately re-arm prefetch")
}

func (s *ReadaheadTestSuite) TestObserve_NoConsecutiveTriggerWhilePrefetchOutstanding() {
	r := NewReadahead(4096, 3)

	_, _ = r.Observe(0, 4096)
	_, _ = r.Observe(4096, 4096)
	prefetchOff, ok := r.Observe(8192, 4096)
	s.Require().True(ok)
	s.Require().Equal(int64(12288), prefetchOff)

	// Simulate the prefetch goroutine storing the result.
	r.Store(12288, make([]byte, 4096))

	// A further sequential observation must NOT trigger another prefetch
	// while a prefetched buffer is still pending.
	_, ok = r.Observe(12288, 4096)
	// Note: Observe(12288, 4096) consumes via lastOffset update — but the
	// outstanding buffer should still block re-arming.
	s.Require().False(ok, "must not arm a new prefetch while one is outstanding")
}

func (s *ReadaheadTestSuite) TestServe_FullRangeHitReturnsBytesAndConsumesRing() {
	r := NewReadahead(4096, 3)
	stored := make([]byte, 4096)
	for i := range stored {
		stored[i] = byte(i % 251)
	}
	r.Store(12288, stored)

	dest := make([]byte, 1024)
	n, hit := r.Serve(dest, 12288)
	s.Require().True(hit)
	s.Require().Equal(1024, n)
	s.Assert().Equal(stored[:1024], dest)

	// Ring consumed — second Serve must miss.
	dest2 := make([]byte, 1024)
	n, hit = r.Serve(dest2, 12288)
	s.Require().False(hit)
	s.Assert().Equal(0, n)
}

func (s *ReadaheadTestSuite) TestServe_OutOfRangeMisses() {
	r := NewReadahead(4096, 3)
	stored := make([]byte, 4096)
	r.Store(12288, stored)

	// Offset below the ring.
	dest := make([]byte, 1024)
	n, hit := r.Serve(dest, 8192)
	s.Require().False(hit)
	s.Assert().Equal(0, n)

	// Offset above the ring.
	n, hit = r.Serve(dest, 20000)
	s.Require().False(hit)
	s.Assert().Equal(0, n)

	// Overflows the ring — let the network handle it.
	big := make([]byte, 8192)
	n, hit = r.Serve(big, 12288)
	s.Require().False(hit)
	s.Assert().Equal(0, n)
}

func (s *ReadaheadTestSuite) TestObserve_NonSequentialDropsRing() {
	r := NewReadahead(4096, 3)
	stored := make([]byte, 4096)
	r.Store(12288, stored)

	// Non-sequential observation should reset state and drop the ring.
	_, _ = r.Observe(99999, 4096)

	dest := make([]byte, 1024)
	n, hit := r.Serve(dest, 12288)
	s.Require().False(hit, "ring must be dropped after a non-sequential observation")
	s.Assert().Equal(0, n)
}

func TestReadaheadSuite(t *testing.T) {
	suite.Run(t, new(ReadaheadTestSuite))
}
