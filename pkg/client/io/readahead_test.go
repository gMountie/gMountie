package io

import (
	"testing"

	"github.com/stretchr/testify/suite"
)

type ReadaheadTestSuite struct {
	suite.Suite
}

func (s *ReadaheadTestSuite) TestObserve_SequentialOffsetsTriggerPrefetch() {
	r := NewReadahead(4096, 3, 1)

	// First two sequential reads do not arm yet.
	arm := r.Observe(0, 4096)
	s.Require().Empty(arm)
	arm = r.Observe(4096, 4096)
	s.Require().Empty(arm)

	// Third sequential read meets the threshold.
	arm = r.Observe(8192, 4096)
	s.Require().Len(arm, 1, "third sequential observation should arm a prefetch")
	s.Assert().Equal(int64(12288), arm[0])
}

func (s *ReadaheadTestSuite) TestObserve_BackwardSeekResetsState() {
	r := NewReadahead(4096, 3, 1)

	r.Observe(0, 4096)
	r.Observe(4096, 4096)
	r.Observe(8192, 4096)

	// Backward seek to 0 — must reset seqHits.
	arm := r.Observe(0, 4096)
	s.Require().Empty(arm, "backward seek must not immediately re-arm prefetch")
}

func (s *ReadaheadTestSuite) TestServe_FullRangeHitReturnsBytesAndConsumesRing() {
	r := NewReadahead(4096, 3, 1)
	stored := make([]byte, 4096)
	for i := range stored {
		stored[i] = byte(i % 251)
	}
	// Arm the slot at 12288 by reaching the threshold with three sequential reads.
	r.Observe(0, 4096)
	r.Observe(4096, 4096)
	arm := r.Observe(8192, 4096)
	s.Require().Equal([]int64{12288}, arm)
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
	r := NewReadahead(4096, 3, 1)
	stored := make([]byte, 4096)
	// Arm the slot at 12288 via three sequential reads.
	r.Observe(0, 4096)
	r.Observe(4096, 4096)
	r.Observe(8192, 4096)
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
	r := NewReadahead(4096, 3, 1)
	stored := make([]byte, 4096)
	// Arm the slot at 12288 via three sequential reads, then store data.
	r.Observe(0, 4096)
	r.Observe(4096, 4096)
	r.Observe(8192, 4096)
	r.Store(12288, stored)

	// Non-sequential observation should evict the chunk behind the new cursor.
	r.Observe(99999, 4096)

	dest := make([]byte, 1024)
	n, hit := r.Serve(dest, 12288)
	s.Require().False(hit, "ring must be dropped after a non-sequential observation")
	s.Assert().Equal(0, n)
}

// --- window tests ---

func (s *ReadaheadTestSuite) TestWindowFillsAheadAndSlides() {
	r := NewReadahead(100, 1, 4)
	arm := r.Observe(0, 100)
	s.Require().Equal([]int64{100, 200, 300, 400}, arm)
	for _, off := range arm {
		r.Store(off, make([]byte, 100))
	}
	n, hit := r.Serve(make([]byte, 100), 100)
	s.Require().True(hit)
	s.Assert().Equal(100, n)
	arm2 := r.Observe(100, 100)
	s.Assert().Equal([]int64{500}, arm2)
}

func (s *ReadaheadTestSuite) TestWindowOneEqualsLegacy() {
	r := NewReadahead(100, 1, 1)
	s.Require().Equal([]int64{100}, r.Observe(0, 100))
	r.Store(100, make([]byte, 100))
	s.Assert().Empty(r.Observe(0, 100))
}

func (s *ReadaheadTestSuite) TestNonSequentialDropsWindow() {
	r := NewReadahead(100, 1, 4)
	for _, off := range r.Observe(0, 100) {
		r.Store(off, make([]byte, 100))
	}
	s.Assert().Equal([]int64{1100, 1200, 1300, 1400}, r.Observe(1000, 100))
	_, hit := r.Serve(make([]byte, 100), 100)
	s.Assert().False(hit)
}

func (s *ReadaheadTestSuite) TestServeMissWhenDestLargerThanChunk() {
	r := NewReadahead(100, 1, 4)
	for _, off := range r.Observe(0, 100) {
		r.Store(off, make([]byte, 100))
	}
	_, hit := r.Serve(make([]byte, 150), 100)
	s.Assert().False(hit)
}

func (s *ReadaheadTestSuite) TestDoesNotReArmInflight() {
	r := NewReadahead(100, 1, 4)
	r.Observe(0, 100)
	s.Assert().Empty(r.Observe(0, 100))
}

func (s *ReadaheadTestSuite) TestNewReadaheadPanicsOnZeroChunkSize() {
	s.Assert().Panics(func() { NewReadahead(0, 1, 4) })
}

func (s *ReadaheadTestSuite) TestObserve_ReadLargerThanChunkArmsNothing() {
	// chunk=4096, threshold=3, window=1. Each read is 8192 bytes — larger than
	// one chunk — so Serve can never hit (Serve misses when len(dest) > avail,
	// and avail <= chunkSize). Observe must therefore never arm a wasted
	// prefetch, even once the sequential threshold is met.
	r := NewReadahead(4096, 3, 1)

	s.Require().Empty(r.Observe(0, 8192))
	s.Require().Empty(r.Observe(8192, 8192))
	arm := r.Observe(16384, 8192)
	s.Assert().Empty(arm, "a read larger than one chunk must not arm a prefetch")
	s.Assert().Empty(r.chunks, "no chunk should be left armed for an unservable read size")
}

func (s *ReadaheadTestSuite) TestObserve_SingleInFlightNeverExceedsWindowOne() {
	// Guards an existing invariant the fix must not break: at window=1, across a
	// sequential servable run, Observe arms at most one chunk per call and the
	// window never holds more than one chunk. Reads are 4096 (== chunk), so
	// they are servable and the unservable-skip does not apply.
	r := NewReadahead(4096, 1, 1)

	off := int64(0)
	for i := 0; i < 8; i++ {
		arm := r.Observe(off, 4096)
		s.Require().LessOrEqual(len(arm), 1, "window=1 must arm at most one prefetch per Observe")
		s.Require().LessOrEqual(len(r.chunks), 1, "window=1 must never hold more than one chunk")
		off += 4096
	}
}

func TestReadaheadSuite(t *testing.T) {
	suite.Run(t, new(ReadaheadTestSuite))
}
