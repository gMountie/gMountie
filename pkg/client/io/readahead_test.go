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

func (s *ReadaheadTestSuite) TestServe_PartialConsumeRetainsChunk() {
	r := NewReadahead(4096, 3, 1)
	stored := make([]byte, 4096)
	for i := range stored {
		stored[i] = byte(i % 251)
	}
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

	dest2 := make([]byte, 1024)
	n, hit = r.Serve(dest2, 12288+1024)
	s.Require().True(hit, "retained tail must still serve")
	s.Require().Equal(1024, n)
	s.Assert().Equal(stored[1024:2048], dest2)

	rest := make([]byte, 2048)
	_, hit = r.Serve(rest, 12288+2048)
	s.Require().True(hit)
	_, hit = r.Serve(make([]byte, 1), 12288)
	s.Assert().False(hit, "fully drained chunk must be gone")
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
	// window=1: only one chunk ever armed, so a 150-byte read across a 100-byte
	// chunk boundary has no second chunk available and must miss.
	r := NewReadahead(100, 1, 1)
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

func (s *ReadaheadTestSuite) TestObserve_ReadLargerThanChunkArmsWindow() {
	r := NewReadahead(4096, 1, 2)
	arm := r.Observe(0, 8192)
	s.Require().Len(arm, 2, "large read must arm a deep window now")
	s.Assert().Equal([]int64{8192, 12288}, arm, "arm contiguous chunks ahead of cursor")
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

func (s *ReadaheadTestSuite) TestObserve_DeepWindowArmsForLargeReads() {
	r := NewReadahead(1<<20, 1, 4)
	arm := r.Observe(0, 1<<20)
	s.Require().Len(arm, 4, "deep window armed ahead of the cursor")
	s.Assert().Equal(int64(1<<20), arm[0])
	s.Assert().Equal(int64(4<<20), arm[3])
}

func (s *ReadaheadTestSuite) TestServe_CrossChunkHitSpansContiguousChunks() {
	r := NewReadahead(4096, 1, 4)
	arm := r.Observe(0, 4096)
	s.Require().Contains(arm, int64(4096))
	s.Require().Contains(arm, int64(8192))
	c0 := make([]byte, 4096)
	c1 := make([]byte, 4096)
	for i := range c0 {
		c0[i] = byte(i % 251)
		c1[i] = byte((i + 7) % 251)
	}
	r.Store(4096, c0)
	r.Store(8192, c1)

	dest := make([]byte, 4096)
	n, hit := r.Serve(dest, 6144)
	s.Require().True(hit, "read spanning two contiguous ready chunks must hit")
	s.Require().Equal(4096, n)
	s.Assert().Equal(c0[2048:], dest[:2048])
	s.Assert().Equal(c1[:2048], dest[2048:])
}

func (s *ReadaheadTestSuite) TestServe_MissWhenNextChunkNotReady() {
	r := NewReadahead(4096, 1, 4)
	r.Observe(0, 4096)
	r.Store(4096, make([]byte, 4096))
	dest := make([]byte, 4096+10)
	n, hit := r.Serve(dest, 4096)
	s.Assert().False(hit)
	s.Assert().Equal(0, n)
	d2 := make([]byte, 4096)
	_, hit = r.Serve(d2, 4096)
	s.Assert().True(hit, "a miss must leave the ready chunk intact")
}

func TestReadaheadSuite(t *testing.T) {
	suite.Run(t, new(ReadaheadTestSuite))
}
