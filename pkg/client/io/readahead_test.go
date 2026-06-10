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
	n, full := r.Serve(dest, 8192)
	s.Require().False(full)
	s.Assert().Equal(0, n)

	// Offset above the ring.
	n, full = r.Serve(dest, 20000)
	s.Require().False(full)
	s.Assert().Equal(0, n)

	// Overflows the ring: the covered prefix (the whole 4096-byte chunk)
	// is served; the caller fetches the remaining 4096 live.
	big := make([]byte, 8192)
	n, full = r.Serve(big, 12288)
	s.Require().False(full)
	s.Assert().Equal(4096, n)
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

func (s *ReadaheadTestSuite) TestServePartialWhenDestLargerThanChunk() {
	// window=1: only one chunk ever armed, so a 150-byte read across a 100-byte
	// chunk boundary serves the 100-byte covered prefix and leaves the tail to
	// the caller's live fetch.
	r := NewReadahead(100, 1, 1)
	for _, off := range r.Observe(0, 100) {
		r.Store(off, make([]byte, 100))
	}
	n, full := r.Serve(make([]byte, 150), 100)
	s.Require().False(full)
	s.Assert().Equal(100, n)
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

func (s *ReadaheadTestSuite) TestServe_PrefixStopsAtInFlightChunk() {
	// The chunk at 4096 is ready; the chunk at 8192 is armed but still in
	// flight. A read spanning both gets the covered 4096-byte prefix without
	// blocking on the in-flight fetch; the ready chunk is fully consumed.
	r := NewReadahead(4096, 1, 4)
	stored := make([]byte, 4096)
	for i := range stored {
		stored[i] = byte(i % 251)
	}
	r.Observe(0, 4096)
	r.Store(4096, stored)

	dest := make([]byte, 4096+10)
	n, full := r.Serve(dest, 4096)
	s.Require().False(full)
	s.Require().Equal(4096, n, "prefix must stop at the in-flight chunk boundary")
	s.Assert().Equal(stored, dest[:4096])

	// The served chunk is drained; the in-flight slot is untouched and can
	// still complete and serve later.
	_, full = r.Serve(make([]byte, 1), 4096)
	s.Require().False(full, "drained chunk must be gone")
	r.Store(8192, make([]byte, 4096))
	n, full = r.Serve(make([]byte, 4096), 8192)
	s.Require().True(full, "in-flight chunk must survive a partial serve and serve once stored")
	s.Assert().Equal(4096, n)
}

func (s *ReadaheadTestSuite) TestServe_PartialPrefixThreeOfFourChunks() {
	// Three of four armed chunks are ready: a read of the full window gets a
	// 75% prefix, and only the consumed (ready) chunks advance — the in-flight
	// slot is untouched.
	r := NewReadahead(1024, 1, 4)
	arm := r.Observe(0, 1024)
	s.Require().Equal([]int64{1024, 2048, 3072, 4096}, arm)
	want := make([]byte, 3072)
	for i := range want {
		want[i] = byte(i % 249)
	}
	r.Store(1024, want[0:1024])
	r.Store(2048, want[1024:2048])
	r.Store(3072, want[2048:3072])
	// Chunk at 4096 remains in flight.

	dest := make([]byte, 4096)
	n, full := r.Serve(dest, 1024)
	s.Require().False(full)
	s.Require().Equal(3072, n, "prefix must cover exactly the three ready chunks")
	s.Assert().Equal(want, dest[:3072])

	// The three consumed chunks are drained; only the in-flight slot remains.
	s.Require().Len(r.chunks, 1)
	s.Assert().Equal(int64(4096), r.chunks[0].off)
	s.Assert().Nil(r.chunks[0].data)
}

func (s *ReadaheadTestSuite) TestServe_ZeroCoverageNoSideEffects() {
	r := NewReadahead(1024, 1, 4)
	r.Observe(0, 1024)
	r.Store(1024, make([]byte, 1024))
	// Half-consume the ready chunk so the consumed cursor is non-trivial.
	_, full := r.Serve(make([]byte, 512), 1024)
	s.Require().True(full)
	before := append([]raChunk(nil), r.chunks...)

	// No ready byte at offset 0 (behind the window) nor at 2048 (in flight).
	n, full := r.Serve(make([]byte, 512), 0)
	s.Require().False(full)
	s.Require().Equal(0, n)
	n, full = r.Serve(make([]byte, 512), 2048)
	s.Require().False(full)
	s.Require().Equal(0, n)

	s.Assert().Equal(before, r.chunks, "a zero-coverage miss must leave cursors and chunks untouched")
}

func (s *ReadaheadTestSuite) TestServe_PartialServeHitsRetainedChunkTail() {
	// A full hit half-drains the chunk at 4096 (retained). The next
	// sequential read overshoots the ready data: it gets the retained tail
	// as a partial prefix, fully draining the chunk.
	r := NewReadahead(4096, 1, 4)
	stored := make([]byte, 4096)
	for i := range stored {
		stored[i] = byte(i % 247)
	}
	r.Observe(0, 4096)
	r.Store(4096, stored)

	n, full := r.Serve(make([]byte, 2048), 4096)
	s.Require().True(full)
	s.Require().Equal(2048, n)

	dest := make([]byte, 4096)
	n, full = r.Serve(dest, 6144)
	s.Require().False(full)
	s.Require().Equal(2048, n, "partial serve must drain exactly the retained tail")
	s.Assert().Equal(stored[2048:], dest[:2048])

	_, full = r.Serve(make([]byte, 1), 4096)
	s.Assert().False(full, "fully drained chunk must be gone")
}

func TestReadaheadSuite(t *testing.T) {
	suite.Run(t, new(ReadaheadTestSuite))
}
