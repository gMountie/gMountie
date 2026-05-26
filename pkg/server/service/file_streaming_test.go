package service

import (
	"context"
	"testing"

	"github.com/hanwen/go-fuse/v2/fuse"
	"github.com/stretchr/testify/suite"
)

type FileStreamingTestSuite struct {
	suite.Suite
}

// TestStream_FrameBufferIsPooledNotAllocatedPerCall asserts that repeated
// Stream calls do not each allocate a fresh frame buffer. Without pooling,
// every call allocates one frameSize buffer (allocs/call == 1); with the pool
// the buffer is recycled and the per-call allocation amortizes below 1.
func (s *FileStreamingTestSuite) TestStream_FrameBufferIsPooledNotAllocatedPerCall() {
	const frameSize = 4096
	streamer := NewReadStreamer(frameSize)
	ctx := context.Background()

	// Alloc-free fileRead (reports a full frame, copies nothing) and emit.
	fileRead := func(buf []byte, off int64) (int, fuse.Status) {
		return len(buf), fuse.OK
	}
	emit := func(data []byte, st fuse.Status) error { return nil }

	run := func() {
		_ = streamer.Stream(ctx, frameSize, 0, fileRead, emit)
	}

	// Warm the pool so the one-time New allocation is not counted.
	run()

	allocs := testing.AllocsPerRun(200, run)
	s.Assert().Less(allocs, 1.0,
		"frame buffer must come from the pool, not a per-call make([]byte, frameSize)")
}

func TestFileStreamingSuite(t *testing.T) {
	suite.Run(t, new(FileStreamingTestSuite))
}
