// Package service contains the server-side business logic that controllers
// delegate to. file_streaming.go holds ReadStreamer — the chunked-read driver
// used by the RpcFile.Read server-streaming handler.
package service

import (
	"context"

	"github.com/hanwen/go-fuse/v2/fuse"
	"github.com/pkg/errors"
)

// ReadStreamer drives a chunked, frame-by-frame Read against a backing
// FUSE file. Each Stream call issues frameSize-sized reads until the
// requested totalSize is satisfied, EOF is hit (short read), or the
// filesystem returns a non-OK status. The terminal status is always
// reported via a final emit call so the wire stream carries an
// explicit completion frame.
type ReadStreamer struct {
	frameSize int
}

// NewReadStreamer constructs a ReadStreamer that emits frames of at most
// frameSize bytes. The caller is responsible for validating frameSize
// against gRPC's max recv size; ServerConfig.FrameSizeBytes enforces
// `min=4096,max=16777216` upstream.
func NewReadStreamer(frameSize int) *ReadStreamer {
	return &ReadStreamer{frameSize: frameSize}
}

// Stream issues frame-sized reads against fileRead and invokes emit for
// each frame. On a non-OK FUSE status, Stream emits a single terminal
// frame carrying that status and returns whatever emit returns. On EOF
// (a short read or zero-byte read), Stream emits a final OK frame with
// no data to mark the close. Transport errors from emit are returned
// directly; a cancelled context short-circuits before the next read.
func (s *ReadStreamer) Stream(
	ctx context.Context,
	totalSize int,
	startOffset int64,
	fileRead func(buf []byte, off int64) (n int, status fuse.Status),
	emit func(data []byte, status fuse.Status) error,
) error {
	remaining := totalSize
	off := startOffset
	for remaining > 0 {
		if err := ctx.Err(); err != nil {
			return errors.Wrap(err, "read stream cancelled")
		}
		chunk := remaining
		if chunk > s.frameSize {
			chunk = s.frameSize
		}
		buf := make([]byte, chunk)
		n, st := fileRead(buf, off)
		if !st.Ok() {
			return emit(nil, st)
		}
		if n == 0 {
			// EOF before any bytes for this chunk: close with terminal OK.
			return emit(nil, fuse.OK)
		}
		if err := emit(buf[:n], fuse.OK); err != nil {
			return err
		}
		off += int64(n)
		remaining -= n
		if n < chunk {
			// Short read => EOF; signal close with a terminal OK frame.
			return emit(nil, fuse.OK)
		}
	}
	// Requested size satisfied exactly; emit terminal OK to mark close.
	return emit(nil, fuse.OK)
}
