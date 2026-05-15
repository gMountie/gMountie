package service

import "github.com/hanwen/go-fuse/v2/fuse"

// FuseFileWriter is the narrow interface WriteFrameSink needs from a backing
// FUSE file. Keeps the sink unit-testable without a real go-fuse file.
type FuseFileWriter interface {
	Write(data []byte, off int64) (uint32, fuse.Status)
}

// WriteFrameSink writes incoming frames sequentially to an open file, tracking
// the running offset and total bytes written. Non-OK status from the backing
// file is surfaced via WriteFrame's return; the caller decides whether to abort.
type WriteFrameSink struct {
	file   FuseFileWriter
	offset int64
	total  uint32
}

// NewWriteFrameSink builds a sink that begins writing at startOffset.
func NewWriteFrameSink(f FuseFileWriter, startOffset int64) *WriteFrameSink {
	return &WriteFrameSink{file: f, offset: startOffset}
}

// WriteFrame writes data at the running offset, advancing it on success. An
// empty data slice is treated as a no-op (returns OK). On non-OK status from
// the backing file, the partial byte count from that call is returned and the
// running totals are left untouched so callers can surface the errno cleanly.
func (w *WriteFrameSink) WriteFrame(data []byte) (uint32, fuse.Status) {
	if len(data) == 0 {
		return 0, fuse.OK
	}
	n, st := w.file.Write(data, w.offset)
	if !st.Ok() {
		return n, st
	}
	w.offset += int64(n)
	w.total += n
	return n, fuse.OK
}

// Total returns the cumulative bytes successfully written across all frames.
func (w *WriteFrameSink) Total() uint32 { return w.total }
