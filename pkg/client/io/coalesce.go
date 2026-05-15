package io

import "sync"

// CoalescedWrite is a contiguous batch of bytes ready to be sent in a
// single streaming Write RPC. Offset is the starting offset in the file;
// Data is a copy of the coalescer's internal buffer so the caller is free
// to mutate or hold it after the coalescer's lock is released.
type CoalescedWrite struct {
	Offset int64
	Data   []byte
}

// WriteCoalescer accumulates small contiguous writes per fd. Big writes
// pass through directly at the call site (the coalescer doesn't see them);
// small contiguous writes accumulate until either:
//   - the buffer reaches threshold,
//   - a non-contiguous Append arrives, or
//   - the file is Flushed / Released / Fsynced.
//
// The mutex protects every field. We hold it only across in-memory buffer
// manipulation — never across network I/O — so concurrent Flush from the
// FUSE close path can race with an in-flight Write without serialising on
// the wire.
type WriteCoalescer struct {
	mu        sync.Mutex
	threshold int
	buf       []byte
	startOff  int64
	hasData   bool
}

// NewWriteCoalescer returns a WriteCoalescer that drains at threshold
// bytes. Callers should pass a positive threshold; threshold == 0 is the
// caller's signal that coalescing is disabled and they should avoid
// creating a coalescer at all.
func NewWriteCoalescer(threshold int) *WriteCoalescer {
	return &WriteCoalescer{threshold: threshold}
}

// Append buffers data at offset off. If the append fills or overflows the
// threshold, or if off is non-contiguous with the pending buffer, Append
// returns the buffer the caller must flush to the network (as a copy —
// safe to send under no lock). Otherwise it returns nil.
//
// Discontiguous semantics: the pre-drain buffer is returned AND the new
// data becomes the head of a fresh buffer at off. The caller will Drain
// the new buffer on Flush/Release or when it next overflows.
//
// Big-write semantics: a single Append with len(data) >= threshold is
// returned as-is, full batch, no truncation. Callers should typically
// short-circuit this path and call streamingWrite directly without going
// through the coalescer — see GrpcFile.Write.
func (c *WriteCoalescer) Append(data []byte, off int64) *CoalescedWrite {
	c.mu.Lock()
	defer c.mu.Unlock()

	// Non-contiguous: drain the existing buffer, then seed a fresh buffer
	// at the new offset with the new data. The returned batch is the
	// PRE-drain content; the post-drain buffer is left holding the new
	// write, which the caller will pick up on the next overflow or Drain.
	if c.hasData && off != c.startOff+int64(len(c.buf)) {
		flushed := c.snapshotLocked()
		c.resetLocked()
		c.startOff = off
		c.buf = append(c.buf, data...)
		c.hasData = true
		// If the new seed already overflows threshold, drain it too — but
		// we can only return one batch. The caller will send `flushed`,
		// then immediately call Append again or hit the overflow on the
		// next small write. For simplicity, we let the seed sit; the next
		// Append (or Drain) handles overflow.
		return flushed
	}

	// Contiguous (or empty): append.
	if !c.hasData {
		c.startOff = off
	}
	c.buf = append(c.buf, data...)
	c.hasData = true

	if len(c.buf) >= c.threshold {
		flushed := c.snapshotLocked()
		c.resetLocked()
		return flushed
	}
	return nil
}

// Drain returns the pending buffer (as a copy) and clears the coalescer.
// Returns nil if the buffer is empty. Callers use this on Flush, Release,
// and Fsync to push any pending bytes before the file-state RPC.
func (c *WriteCoalescer) Drain() *CoalescedWrite {
	c.mu.Lock()
	defer c.mu.Unlock()
	if !c.hasData {
		return nil
	}
	out := c.snapshotLocked()
	c.resetLocked()
	return out
}

// snapshotLocked returns a CoalescedWrite with a fresh copy of the
// internal buffer. Must be called with c.mu held.
func (c *WriteCoalescer) snapshotLocked() *CoalescedWrite {
	cp := make([]byte, len(c.buf))
	copy(cp, c.buf)
	return &CoalescedWrite{Offset: c.startOff, Data: cp}
}

// resetLocked clears the pending state but keeps the buf slice's
// underlying capacity for the next batch. Must be called with c.mu held.
func (c *WriteCoalescer) resetLocked() {
	c.buf = c.buf[:0]
	c.startOff = 0
	c.hasData = false
}
