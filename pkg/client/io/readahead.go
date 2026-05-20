package io

import "sync"

// Readahead is per-fd state for client-side sequential-read prefetch. The
// design is intentionally minimal: detect a run of N strictly-sequential
// reads, then prefetch a single chunk one chunk ahead of the current
// offset. There is at most one outstanding prefetch at a time; if the
// caller drifts (backwards seek, gap, or just stops reading sequentially)
// the ring is discarded and the heuristic re-arms from scratch.
//
// All fields are guarded by mu. Lock granularity is per-fd so contention
// is negligible — the synchronous Read path and the prefetch goroutine
// for the same file are the only callers, and they serialise naturally
// via the kernel's FUSE request stream and our single in-flight rule.
type Readahead struct {
	mu        sync.Mutex
	chunkSize int   // size of a single prefetch fetch, in bytes
	threshold int   // sequential reads needed before arming a prefetch
	seqHits   int   // run length of strictly-sequential observations
	lastOff   int64 // offset of the last observed Read
	lastSize  int   // size of the last observed Read
	// prefetched holds the bytes returned by the most recent prefetch.
	// Non-nil means "prefetch in flight or sitting in the ring waiting to
	// be consumed". Observe will not arm a new prefetch while this is
	// non-nil.
	prefetched    []byte
	prefetchedOff int64
}

// NewReadahead returns a Readahead with chunkSize-sized prefetch fetches,
// arming after threshold strictly-sequential reads.
func NewReadahead(chunkSize, threshold int) *Readahead {
	return &Readahead{
		chunkSize: chunkSize,
		threshold: threshold,
	}
}

// Observe is called by the synchronous Read path after a successful Read
// of n bytes at offset off. It updates the sequential-run counter and
// reports whether the caller should kick off a prefetch. The returned
// prefetchOff is the offset to start the prefetch at (off + n); the
// returned bool is true only on the read that crosses the threshold,
// and only while no prefetch buffer is already in flight or pending in
// the ring.
//
// A non-sequential observation (backwards seek or gap) resets the
// sequential-run counter and discards any pending prefetched bytes.
func (r *Readahead) Observe(off int64, n int) (int64, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()

	// Strictly-sequential continuation of the previous read. With pristine
	// state (lastOff=0, lastSize=0), the first read at offset 0 is treated
	// as sequential; the first read at any other offset is treated as the
	// start of a fresh run.
	sequential := off == r.lastOff+int64(r.lastSize)

	if sequential {
		r.seqHits++
	} else {
		r.seqHits = 1
		r.prefetched = nil
		r.prefetchedOff = 0
	}
	r.lastOff = off
	r.lastSize = n

	if r.seqHits >= r.threshold && r.prefetched == nil {
		return off + int64(n), true
	}
	return 0, false
}

// Store is called by the prefetch goroutine after a successful prefetch
// fetch. It parks the bytes in the ring at the given offset. If an
// intervening non-sequential Observe has already dropped the ring,
// callers will detect that on the next Serve and re-fetch.
func (r *Readahead) Store(off int64, data []byte) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.prefetched = data
	r.prefetchedOff = off
}

// Serve attempts to satisfy a Read(dest, off) entirely from the ring.
// It returns (len(dest), true) on a hit and clears the ring (one-shot
// consume). On any miss — no ring, offset below or above the ring, or
// dest larger than the ring — it returns (0, false) and leaves the ring
// untouched.
//
// The "dest larger than the ring" branch is deliberate: we don't try to
// stitch the prefix from the ring with a fresh fetch for the rest. The
// streaming-Read path is already efficient for big reads, and the
// stitched-result code path would be a footgun. Let the network handle
// it and rely on the next sequential read to re-arm.
func (r *Readahead) Serve(dest []byte, off int64) (int, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.prefetched == nil {
		return 0, false
	}
	end := r.prefetchedOff + int64(len(r.prefetched))
	if off < r.prefetchedOff || off >= end {
		return 0, false
	}
	avail := end - off
	if int64(len(dest)) > avail {
		return 0, false
	}
	start := off - r.prefetchedOff
	copy(dest, r.prefetched[start:start+int64(len(dest))])
	// One-shot consume: clear the ring so the next sequential read
	// triggers a fresh prefetch.
	r.prefetched = nil
	r.prefetchedOff = 0
	return len(dest), true
}
