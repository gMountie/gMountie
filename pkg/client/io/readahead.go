package io

import "sync"

// raChunk is a single prefetch slot: the file offset at which the fetch
// was issued and the bytes returned. data is nil while the fetch is in
// flight; non-nil means the chunk is ready to serve. consumed tracks how
// many bytes from the front of data have already been served, allowing
// partial reads to retain the tail for the next Serve call.
type raChunk struct {
	off      int64
	data     []byte
	consumed int // bytes already served from the front of data (partial consume)
}

// Readahead is per-fd state for client-side sequential-read prefetch.
// It maintains a sliding window of up to window chunks ahead of the
// current read cursor. When Observe detects a run of threshold
// strictly-sequential reads it arms up to window concurrent prefetches
// and returns their offsets to the caller. As consumed chunks are
// removed via Serve, Observe slides the window forward and returns new
// offsets to keep the pipeline full.
//
// Non-sequential observations (backwards seek or gap) evict all chunks
// that now lie behind the new cursor; if every chunk is evicted the
// window is reset and the heuristic re-arms from scratch.
//
// All fields are guarded by mu. Lock granularity is per-fd; contention
// is negligible because the synchronous Read path and the prefetch
// goroutines for the same file are the only callers, and the lock is
// never held across a network call.
type Readahead struct {
	mu        sync.Mutex
	chunkSize int       // size of a single prefetch fetch, in bytes
	threshold int       // sequential reads needed before arming a prefetch
	window    int       // maximum number of chunks in flight/ready ahead
	seqHits   int       // run length of strictly-sequential observations
	lastOff   int64     // offset of the last observed Read
	lastSize  int       // size of the last observed Read
	chunks    []raChunk // ascending by off; len <= window
}

// NewReadahead returns a Readahead with chunkSize-sized prefetch fetches,
// arming after threshold strictly-sequential reads, keeping up to window
// chunks in flight or ready ahead of the read cursor. window < 1 is
// clamped to 1.
func NewReadahead(chunkSize, threshold, window int) *Readahead {
	if chunkSize <= 0 {
		panic("NewReadahead: chunkSize must be > 0")
	}
	if window < 1 {
		window = 1
	}
	return &Readahead{
		chunkSize: chunkSize,
		threshold: threshold,
		window:    window,
	}
}

// Observe is called by the synchronous Read path after a successful Read
// of n bytes at offset off. It updates the sequential-run counter and
// returns the offsets of any new prefetch fetches the caller should kick
// off to keep the window full.
//
// Each returned offset is immediately recorded in-flight so a subsequent
// Observe before its Store does not re-arm the same slot.
//
// A non-sequential observation resets the sequential-run counter and
// evicts any chunks that no longer lie ahead of the new cursor (off+n).
// Because a backward seek or gap places the new cursor behind previously
// queued chunks they all fall outside the window and are dropped.
func (r *Readahead) Observe(off int64, n int) []int64 {
	r.mu.Lock()
	defer r.mu.Unlock()

	sequential := off == r.lastOff+int64(r.lastSize)
	if sequential {
		r.seqHits++
	} else {
		r.seqHits = 1
	}
	r.lastOff = off
	r.lastSize = n

	// Evict chunks that are entirely behind the new cursor regardless of
	// whether we have reached the threshold — stale data must not survive
	// a seek.
	next := off + int64(n)
	kept := r.chunks[:0]
	for _, c := range r.chunks {
		endOff := c.off + int64(r.chunkSize)
		if c.data != nil {
			endOff = c.off + int64(len(c.data))
		}
		if endOff > next {
			kept = append(kept, c)
		}
	}
	r.chunks = kept

	if r.seqHits < r.threshold {
		return nil
	}

	// Arm new slots until the window is full.
	armFrom := next
	if len(r.chunks) > 0 {
		armFrom = r.chunks[len(r.chunks)-1].off + int64(r.chunkSize)
	}
	var arm []int64
	for len(r.chunks) < r.window {
		r.chunks = append(r.chunks, raChunk{off: armFrom})
		arm = append(arm, armFrom)
		armFrom += int64(r.chunkSize)
	}
	return arm
}

// Store is called by a prefetch goroutine after a successful fetch. It
// marks the in-flight slot at off as ready with the supplied bytes. If
// an intervening non-sequential Observe has already evicted the slot,
// Store is a no-op.
func (r *Readahead) Store(off int64, data []byte) {
	r.mu.Lock()
	defer r.mu.Unlock()
	for i := range r.chunks {
		if r.chunks[i].off == off {
			r.chunks[i].data = data
			return
		}
	}
}

// Serve attempts to satisfy a Read(dest, off) entirely from ready chunks in
// the window. It is full-or-miss: if the requested range is not fully covered
// by contiguous ready chunks it returns (0, false) without modifying any state
// (side-effect-free on miss). On a hit it copies the bytes, advances the
// consumed cursor on each touched chunk, and drops any chunk that has been
// fully consumed. Partially consumed chunks are retained for subsequent calls.
// Cross-chunk reads are satisfied by spanning contiguous ready chunks.
func (r *Readahead) Serve(dest []byte, off int64) (int, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()

	need := int64(len(dest))
	if need == 0 {
		return 0, true
	}
	end := off + need

	start := -1
	for i := range r.chunks {
		c := r.chunks[i]
		if c.data == nil {
			continue
		}
		if c.off+int64(c.consumed) <= off && off < c.off+int64(len(c.data)) {
			start = i
			break
		}
	}
	if start == -1 {
		return 0, false
	}

	covEnd := r.chunks[start].off + int64(len(r.chunks[start].data))
	last := start
	for covEnd < end {
		next := last + 1
		if next >= len(r.chunks) {
			return 0, false
		}
		c := r.chunks[next]
		if c.data == nil || c.off != covEnd {
			return 0, false
		}
		covEnd = c.off + int64(len(c.data))
		last = next
	}

	written := int64(0)
	for i := start; i <= last; i++ {
		c := &r.chunks[i]
		srcStart := int64(0)
		if i == start {
			srcStart = off - c.off
		}
		take := int64(len(c.data)) - srcStart
		if take > need-written {
			take = need - written
		}
		copy(dest[written:written+take], c.data[srcStart:srcStart+take])
		written += take
		if int(srcStart+take) > c.consumed {
			c.consumed = int(srcStart + take)
		}
	}

	kept := r.chunks[:0]
	for _, c := range r.chunks {
		if c.data != nil && c.consumed >= len(c.data) {
			continue
		}
		kept = append(kept, c)
	}
	r.chunks = kept
	return int(written), true
}
