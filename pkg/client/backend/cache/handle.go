package cache

import (
	"sync"

	"go.gmountie.dev/gmountie/pkg/client/backend"
)

// cachedHandle wraps an inner backend.FileHandle returned by the inner
// FileSystemBackend.Open or Create. The cache decorator returns
// these to the go-fuse adapter layer; per-handle ops (Read, Write,
// Release, Flush, Fsync, Allocate, locks) pass through to the inner
// backend with the wrapper unwrapping itself first.
//
// Path() is the path the handle was opened against — needed by the
// cache layer to key data-chunk invalidations off the right path
// when an inner backend's path-from-handle accessor changes shape.
type cachedHandle struct {
	inner backend.FileHandle
	path  string

	// Sequential-read detection for prefetch over-read (cachedBackend.Read).
	// A caching FUSE fs rarely sees the kernel fan reads out concurrently, so a
	// small mutex over the heuristic counters is cheap and keeps -race clean.
	// seqMu also guards the in-flight over-read range below.
	seqMu   sync.Mutex
	seqLast int // last chunk index read (-1 = none yet)
	seqRun  int // consecutive in-order reads

	// In-flight over-read dedup: when one read is over-reading a span of chunks,
	// concurrent reads whose chunk falls in that span WAIT on inflightDone and
	// serve from cache once it lands, instead of launching a duplicate (and
	// overlapping) span fetch. [inflightStart,inflightEnd) is the span; nil
	// inflightDone means no over-read is in flight.
	inflightStart int
	inflightEnd   int
	inflightDone  chan struct{}
}

// newCachedHandle wraps an inner handle.
func newCachedHandle(inner backend.FileHandle, path string) *cachedHandle {
	return &cachedHandle{inner: inner, path: path, seqLast: -1}
}

// recordRead notes a read at chunkIndex and reports whether access has been
// sequential for at least `threshold` reads — the signal to prefetch a span on
// a miss. Gating on this keeps random reads from amplifying bandwidth W×.
func (h *cachedHandle) recordRead(chunkIndex, threshold int) (prefetch bool) {
	h.seqMu.Lock()
	defer h.seqMu.Unlock()
	switch chunkIndex {
	case h.seqLast + 1:
		h.seqRun++
	case h.seqLast:
		// re-read of the same chunk; leave the streak unchanged
	default:
		h.seqRun = 1
	}
	h.seqLast = chunkIndex
	return h.seqRun >= threshold
}

// waitInflight reports whether chunkIndex is covered by an in-flight over-read;
// if so it returns that fetch's done-channel to wait on. The caller waits, then
// re-checks the cache (the span will have landed) instead of duplicating the
// fetch.
func (h *cachedHandle) waitInflight(chunkIndex int) (<-chan struct{}, bool) {
	h.seqMu.Lock()
	defer h.seqMu.Unlock()
	if h.inflightDone != nil && chunkIndex >= h.inflightStart && chunkIndex < h.inflightEnd {
		return h.inflightDone, true
	}
	return nil, false
}

// beginSpanFetch registers [start,start+span) as the in-flight over-read so
// concurrent reads covered by it wait (waitInflight) rather than duplicating the
// fetch. It returns a finish func the caller MUST call once the span's chunks
// are cached (or the fetch failed) to wake the waiters and clear the range.
func (h *cachedHandle) beginSpanFetch(start, span int) (finish func()) {
	ch := make(chan struct{})
	h.seqMu.Lock()
	h.inflightStart, h.inflightEnd, h.inflightDone = start, start+span, ch
	h.seqMu.Unlock()
	var once sync.Once
	return func() {
		once.Do(func() {
			h.seqMu.Lock()
			h.inflightStart, h.inflightEnd, h.inflightDone = 0, 0, nil
			h.seqMu.Unlock()
			close(ch)
		})
	}
}

// Path returns the path the wrapper was constructed with. This is the
// authoritative path for cache invalidation purposes — it's the path
// the caller passed to Open/Create, not whatever inner.Path() returns.
func (h *cachedHandle) Path() string { return h.path }

// Unwrap returns the inner handle. backend.BackendClient's resolveHandle
// walks Unwrap chains to find the leaf *grpcFileHandle.
func (h *cachedHandle) Unwrap() backend.FileHandle { return h.inner }
