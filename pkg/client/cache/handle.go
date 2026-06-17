package cache

import (
	"sync"

	"go.gmountie.dev/gmountie/pkg/client/io"
)

// cachedHandle wraps an inner io.FileHandle returned by the inner
// FileSystemBackend.Open or Create. The cache decorator returns
// these to the go-fuse adapter layer; per-handle ops (Read, Write,
// Release, Flush, Fsync, Allocate, locks) pass through to the inner
// backend with the wrapper unwrapping itself first.
//
// Path() is the path the handle was opened against — needed by the
// cache layer to key data-chunk invalidations off the right path
// when an inner backend's path-from-handle accessor changes shape.
type cachedHandle struct {
	inner io.FileHandle
	path  string

	// Sequential-read detection for prefetch over-read (cachedBackend.Read).
	// A caching FUSE fs rarely sees the kernel fan reads out concurrently, so a
	// small mutex over the heuristic counters is cheap and keeps -race clean.
	seqMu   sync.Mutex
	seqLast int // last chunk index read (-1 = none yet)
	seqRun  int // consecutive in-order reads
}

// newCachedHandle wraps an inner handle.
func newCachedHandle(inner io.FileHandle, path string) *cachedHandle {
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

// Path returns the path the wrapper was constructed with. This is the
// authoritative path for cache invalidation purposes — it's the path
// the caller passed to Open/Create, not whatever inner.Path() returns.
func (h *cachedHandle) Path() string { return h.path }

// Unwrap returns the inner handle. io.BackendClient's resolveHandle
// walks Unwrap chains to find the leaf *grpcFileHandle.
func (h *cachedHandle) Unwrap() io.FileHandle { return h.inner }
