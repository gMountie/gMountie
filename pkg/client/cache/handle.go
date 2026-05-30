package cache

import "go.gmountie.dev/gmountie/pkg/client/io"

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
}

// newCachedHandle wraps an inner handle.
func newCachedHandle(inner io.FileHandle, path string) *cachedHandle {
	return &cachedHandle{inner: inner, path: path}
}

// Path returns the path the wrapper was constructed with. This is the
// authoritative path for cache invalidation purposes — it's the path
// the caller passed to Open/Create, not whatever inner.Path() returns.
func (h *cachedHandle) Path() string { return h.path }

// Unwrap returns the inner handle. io.BackendClient's resolveHandle
// walks Unwrap chains to find the leaf *grpcFileHandle.
func (h *cachedHandle) Unwrap() io.FileHandle { return h.inner }
