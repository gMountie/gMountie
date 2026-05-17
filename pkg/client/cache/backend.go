// Package cache: backend.go is the integration glue. cachedBackend decorates
// an io.FileSystemBackend with three sub-caches (attr, dir, data) sharing a
// single byte-budget accountant. Read ops are read-through; mutating ops
// invalidate the appropriate cache slices per the Phase 4 Sub-spec B
// per-op invalidation table.
package cache

import (
	"context"
	"path"
	"strings"

	"gmountie/pkg/client/cache/persist"
	"gmountie/pkg/client/io"
	"gmountie/pkg/client/metrics"

	"github.com/hanwen/go-fuse/v2/fuse"
)

// cachedBackend decorates an inner FileSystemBackend with three
// sub-caches sharing one accountant. Construct via NewCachedBackend;
// implements io.FileSystemBackend.
type cachedBackend struct {
	inner io.FileSystemBackend
	cfg   Config
	acct  *accountant
	attr  *attrCache
	dir   *dirCache
	data  *dataCache
}

// NewCachedBackend wraps inner. cfg.MemoryMaxBytes <= 0 disables byte-cap
// eviction in the memory tier (entries live until invalidated or the process
// dies; the disk tier still respects DiskMaxBytes independently). p may be
// nil for memory-only operation.
func NewCachedBackend(inner io.FileSystemBackend, cfg Config, p *persist.Persist) io.FileSystemBackend {
	acct := newAccountant(cfg.MemoryMaxBytes)
	return &cachedBackend{
		inner: inner,
		cfg:   cfg,
		acct:  acct,
		attr:  newAttrCacheWithPersist(acct, cfg.AttrTTL, cfg.NegativeTTL, nil, p),
		dir:   newDirCacheWithPersist(acct, cfg.DirTTL, nil, p),
		data:  newDataCacheWithPersist(acct, cfg.ChunkSizeBytes, p),
	}
}

// --- Read path ---

func (b *cachedBackend) Stat(ctx context.Context, p string) (*io.Attr, fuse.Status) {
	if a, hit, pos := b.attr.get(p); hit {
		if pos {
			return a, fuse.OK
		}
		return nil, fuse.ENOENT
	}
	metrics.CacheMiss("attr")
	a, st := b.inner.Stat(ctx, p)
	if st == fuse.OK && a != nil {
		b.attr.putPositive(p, a)
	} else if st == fuse.ENOENT {
		b.attr.putNegative(p)
	}
	return a, st
}

func (b *cachedBackend) Lookup(ctx context.Context, parent, name string) (*io.Attr, fuse.Status) {
	full := joinPath(parent, name)
	if a, hit, pos := b.attr.get(full); hit {
		if pos {
			return a, fuse.OK
		}
		return nil, fuse.ENOENT
	}
	metrics.CacheMiss("attr")
	a, st := b.inner.Lookup(ctx, parent, name)
	if st == fuse.OK && a != nil {
		b.attr.putPositive(full, a)
	} else if st == fuse.ENOENT {
		b.attr.putNegative(full)
	}
	return a, st
}

func (b *cachedBackend) ListDir(ctx context.Context, p string) ([]io.DirEntry, fuse.Status) {
	if entries, hit := b.dir.get(p); hit {
		return entries, fuse.OK
	}
	metrics.CacheMiss("dir")
	entries, st := b.inner.ListDir(ctx, p)
	if st == fuse.OK {
		b.dir.put(p, entries)
	}
	return entries, st
}

func (b *cachedBackend) Read(ctx context.Context, fh io.FileHandle, off int64, dest []byte) (int, fuse.Status) {
	ch, ok := fh.(*cachedHandle)
	if !ok {
		return b.inner.Read(ctx, fh, off, dest)
	}
	chunkSize := int64(b.cfg.ChunkSizeBytes)
	total := 0
	for total < len(dest) {
		fileOff := off + int64(total)
		chunkIndex := int(fileOff / chunkSize)
		chunkStart := int64(chunkIndex) * chunkSize
		insideOff := int(fileOff - chunkStart)
		// How many bytes can we satisfy from this chunk?
		want := len(dest) - total
		if want > int(chunkSize)-insideOff {
			want = int(chunkSize) - insideOff
		}
		// Try cache first.
		cached := b.data.get(ch.path, chunkIndex)
		if cached != nil {
			if insideOff >= len(cached) {
				// EOF mid-stream: insideOff is past the cached chunk's end
				// even though dest still has room. The chunk we have is the
				// last one and it's short, so the file ends here.
				return total, fuse.OK
			}
			avail := len(cached) - insideOff
			if avail < want {
				want = avail
			}
			n := copy(dest[total:total+want], cached[insideOff:insideOff+want])
			total += n
			// A short chunk (< chunkSize) is the file's last chunk; nothing
			// more to read from the backend regardless of dest's remaining
			// capacity. A full chunk just means advance to the next chunk.
			if int64(len(cached)) < chunkSize {
				return total, fuse.OK
			}
			continue
		}
		// Miss: fetch this chunk from inner. Read full-chunk-aligned.
		metrics.CacheMiss("data")
		buf := make([]byte, chunkSize)
		n, st := b.inner.Read(ctx, ch.inner, chunkStart, buf)
		if st != fuse.OK {
			return total, st
		}
		if n == 0 {
			return total, fuse.OK
		}
		filled := buf[:n]
		b.data.put(ch.path, chunkIndex, filled)
		if insideOff >= n {
			// EOF before our requested offset.
			return total, fuse.OK
		}
		avail := n - insideOff
		if avail < want {
			want = avail
		}
		copied := copy(dest[total:total+want], filled[insideOff:insideOff+want])
		total += copied
		// Short chunk = last chunk of file; otherwise continue to next
		// chunk. This assumes inner.Read only short-reads at EOF.
		// Today's BackendClient.Read (streaming Recv loop) holds that
		// invariant. If a future inner ever short-reads for a different
		// reason (partial-payload retry assembly, etc.), this branch
		// would truncate the cached chunk and the user-visible Read —
		// in that case, loop the inner fetch until full-or-EOF before
		// caching.
		if int64(n) < chunkSize {
			return total, fuse.OK
		}
	}
	return total, fuse.OK
}

func (b *cachedBackend) Access(ctx context.Context, p string, mode uint32) fuse.Status {
	return b.inner.Access(ctx, p, mode)
}

func (b *cachedBackend) StatFs(ctx context.Context, p string) (*io.StatFs, fuse.Status) {
	return b.inner.StatFs(ctx, p)
}

func (b *cachedBackend) GetXAttr(ctx context.Context, p, attr string) ([]byte, fuse.Status) {
	return b.inner.GetXAttr(ctx, p, attr)
}

func (b *cachedBackend) GetLk(ctx context.Context, fh io.FileHandle, owner uint64, lk *fuse.FileLock, flags uint32, out *fuse.FileLock) fuse.Status {
	return b.inner.GetLk(ctx, unwrapHandle(fh), owner, lk, flags, out)
}

// --- Open / Create / file-handle ops ---

func (b *cachedBackend) Open(ctx context.Context, p string, flags uint32) (io.FileHandle, fuse.Status) {
	h, st := b.inner.Open(ctx, p, flags)
	if st != fuse.OK {
		return nil, st
	}
	return newCachedHandle(h, p), fuse.OK
}

func (b *cachedBackend) Create(ctx context.Context, parent, name string, flags, mode uint32) (io.FileHandle, *io.Attr, fuse.Status) {
	full := joinPath(parent, name)
	h, a, st := b.inner.Create(ctx, parent, name, flags, mode)
	if st != fuse.OK {
		return nil, nil, st
	}
	b.dir.invalidate(parent)
	b.attr.invalidate(parent)
	b.attr.invalidate(full) // drop any negative entry from a prior failed Stat
	if a != nil {
		b.attr.putPositive(full, a)
	}
	return newCachedHandle(h, full), a, fuse.OK
}

func (b *cachedBackend) Write(ctx context.Context, fh io.FileHandle, off int64, data []byte) (uint32, fuse.Status) {
	n, st := b.inner.Write(ctx, unwrapHandle(fh), off, data)
	if st != fuse.OK {
		return n, st
	}
	if ch, ok := fh.(*cachedHandle); ok {
		b.data.invalidateRange(ch.path, off, int64(len(data)))
		b.attr.invalidate(ch.path)
	}
	return n, fuse.OK
}

func (b *cachedBackend) Release(ctx context.Context, fh io.FileHandle) fuse.Status {
	return b.inner.Release(ctx, unwrapHandle(fh))
}

func (b *cachedBackend) Flush(ctx context.Context, fh io.FileHandle) fuse.Status {
	return b.inner.Flush(ctx, unwrapHandle(fh))
}

func (b *cachedBackend) Fsync(ctx context.Context, fh io.FileHandle, flags int64) fuse.Status {
	return b.inner.Fsync(ctx, unwrapHandle(fh), flags)
}

func (b *cachedBackend) Allocate(ctx context.Context, fh io.FileHandle, off, size uint64, mode uint32) fuse.Status {
	st := b.inner.Allocate(ctx, unwrapHandle(fh), off, size, mode)
	if st != fuse.OK {
		return st
	}
	if ch, ok := fh.(*cachedHandle); ok {
		b.data.invalidateRange(ch.path, int64(off), int64(size))
		b.attr.invalidate(ch.path)
	}
	return fuse.OK
}

func (b *cachedBackend) SetLk(ctx context.Context, fh io.FileHandle, owner uint64, lk *fuse.FileLock, flags uint32) fuse.Status {
	return b.inner.SetLk(ctx, unwrapHandle(fh), owner, lk, flags)
}

func (b *cachedBackend) SetLkw(ctx context.Context, fh io.FileHandle, owner uint64, lk *fuse.FileLock, flags uint32) fuse.Status {
	return b.inner.SetLkw(ctx, unwrapHandle(fh), owner, lk, flags)
}

// --- Path-level mutating ops ---

func (b *cachedBackend) Mkdir(ctx context.Context, p string, mode uint32) fuse.Status {
	st := b.inner.Mkdir(ctx, p, mode)
	if st != fuse.OK {
		return st
	}
	parent := pathParent(p)
	b.dir.invalidate(parent)
	b.attr.invalidate(parent)
	b.attr.invalidate(p) // drop any negative-cached entry for the just-created path
	return fuse.OK
}

func (b *cachedBackend) Rmdir(ctx context.Context, p string) fuse.Status {
	st := b.inner.Rmdir(ctx, p)
	if st != fuse.OK {
		return st
	}
	b.attr.invalidate(p)
	b.dir.invalidate(p)
	parent := pathParent(p)
	b.dir.invalidate(parent)
	b.attr.putNegative(p)
	return fuse.OK
}

func (b *cachedBackend) Unlink(ctx context.Context, p string) fuse.Status {
	st := b.inner.Unlink(ctx, p)
	if st != fuse.OK {
		return st
	}
	b.attr.invalidate(p)
	b.data.invalidatePath(p)
	parent := pathParent(p)
	b.dir.invalidate(parent)
	b.attr.putNegative(p)
	return fuse.OK
}

func (b *cachedBackend) Rename(ctx context.Context, oldPath, newPath string) fuse.Status {
	st := b.inner.Rename(ctx, oldPath, newPath)
	if st != fuse.OK {
		return st
	}
	b.attr.invalidate(oldPath)
	b.attr.invalidate(newPath)
	b.data.invalidatePath(oldPath)
	b.data.invalidatePath(newPath)
	b.dir.invalidate(pathParent(oldPath))
	b.dir.invalidate(pathParent(newPath))
	b.attr.putNegative(oldPath)
	return fuse.OK
}

func (b *cachedBackend) Truncate(ctx context.Context, p string, size uint64) fuse.Status {
	st := b.inner.Truncate(ctx, p, size)
	if st != fuse.OK {
		return st
	}
	// Conservative: drop every chunk for p. Truncate may zero-extend or
	// shrink; either way every cached chunk's relationship to the new
	// file length is suspect.
	b.data.invalidatePath(p)
	b.attr.invalidate(p)
	return fuse.OK
}

func (b *cachedBackend) Chmod(ctx context.Context, p string, mode uint32) fuse.Status {
	st := b.inner.Chmod(ctx, p, mode)
	if st != fuse.OK {
		return st
	}
	b.attr.invalidate(p)
	return fuse.OK
}

func (b *cachedBackend) Chown(ctx context.Context, p string, uid, gid uint32) fuse.Status {
	st := b.inner.Chown(ctx, p, uid, gid)
	if st != fuse.OK {
		return st
	}
	b.attr.invalidate(p)
	return fuse.OK
}

// --- helpers ---

// unwrapHandle returns the inner io.FileHandle if fh is a *cachedHandle,
// otherwise fh itself. Pass-through file ops (Read, Write, Release, ...)
// use this so the gRPC backend's resolveHandle reaches the leaf
// *grpcFileHandle.
func unwrapHandle(fh io.FileHandle) io.FileHandle {
	if ch, ok := fh.(*cachedHandle); ok {
		return ch.inner
	}
	return fh
}

// joinPath joins parent and name using path.Join semantics, treating an
// empty parent (the mount root) as a no-op.
func joinPath(parent, name string) string {
	if parent == "" {
		return name
	}
	return path.Join(parent, name)
}

// pathParent returns the parent directory portion of p. "" represents the
// mount root in our path convention; the root's parent is also "".
func pathParent(p string) string {
	if p == "" || p == "/" {
		return ""
	}
	idx := strings.LastIndex(p, "/")
	if idx <= 0 {
		return ""
	}
	return p[:idx]
}
