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
	"time"

	"go.gmountie.dev/gmountie/pkg/client/cache/persist"
	"go.gmountie.dev/gmountie/pkg/client/io"
	"go.gmountie.dev/gmountie/pkg/client/metrics"
	"go.gmountie.dev/gmountie/pkg/proto"

	"github.com/hanwen/go-fuse/v2/fuse"
)

// cachedBackend decorates an inner FileSystemBackend with three
// sub-caches sharing one accountant. Construct via NewCachedBackend;
// implements io.FileSystemBackend.
type cachedBackend struct {
	inner      io.FileSystemBackend
	cfg        Config
	acct       *accountant
	attr       *attrCache
	dir        *dirCache
	data       *dataCache
	validity   *validityTracker
	subscriber *subscribeConsumer
	subCancel  context.CancelFunc
	// persist is the on-disk backing store. Non-nil only when NewCachedBackend
	// was called with a non-nil *persist.Persist. Owned by this cachedBackend:
	// Close() shuts it down after stopping the subscriber.
	persist *persist.Persist
}

// NewCachedBackend wraps inner. cfg.MemoryMaxBytes <= 0 disables byte-cap
// eviction in the memory tier (entries live until invalidated or the process
// dies; the disk tier still respects DiskMaxBytes independently). p may be
// nil for memory-only operation. client and volume are used to start the
// Subscribe-based invalidation goroutine; pass nil client to disable it.
func NewCachedBackend(inner io.FileSystemBackend, cfg Config, p *persist.Persist, client proto.RpcFsClient, volume string) io.FileSystemBackend {
	acct := newAccountant(cfg.MemoryMaxBytes)
	b := &cachedBackend{
		inner:    inner,
		cfg:      cfg,
		acct:     acct,
		attr:     newAttrCacheWithPersist(acct, cfg.AttrTTL, cfg.NegativeTTL, nil, p),
		dir:      newDirCacheWithPersist(acct, cfg.DirTTL, nil, p),
		data:     newDataCacheWithPersist(acct, cfg.ChunkSizeBytes, p),
		validity: newValidityTracker(),
		persist:  p,
	}
	if !cfg.SubscribeEnabled {
		// Subscribe disabled: freshness is TTL-driven only. Mark the
		// cache globally verified at construction so reads never pay an
		// extra GetAttrIfChanged RTT — the relaxed TTL is the sole
		// eviction signal in this mode, matching Sub-spec C behaviour.
		b.validity.markGlobalVerified()
	} else if client != nil && volume != "" {
		// Subscribe enabled and a real gRPC client is available: start
		// the push-invalidation goroutine. The validity tracker stays
		// unverified until the first HEARTBEAT arrives.
		ctx, cancel := context.WithCancel(context.Background())
		b.subCancel = cancel
		b.subscriber = newSubscribeConsumer(client, volume, &subscribeBackendAdapter{b}, b.validity)
		go b.subscriber.run(ctx)
	}
	// else: SubscribeEnabled=true but no client (test scenarios or future
	// offline mode) — tracker stays unverified; gating logic applies.
	return b
}

// Close stops the subscriber goroutine (if running), closes the persist
// tier (if owned), and closes the inner backend. Mount code calls Close
// before discarding a backend on Unmount.
func (b *cachedBackend) Close() error {
	if b.subCancel != nil {
		b.subCancel()
	}
	var errs []error
	if b.persist != nil {
		if err := b.persist.Close(); err != nil {
			errs = append(errs, err)
		}
	}
	if err := b.inner.Close(); err != nil {
		errs = append(errs, err)
	}
	if len(errs) > 0 {
		return errs[0]
	}
	return nil
}

// subscribeBackendAdapter bridges cachedBackend to the subscribeBackendOps
// interface consumed by subscribeConsumer. Kept as a thin wrapper so the
// subscriber is independently testable without a full cachedBackend.
type subscribeBackendAdapter struct{ b *cachedBackend }

func (a *subscribeBackendAdapter) invalidateAttr(p string) { a.b.attr.invalidate(p) }
func (a *subscribeBackendAdapter) invalidateData(p string) { a.b.data.invalidatePath(p) }
func (a *subscribeBackendAdapter) invalidateDir(p string)  { a.b.dir.invalidate(p) }
func (a *subscribeBackendAdapter) putNegative(p string)    { a.b.attr.putNegative(p) }

// revalidateResult carries the outcome of a GetAttrIfChanged revalidation
// call made by the gating logic in Stat/Lookup/ListDir/Read.
type revalidateResult struct {
	notModified bool     // server confirmed version unchanged
	enoent      bool     // path is gone on the server
	freshAttrs  *io.Attr // new attrs when version changed
	fallback    bool     // revalidation RPC itself failed; caller falls through to inner
}

// revalidate calls GetAttrIfChanged on inner and interprets the result,
// updating the validity tracker and invalidating caches as appropriate.
func (b *cachedBackend) revalidate(ctx context.Context, path string, cachedVersion uint64) revalidateResult {
	attrs, notMod, st := b.inner.GetAttrIfChanged(ctx, path, cachedVersion)
	if !st.Ok() && st != fuse.ENOENT {
		metrics.CacheRevalidation("error")
		return revalidateResult{fallback: true}
	}
	if notMod {
		b.validity.markPathVerified(path)
		metrics.CacheRevalidation("not_modified")
		return revalidateResult{notModified: true}
	}
	// Version changed or path gone: flush all three caches for this path.
	b.attr.invalidate(path)
	b.data.invalidatePath(path)
	b.dir.invalidate(pathParent(path))
	if st == fuse.ENOENT {
		b.attr.putNegative(path)
		metrics.CacheRevalidation("enoent")
		return revalidateResult{enoent: true}
	}
	metrics.CacheRevalidation("changed")
	return revalidateResult{freshAttrs: attrs}
}

// --- Read path ---

// GetAttrIfChanged passes through to inner; cachedBackend does not
// intercept this call — it is the mechanism by which higher-level Stat
// gating works, not a cacheable operation itself.
func (b *cachedBackend) GetAttrIfChanged(ctx context.Context, p string, knownVersion uint64) (*io.Attr, bool, fuse.Status) {
	return b.inner.GetAttrIfChanged(ctx, p, knownVersion)
}

func (b *cachedBackend) Stat(ctx context.Context, p string) (*io.Attr, fuse.Status) {
	cached, hit, pos := b.attr.get(p)
	if !hit {
		return b.statFromInner(ctx, p)
	}
	// Fast path: globally verified or this path already revalidated this epoch.
	if b.validity.globalState() == stateVerified || b.validity.isPathVerified(p) {
		if pos {
			return cached, fuse.OK
		}
		return nil, fuse.ENOENT
	}
	// Unverified: run lightweight revalidation.
	knownVersion := uint64(0)
	if cached != nil {
		knownVersion = cached.Version
	}
	r := b.revalidate(ctx, p, knownVersion)
	switch {
	case r.notModified:
		if pos {
			return cached, fuse.OK
		}
		return nil, fuse.ENOENT
	case r.enoent:
		return nil, fuse.ENOENT
	case r.freshAttrs != nil:
		b.attr.putPositive(p, r.freshAttrs)
		return r.freshAttrs, fuse.OK
	default: // fallback: revalidation RPC itself failed
		return b.statFromInner(ctx, p)
	}
}

// statFromInner fetches attrs from inner and populates the attr cache.
func (b *cachedBackend) statFromInner(ctx context.Context, p string) (*io.Attr, fuse.Status) {
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
	cached, hit, pos := b.attr.get(full)
	if !hit {
		return b.lookupFromInner(ctx, parent, name, full)
	}
	if b.validity.globalState() == stateVerified || b.validity.isPathVerified(full) {
		if pos {
			return cached, fuse.OK
		}
		return nil, fuse.ENOENT
	}
	knownVersion := uint64(0)
	if cached != nil {
		knownVersion = cached.Version
	}
	r := b.revalidate(ctx, full, knownVersion)
	switch {
	case r.notModified:
		if pos {
			return cached, fuse.OK
		}
		return nil, fuse.ENOENT
	case r.enoent:
		return nil, fuse.ENOENT
	case r.freshAttrs != nil:
		b.attr.putPositive(full, r.freshAttrs)
		return r.freshAttrs, fuse.OK
	default:
		return b.lookupFromInner(ctx, parent, name, full)
	}
}

// lookupFromInner fetches from inner and populates the attr cache.
func (b *cachedBackend) lookupFromInner(ctx context.Context, parent, name, full string) (*io.Attr, fuse.Status) {
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
		// Gate on validity: revalidate the directory's own attr to check for
		// freshness. Use the dir path as the revalidation key.
		if b.validity.globalState() == stateVerified || b.validity.isPathVerified(p) {
			return entries, fuse.OK
		}
		// Run revalidation on the directory itself.
		cached, _, pos := b.attr.get(p)
		if pos {
			knownVersion := uint64(0)
			if cached != nil {
				knownVersion = cached.Version
			}
			r := b.revalidate(ctx, p, knownVersion)
			switch {
			case r.notModified:
				// Directory attr unchanged; listing is still valid.
				// Re-fetch from dir cache (revalidate may not have changed it).
				if entries2, hit2 := b.dir.get(p); hit2 {
					return entries2, fuse.OK
				}
				// Dir cache was evicted in the meantime; fall through to inner.
			case r.enoent:
				return nil, fuse.ENOENT
			case r.freshAttrs != nil:
				// Dir changed: revalidate flushed the dir cache; fall through to
				// listDirFromInner to replace the stale listing.
			default:
				// Fallback: revalidation RPC error; serve cached listing.
				return entries, fuse.OK
			}
		}
		// Dir cached but attr unverified/changed: fall through to inner.
	}
	return b.listDirFromInner(ctx, p)
}

// listDirFromInner fetches from inner and populates the dir cache.
func (b *cachedBackend) listDirFromInner(ctx context.Context, p string) ([]io.DirEntry, fuse.Status) {
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
	// Gate data reads on validity: if unverified, revalidate the file's attr
	// first. A version change invalidates data chunks so the miss path below
	// will refetch from inner.
	if b.validity.globalState() != stateVerified && !b.validity.isPathVerified(ch.path) {
		cached, _, _ := b.attr.get(ch.path)
		knownVersion := uint64(0)
		if cached != nil {
			knownVersion = cached.Version
		}
		r := b.revalidate(ctx, ch.path, knownVersion)
		if r.enoent {
			return 0, fuse.ENOENT
		}
		// On freshAttrs: revalidate already invalidated data chunks; the chunk
		// lookup below will miss and fall through to inner — correct.
		// On notModified / fallback: continue to existing chunk-loop path.
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

// Readlink is a pass-through. The link target is content of the link inode,
// not the inode's attrs — it's small (PATH_MAX) and rarely re-read, so we
// don't add a target cache yet.
func (b *cachedBackend) Readlink(ctx context.Context, p string) (string, fuse.Status) {
	return b.inner.Readlink(ctx, p)
}

// Symlink creates a new dirent (the link). Invalidates the parent dir +
// attr caches like Mkdir, and drops any negative-cached entry for the new
// path so a follow-up Lookup re-reads.
func (b *cachedBackend) Symlink(ctx context.Context, target, linkPath string) fuse.Status {
	st := b.inner.Symlink(ctx, target, linkPath)
	if st != fuse.OK {
		return st
	}
	parent := pathParent(linkPath)
	b.dir.invalidate(parent)
	b.attr.invalidate(parent)
	b.attr.invalidate(linkPath)
	return fuse.OK
}

func (b *cachedBackend) StatFs(ctx context.Context, p string) (*io.StatFs, fuse.Status) {
	return b.inner.StatFs(ctx, p)
}

func (b *cachedBackend) GetXAttr(ctx context.Context, p, attr string) ([]byte, fuse.Status) {
	return b.inner.GetXAttr(ctx, p, attr)
}

// SetXAttr stores an extended attribute. Xattrs are not cached and
// don't affect the stat-shaped attr cache.
func (b *cachedBackend) SetXAttr(ctx context.Context, p, attr string, data []byte, flags uint32) fuse.Status {
	return b.inner.SetXAttr(ctx, p, attr, data, flags)
}

// RemoveXAttr deletes an extended attribute. Pass-through like GetXAttr.
func (b *cachedBackend) RemoveXAttr(ctx context.Context, p, attr string) fuse.Status {
	return b.inner.RemoveXAttr(ctx, p, attr)
}

// ListXAttr returns extended-attribute names. Pass-through like GetXAttr.
func (b *cachedBackend) ListXAttr(ctx context.Context, p string) ([]string, fuse.Status) {
	return b.inner.ListXAttr(ctx, p)
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

// CopyFileRange passes through, then invalidates the DESTINATION like a
// Write of the copied range: the data cache for [offOut, offOut+n) and
// the attr entry (size/mtime moved). Source is untouched (atime only).
func (b *cachedBackend) CopyFileRange(ctx context.Context, fhIn io.FileHandle, offIn uint64, fhOut io.FileHandle, offOut uint64, length, flags uint64) (uint64, fuse.Status) {
	n, st := b.inner.CopyFileRange(ctx, unwrapHandle(fhIn), offIn, unwrapHandle(fhOut), offOut, length, flags)
	if st != fuse.OK {
		return n, st
	}
	if ch, ok := fhOut.(*cachedHandle); ok && n > 0 {
		b.data.invalidateRange(ch.path, int64(offOut), int64(n))
		b.attr.invalidate(ch.path)
	}
	return n, fuse.OK
}

// Lseek is a pure pass-through — hole geometry isn't cached.
func (b *cachedBackend) Lseek(ctx context.Context, fh io.FileHandle, offset uint64, whence uint32) (uint64, fuse.Status) {
	return b.inner.Lseek(ctx, unwrapHandle(fh), offset, whence)
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

func (b *cachedBackend) Utimens(ctx context.Context, p string, atime, mtime *time.Time) fuse.Status {
	st := b.inner.Utimens(ctx, p, atime, mtime)
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
