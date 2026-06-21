// Package cache: backend.go is the integration glue. cachedBackend decorates
// an io.FileSystemBackend with three sub-caches (attr, dir, data) sharing a
// single byte-budget accountant. Read ops are read-through; mutating ops
// invalidate the appropriate cache slices per the Phase 4 Sub-spec B
// per-op invalidation table.
package cache

import (
	"context"
	"strings"

	"go.gmountie.dev/gmountie/pkg/client/cache/persist"
	"go.gmountie.dev/gmountie/pkg/client/io"
	"go.gmountie.dev/gmountie/pkg/client/metrics"
	"go.gmountie.dev/gmountie/pkg/proto"

	"github.com/hanwen/go-fuse/v2/fuse"
	"google.golang.org/grpc"
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
	xattr      *xattrCache
	statfs     *statfsCache
	validity   *validityTracker
	subscriber *subscribeConsumer
	subCancel  context.CancelFunc
	// persist is the on-disk backing store. Non-nil only when NewCachedBackend
	// was called with a non-nil *persist.Persist. Owned by this cachedBackend:
	// Close() shuts it down after stopping the subscriber.
	persist *persist.Persist
}

// invalidationSource is the one wire method the cache decorator needs from the
// gRPC client: opening the Subscribe stream that drives push-invalidation.
// NewCachedBackend takes this narrow interface instead of the full
// proto.RpcFsClient so the decorator can't reach past the FileSystemBackend
// abstraction into unrelated RPCs (AR-L1). proto.RpcFsClient satisfies it, so
// callers pass their existing client unchanged.
type invalidationSource interface {
	Subscribe(ctx context.Context, in *proto.SubscribeRequest, opts ...grpc.CallOption) (grpc.ServerStreamingClient[proto.SubscribeEvent], error)
}

// NewCachedBackend wraps inner. cfg.MemoryMaxBytes <= 0 disables byte-cap
// eviction in the memory tier (entries live until invalidated or the process
// dies; the disk tier still respects DiskMaxBytes independently). p may be
// nil for memory-only operation. client and volume are used to start the
// Subscribe-based invalidation goroutine; pass nil client to disable it.
func NewCachedBackend(inner io.FileSystemBackend, cfg Config, p *persist.Persist, client invalidationSource, volume string) io.FileSystemBackend {
	acct := newAccountant(cfg.MemoryMaxBytes, deriveMaxEntries(cfg.MemoryMaxBytes))
	b := &cachedBackend{
		inner:    inner,
		cfg:      cfg,
		acct:     acct,
		attr:     newAttrCacheWithPersist(acct, cfg.AttrTTL, cfg.NegativeTTL, nil, p),
		dir:      newDirCacheWithPersist(acct, cfg.DirTTL, nil, p),
		data:     newDataCacheWithPersist(acct, cfg.ChunkSizeBytes, p),
		xattr:    newXAttrCache(acct, cfg.XAttrTTL, nil),
		statfs:   newStatfsCache(cfg.StatFsTTL, nil),
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

// Close stops the subscriber goroutine (if running), flushes the data cache's
// async persist worker, closes the persist tier (if owned), and closes the
// inner backend. Mount code calls Close before discarding a backend on Unmount.
func (b *cachedBackend) Close() error {
	if b.subCancel != nil {
		b.subCancel()
	}
	var errs []error
	// Flush the data cache's async persist worker BEFORE closing the persist
	// tier, so any in-flight WriteChunk/PutChunkRef complete against an open DB.
	if b.data != nil {
		b.data.Close()
	}
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

func (a *subscribeBackendAdapter) invalidateAttr(p string)  { a.b.attr.invalidate(p) }
func (a *subscribeBackendAdapter) invalidateData(p string)  { a.b.data.invalidatePath(p) }
func (a *subscribeBackendAdapter) invalidateDir(p string)   { a.b.dir.invalidate(p) }
func (a *subscribeBackendAdapter) invalidateXAttr(p string) { a.b.xattr.invalidate(p) }
func (a *subscribeBackendAdapter) putNegative(p string)     { a.b.attr.putNegative(p) }

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
	// Capture the unverified epoch BEFORE the RPC. If a markGlobalUnverified
	// races us (the subscriber dropping the stream while this revalidation is
	// in flight), the stamp we later store carries this stale epoch and so is
	// non-authoritative — closing the CQ-M2 lost-update where a prior-epoch
	// stamp leaked into the new epoch and served stale attrs.
	epoch := b.validity.currentEpoch()
	attrs, notMod, st := b.inner.GetAttrIfChanged(ctx, path, cachedVersion)
	if st != proto.FsError_FS_OK && st != proto.FsError_FS_ENOENT {
		metrics.CacheRevalidation("error")
		return revalidateResult{fallback: true}
	}
	if notMod {
		b.validity.markPathVerified(path, epoch)
		metrics.CacheRevalidation("not_modified")
		return revalidateResult{notModified: true}
	}
	// Version changed or path gone: flush all three caches for this path.
	b.attr.invalidate(path)
	b.data.invalidatePath(path)
	b.dir.invalidate(pathParent(path))
	if st == proto.FsError_FS_ENOENT {
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
func (b *cachedBackend) GetAttrIfChanged(ctx context.Context, p string, knownVersion uint64) (*io.Attr, bool, proto.FsError) {
	return b.inner.GetAttrIfChanged(ctx, p, knownVersion)
}

// cachedAttrLookup is the shared attr-cache read path behind Stat and Lookup.
// Both are byte-identical apart from the cache key and the miss/fallback fetch,
// so they differ only in `key` and the `fromInner` closure: get → fast-path
// (globally verified or this-epoch path-verified) → lightweight revalidation →
// outcome switch. fromInner both fetches from inner AND primes the attr cache
// (statFromInner / lookupFromInner), so it is the single fallback for a miss
// and for a revalidation-RPC failure.
func (b *cachedBackend) cachedAttrLookup(
	ctx context.Context,
	key string,
	fromInner func() (*io.Attr, proto.FsError),
) (*io.Attr, proto.FsError) {
	cached, hit, pos := b.attr.get(key)
	if !hit {
		return fromInner()
	}
	// Fast path: globally verified or this path already revalidated this epoch.
	if b.validity.globalState() == stateVerified || b.validity.isPathVerified(key) {
		if pos {
			return cached, proto.FsError_FS_OK
		}
		return nil, proto.FsError_FS_ENOENT
	}
	// Unverified: run lightweight revalidation.
	knownVersion := uint64(0)
	if cached != nil {
		knownVersion = cached.Version
	}
	r := b.revalidate(ctx, key, knownVersion)
	switch {
	case r.notModified:
		if pos {
			return cached, proto.FsError_FS_OK
		}
		return nil, proto.FsError_FS_ENOENT
	case r.enoent:
		return nil, proto.FsError_FS_ENOENT
	case r.freshAttrs != nil:
		b.attr.putPositive(key, r.freshAttrs)
		return r.freshAttrs, proto.FsError_FS_OK
	default: // fallback: revalidation RPC itself failed
		return fromInner()
	}
}

func (b *cachedBackend) Stat(ctx context.Context, p string) (*io.Attr, proto.FsError) {
	return b.cachedAttrLookup(ctx, p, func() (*io.Attr, proto.FsError) {
		return b.statFromInner(ctx, p)
	})
}

// statFromInner fetches attrs from inner and populates the attr cache.
func (b *cachedBackend) statFromInner(ctx context.Context, p string) (*io.Attr, proto.FsError) {
	metrics.CacheMiss("attr")
	a, st := b.inner.Stat(ctx, p)
	if st == proto.FsError_FS_OK && a != nil {
		b.attr.putPositive(p, a)
	} else if st == proto.FsError_FS_ENOENT {
		b.attr.putNegative(p)
	}
	return a, st
}

func (b *cachedBackend) Lookup(ctx context.Context, parent, name string) (*io.Attr, proto.FsError) {
	full := joinPath(parent, name)
	return b.cachedAttrLookup(ctx, full, func() (*io.Attr, proto.FsError) {
		return b.lookupFromInner(ctx, parent, name, full)
	})
}

// lookupFromInner fetches from inner and populates the attr cache.
func (b *cachedBackend) lookupFromInner(ctx context.Context, parent, name, full string) (*io.Attr, proto.FsError) {
	metrics.CacheMiss("attr")
	a, st := b.inner.Lookup(ctx, parent, name)
	if st == proto.FsError_FS_OK && a != nil {
		b.attr.putPositive(full, a)
	} else if st == proto.FsError_FS_ENOENT {
		b.attr.putNegative(full)
	}
	return a, st
}

func (b *cachedBackend) ListDir(ctx context.Context, p string) ([]io.DirEntryPlus, proto.FsError) {
	if entries, hit := b.dir.get(p); hit {
		// Gate on validity: revalidate the directory's own attr to check for
		// freshness. Use the dir path as the revalidation key.
		if b.validity.globalState() == stateVerified || b.validity.isPathVerified(p) {
			return plusFromEntries(entries), proto.FsError_FS_OK
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
					return plusFromEntries(entries2), proto.FsError_FS_OK
				}
				// Dir cache was evicted in the meantime; fall through to inner.
			case r.enoent:
				return nil, proto.FsError_FS_ENOENT
			case r.freshAttrs != nil:
				// Dir changed: revalidate flushed the dir cache; fall through to
				// listDirFromInner to replace the stale listing.
			default:
				// Fallback: revalidation RPC error; serve cached listing.
				return plusFromEntries(entries), proto.FsError_FS_OK
			}
		}
		// Dir cached but attr unverified/changed: fall through to inner.
	}
	return b.listDirFromInner(ctx, p)
}

// listDirFromInner fetches from inner, primes the attr cache from plus
// entries, and populates the dir cache.
func (b *cachedBackend) listDirFromInner(ctx context.Context, p string) ([]io.DirEntryPlus, proto.FsError) {
	metrics.CacheMiss("dir")
	entries, st := b.inner.ListDir(ctx, p)
	if st == proto.FsError_FS_OK {
		// Prime the attr cache (standard positive TTL, same as Stat) from each
		// entry that carries attrs — this is the READDIRPLUS win: the kernel's
		// per-child LOOKUP after a readdir is served from cache with zero
		// backend calls. Entries with nil Attr (plus disabled, or the per-entry
		// stat failed server-side) prime nothing.
		stripped := make([]io.DirEntry, len(entries))
		for i, e := range entries {
			stripped[i] = e.DirEntry
			if e.Attr != nil {
				b.attr.putPositive(joinPath(p, e.Name), e.Attr)
			}
			if e.XattrListed {
				// Cache the names (empty == "no xattrs"), so the kernel's per-file
				// listxattr after this readdir is a local hit — the cold-pass win.
				b.xattr.put(joinPath(p, e.Name), e.XattrNames)
			}
		}
		// The dir cache keeps the plain dirent shape: per-path attrs live in
		// the attr cache only (one source of truth — duplicating them in the
		// listing would drift on per-path invalidation), and the persisted gob
		// format stays unchanged.
		b.dir.put(p, stripped)
	}
	return entries, st
}

// plusFromEntries widens a cached dirent listing to the interface's
// []io.DirEntryPlus shape. Cached listings carry no attrs (see
// listDirFromInner), so Attr is nil on every element — by the time a listing
// is served from cache, its attrs were already primed into the attr cache.
func plusFromEntries(entries []io.DirEntry) []io.DirEntryPlus {
	out := make([]io.DirEntryPlus, len(entries))
	for i, e := range entries {
		out[i] = io.DirEntryPlus{DirEntry: e}
	}
	return out
}

const (
	// prefetchSpanChunks is how many chunks a sequential cache miss over-reads
	// in ONE inner Read RPC. The server streams the span as a single sustained
	// stream (which fills the link — see the single-Read-RPC probe); fetching
	// chunks one per RPC instead stalls ~1 RTT each and defeats pipelining at
	// WAN. 16 * 1 MiB = 16 MiB per sequential reader, well within the memory
	// tier budget. The extra chunks are cached for the upcoming sequential reads.
	prefetchSpanChunks = 16
	// prefetchSeqThreshold is the consecutive in-order read count before the
	// over-read kicks in (mirrors the io-layer readahead threshold), so random
	// reads don't amplify bandwidth.
	prefetchSeqThreshold = 3
)

func (b *cachedBackend) Read(ctx context.Context, fh io.FileHandle, off int64, dest []byte) (int, proto.FsError) {
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
			return 0, proto.FsError_FS_ENOENT
		}
		// On freshAttrs: revalidate already invalidated data chunks; the chunk
		// lookup below will miss and fall through to inner — correct.
		// On notModified / fallback: continue to existing chunk-loop path.
	}
	chunkSize := int64(b.cfg.ChunkSizeBytes)
	// Decide once per FUSE read whether this is a sequential stream: if so, a
	// miss over-reads a span of chunks in one sustained inner Read RPC instead
	// of one-chunk-per-RPC (the WAN readahead-defeat fix).
	prefetch := ch.recordRead(int(off/chunkSize), prefetchSeqThreshold)
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
				return total, proto.FsError_FS_OK
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
				return total, proto.FsError_FS_OK
			}
			continue
		}
		// Miss: fetch from inner. On a sequential stream, over-read a span of
		// chunks in ONE inner Read RPC (a single sustained server-stream fills
		// the link; per-chunk RPCs stall ~1 RTT each — the WAN readahead-defeat).
		// chunkStart is the requested chunk's start, so it's the first chunk of
		// the span; the extra chunks are cached for the upcoming sequential reads.
		// If a concurrent over-read is already fetching this chunk, wait for it
		// and retry from cache rather than launching a duplicate, overlapping
		// span fetch (the bandwidth-amplification the goroutine dump exposed).
		if doneCh, waiting := ch.waitInflight(chunkIndex); waiting {
			<-doneCh
			continue
		}
		metrics.CacheMiss("data")
		spanChunks := 1
		if prefetch {
			spanChunks = prefetchSpanChunks
		}
		// For an over-read, publish the in-flight span so concurrent reads
		// covered by it wait (above) instead of duplicating the fetch.
		var finishSpan func()
		if spanChunks > 1 {
			finishSpan = ch.beginSpanFetch(chunkIndex, spanChunks)
		}
		// Capture the path's invalidation generation BEFORE sampling the server,
		// so the chunks cached below carry the generation as-of the read. If an
		// invalidation lands during/after the fetch, the persist of these chunks
		// is dropped (or undone) rather than resurrecting stale bytes on disk.
		spanGen := b.data.currentGen(ch.path)
		buf := make([]byte, spanChunks*int(chunkSize))
		n, st := b.inner.Read(ctx, ch.inner, chunkStart, buf)
		if st != proto.FsError_FS_OK {
			if finishSpan != nil {
				finishSpan()
			}
			return total, st
		}
		if n == 0 {
			if finishSpan != nil {
				finishSpan()
			}
			return total, proto.FsError_FS_OK
		}
		// Cache every chunk the span returned: full chunks plus a final short
		// chunk (EOF). Copy each into its own buffer so evicting one chunk does
		// not pin the whole span's backing array. inner.Read only short-reads at
		// EOF (BackendClient's streaming Recv loop), so a short return ends the
		// file — the same invariant the single-chunk path relied on.
		for j := 0; j*int(chunkSize) < n; j++ {
			cs := j * int(chunkSize)
			ce := cs + int(chunkSize)
			if ce > n {
				ce = n
			}
			cbuf := make([]byte, ce-cs)
			copy(cbuf, buf[cs:ce])
			b.data.putGen(ch.path, chunkIndex+j, cbuf, spanGen)
		}
		// Chunks cached; wake any waiters so they serve from cache.
		if finishSpan != nil {
			finishSpan()
		}
		// Serve the requested chunk — the first in the span, buf[0:served].
		served := n
		if served > int(chunkSize) {
			served = int(chunkSize)
		}
		if insideOff >= served {
			// EOF before our requested offset.
			return total, proto.FsError_FS_OK
		}
		avail := served - insideOff
		if avail < want {
			want = avail
		}
		copied := copy(dest[total:total+want], buf[insideOff:insideOff+want])
		total += copied
		// Short first chunk = file ends here; otherwise continue to the next
		// chunk (now served from the over-read cache).
		if served < int(chunkSize) {
			return total, proto.FsError_FS_OK
		}
	}
	return total, proto.FsError_FS_OK
}

func (b *cachedBackend) Access(ctx context.Context, p string, mode uint32) proto.FsError {
	return b.inner.Access(ctx, p, mode)
}

// Readlink is a pass-through. The link target is content of the link inode,
// not the inode's attrs — it's small (PATH_MAX) and rarely re-read, so we
// don't add a target cache yet.
func (b *cachedBackend) Readlink(ctx context.Context, p string) (string, proto.FsError) {
	return b.inner.Readlink(ctx, p)
}

// Symlink creates a new dirent (the link). Invalidates the parent dir +
// attr caches like Mkdir, and drops any negative-cached entry for the new
// path. The reply attrs prime the attr cache (like Create) so the kernel's
// immediate Lookup on the new link is a cache hit; nil attrs (server stat
// failed) leave the entry invalidated so the next Stat refetches.
func (b *cachedBackend) Symlink(ctx context.Context, target, linkPath string) (*io.Attr, proto.FsError) {
	a, st := b.inner.Symlink(ctx, target, linkPath)
	if st != proto.FsError_FS_OK {
		return nil, st
	}
	parent := pathParent(linkPath)
	b.dir.invalidate(parent)
	b.attr.invalidate(parent)
	b.attr.invalidate(linkPath)
	if a != nil {
		b.attr.putPositive(linkPath, a)
	}
	return a, proto.FsError_FS_OK
}

func (b *cachedBackend) StatFs(ctx context.Context, p string) (*io.StatFs, proto.FsError) {
	if v, ok := b.statfs.get(p); ok {
		return v, proto.FsError_FS_OK
	}
	v, st := b.inner.StatFs(ctx, p)
	if st == proto.FsError_FS_OK {
		b.statfs.put(p, v)
	}
	return v, st
}

func (b *cachedBackend) GetXAttr(ctx context.Context, p, attr string) ([]byte, proto.FsError) {
	return b.inner.GetXAttr(ctx, p, attr)
}

// SetXAttr stores an extended attribute, then drops the cached names list and
// the attr entry: an xattr write bumps the inode ctime, so the cached attr
// version is now stale too.
func (b *cachedBackend) SetXAttr(ctx context.Context, p, attr string, data []byte, flags uint32) proto.FsError {
	st := b.inner.SetXAttr(ctx, p, attr, data, flags)
	if st == proto.FsError_FS_OK {
		b.xattr.invalidate(p)
		b.attr.invalidate(p)
	}
	return st
}

// RemoveXAttr deletes an extended attribute; same invalidation as SetXAttr.
func (b *cachedBackend) RemoveXAttr(ctx context.Context, p, attr string) proto.FsError {
	st := b.inner.RemoveXAttr(ctx, p, attr)
	if st == proto.FsError_FS_OK {
		b.xattr.invalidate(p)
		b.attr.invalidate(p)
	}
	return st
}

// ListXAttr is read-through against the advisory xattr cache. It serves on
// TTL + invalidation only (no GetAttrIfChanged revalidation): a stale names
// list is at worst a wrong ls "+" indicator, never an enforcement decision.
func (b *cachedBackend) ListXAttr(ctx context.Context, p string) ([]string, proto.FsError) {
	if names, hit := b.xattr.get(p); hit {
		return names, proto.FsError_FS_OK
	}
	names, st := b.inner.ListXAttr(ctx, p)
	if st == proto.FsError_FS_OK {
		b.xattr.put(p, names)
	}
	return names, st
}

func (b *cachedBackend) GetLk(ctx context.Context, fh io.FileHandle, owner uint64, lk *fuse.FileLock, flags uint32, out *fuse.FileLock) proto.FsError {
	return b.inner.GetLk(ctx, unwrapHandle(fh), owner, lk, flags, out)
}

// --- Open / Create / file-handle ops ---

func (b *cachedBackend) Open(ctx context.Context, p string, flags uint32) (io.FileHandle, proto.FsError) {
	h, st := b.inner.Open(ctx, p, flags)
	if st != proto.FsError_FS_OK {
		return nil, st
	}
	return newCachedHandle(h, p), proto.FsError_FS_OK
}

func (b *cachedBackend) Create(ctx context.Context, parent, name string, flags, mode uint32) (io.FileHandle, *io.Attr, proto.FsError) {
	full := joinPath(parent, name)
	h, a, st := b.inner.Create(ctx, parent, name, flags, mode)
	if st != proto.FsError_FS_OK {
		return nil, nil, st
	}
	b.dir.invalidate(parent)
	b.attr.invalidate(parent)
	b.attr.invalidate(full) // drop any negative entry from a prior failed Stat
	if a != nil {
		b.attr.putPositive(full, a)
	}
	return newCachedHandle(h, full), a, proto.FsError_FS_OK
}

func (b *cachedBackend) Write(ctx context.Context, fh io.FileHandle, off int64, data []byte) (uint32, proto.FsError) {
	n, st := b.inner.Write(ctx, unwrapHandle(fh), off, data)
	if st != proto.FsError_FS_OK {
		return n, st
	}
	if ch, ok := fh.(*cachedHandle); ok {
		ch.wrote.Store(true)
		b.data.invalidateRange(ch.path, off, int64(len(data)))
		// Optimistic attr update instead of eviction. We just wrote n bytes at
		// off, so we are authoritative on the file's new minimum size: bump the
		// cached size and stamp the path verified so the GetAttr macOS/FUSE-T
		// fires after every write is served from cache, not a WAN round-trip.
		// Evicting here turned a 1 GB Finder copy into ~28k GetAttr RPCs (Linux
		// hides it behind the kernel attr cache; FUSE-T does not). Release
		// reconciles with the server's authoritative attrs.
		if b.attr.bumpSize(ch.path, off+int64(n)) {
			b.validity.markPathVerified(ch.path, b.validity.currentEpoch())
		}
	}
	return n, proto.FsError_FS_OK
}

func (b *cachedBackend) Release(ctx context.Context, fh io.FileHandle) proto.FsError {
	st := b.inner.Release(ctx, unwrapHandle(fh))
	if ch, ok := fh.(*cachedHandle); ok && ch.wrote.Load() {
		// The file was written with optimistic attrs (see Write); drop them so
		// the next Stat fetches the server's authoritative size/mtime/blocks.
		b.attr.invalidate(ch.path)
	}
	return st
}

func (b *cachedBackend) Flush(ctx context.Context, fh io.FileHandle) proto.FsError {
	return b.inner.Flush(ctx, unwrapHandle(fh))
}

func (b *cachedBackend) Fsync(ctx context.Context, fh io.FileHandle, flags int64) proto.FsError {
	return b.inner.Fsync(ctx, unwrapHandle(fh), flags)
}

func (b *cachedBackend) Allocate(ctx context.Context, fh io.FileHandle, off, size uint64, mode uint32) proto.FsError {
	st := b.inner.Allocate(ctx, unwrapHandle(fh), off, size, mode)
	if st != proto.FsError_FS_OK {
		return st
	}
	if ch, ok := fh.(*cachedHandle); ok {
		ch.wrote.Store(true)
		b.data.invalidateRange(ch.path, int64(off), int64(size))
		// Same optimistic-update rationale as Write: allocation can grow the
		// file, so bump the cached size and keep the entry verified rather than
		// evicting it. Release reconciles with the server.
		if b.attr.bumpSize(ch.path, int64(off+size)) {
			b.validity.markPathVerified(ch.path, b.validity.currentEpoch())
		}
	}
	return proto.FsError_FS_OK
}

func (b *cachedBackend) SetLk(ctx context.Context, fh io.FileHandle, owner uint64, lk *fuse.FileLock, flags uint32) proto.FsError {
	return b.inner.SetLk(ctx, unwrapHandle(fh), owner, lk, flags)
}

func (b *cachedBackend) SetLkw(ctx context.Context, fh io.FileHandle, owner uint64, lk *fuse.FileLock, flags uint32) proto.FsError {
	return b.inner.SetLkw(ctx, unwrapHandle(fh), owner, lk, flags)
}

// CopyFileRange passes through, then invalidates the DESTINATION like a
// Write of the copied range: the data cache for [offOut, offOut+n) and
// the attr entry (size/mtime moved). Source is untouched (atime only).
func (b *cachedBackend) CopyFileRange(ctx context.Context, fhIn io.FileHandle, offIn uint64, fhOut io.FileHandle, offOut uint64, length, flags uint64) (uint64, proto.FsError) {
	n, st := b.inner.CopyFileRange(ctx, unwrapHandle(fhIn), offIn, unwrapHandle(fhOut), offOut, length, flags)
	if st != proto.FsError_FS_OK {
		return n, st
	}
	if ch, ok := fhOut.(*cachedHandle); ok && n > 0 {
		b.data.invalidateRange(ch.path, int64(offOut), int64(n))
		b.attr.invalidate(ch.path)
	}
	return n, proto.FsError_FS_OK
}

// Lseek is a pure pass-through — hole geometry isn't cached.
func (b *cachedBackend) Lseek(ctx context.Context, fh io.FileHandle, offset uint64, whence uint32) (uint64, proto.FsError) {
	return b.inner.Lseek(ctx, unwrapHandle(fh), offset, whence)
}

// --- Path-level mutating ops ---

// Mkdir invalidates the parent (new dirent bumps its mtime) and primes the
// attr cache from the reply attrs (like Create); nil attrs (server stat
// failed) just drop any negative-cached entry so the next Stat refetches.
func (b *cachedBackend) Mkdir(ctx context.Context, p string, mode uint32) (*io.Attr, proto.FsError) {
	a, st := b.inner.Mkdir(ctx, p, mode)
	if st != proto.FsError_FS_OK {
		return nil, st
	}
	parent := pathParent(p)
	b.dir.invalidate(parent)
	b.attr.invalidate(parent)
	b.attr.invalidate(p) // drop any negative-cached entry for the just-created path
	if a != nil {
		b.attr.putPositive(p, a)
	}
	return a, proto.FsError_FS_OK
}

func (b *cachedBackend) Rmdir(ctx context.Context, p string) proto.FsError {
	st := b.inner.Rmdir(ctx, p)
	if st != proto.FsError_FS_OK {
		return st
	}
	b.attr.invalidate(p)
	b.dir.invalidate(p)
	parent := pathParent(p)
	b.dir.invalidate(parent)
	b.attr.invalidate(parent) // Rmdir changes the parent's mtime
	b.attr.putNegative(p)
	return proto.FsError_FS_OK
}

func (b *cachedBackend) Unlink(ctx context.Context, p string) proto.FsError {
	st := b.inner.Unlink(ctx, p)
	if st != proto.FsError_FS_OK {
		return st
	}
	b.attr.invalidate(p)
	b.data.invalidatePath(p)
	parent := pathParent(p)
	b.dir.invalidate(parent)
	b.attr.invalidate(parent) // Unlink changes the parent's mtime
	b.attr.putNegative(p)
	return proto.FsError_FS_OK
}

func (b *cachedBackend) Rename(ctx context.Context, oldPath, newPath string) proto.FsError {
	st := b.inner.Rename(ctx, oldPath, newPath)
	if st != proto.FsError_FS_OK {
		return st
	}
	b.attr.invalidate(oldPath)
	b.attr.invalidate(newPath)
	b.data.invalidatePath(oldPath)
	b.data.invalidatePath(newPath)
	oldParent := pathParent(oldPath)
	newParent := pathParent(newPath)
	b.dir.invalidate(oldParent)
	b.dir.invalidate(newParent)
	b.attr.invalidate(oldParent) // Rename changes the mtime of both parent dirs
	b.attr.invalidate(newParent)
	b.attr.putNegative(oldPath)
	return proto.FsError_FS_OK
}

// SetAttr forwards the single-RPC attribute application, then reconciles the
// caches: FATTR_SIZE drops every data chunk for p (truncate changes content —
// same conservatism as Truncate), and the attr entry is re-primed from the
// returned final attrs (like Create primes from CreateReply). The parent is
// untouched — setattr never changes the parent directory. On failure the
// server may have applied EARLIER fields before stopping (size→mode→owner→
// times), so unlike the other mutation wrappers we still invalidate
// conservatively rather than assuming nothing changed.
func (b *cachedBackend) SetAttr(ctx context.Context, p string, in io.SetAttrIn) (*io.Attr, proto.FsError) {
	a, st := b.inner.SetAttr(ctx, p, in)
	// A requested size change makes every cached chunk suspect on success
	// AND on failure: size applies first server-side, so even a failed call
	// may already have truncated the file.
	if in.Valid&fuse.FATTR_SIZE != 0 {
		b.data.invalidatePath(p)
	}
	if st != proto.FsError_FS_OK {
		b.attr.invalidate(p)
		return nil, st
	}
	if a != nil {
		b.attr.putPositive(p, a)
	} else {
		// Server omitted the final attrs: drop the stale entry so the next
		// Stat refetches.
		b.attr.invalidate(p)
	}
	return a, proto.FsError_FS_OK
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

// joinPath joins parent and name into the cache key. It MUST mirror the io
// layer's join byte-for-byte (pkg/client/io/backend_grpc.go joinPath: raw
// parent + "/" + name, empty-parent passthrough) so a cache key always equals
// the wire path the server sees. The wire path is the invalidation source of
// truth — path.Join normalization here would let the same (parent,name) yield a
// cache key that never matches a Subscribe event's path (MN-L2). FUSE paths are
// already clean, so normalization buys nothing.
func joinPath(parent, name string) string {
	if parent == "" {
		return name
	}
	return parent + "/" + name
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
