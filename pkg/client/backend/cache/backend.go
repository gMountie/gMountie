// Package cache: backend.go is the integration glue. cachedBackend decorates
// an backend.FileSystemBackend with three sub-caches (attr, dir, data) sharing a
// single byte-budget accountant. Read ops are read-through; mutating ops
// invalidate the appropriate cache slices per the Phase 4 Sub-spec B
// per-op invalidation table.
package cache

import (
	"context"
	"strings"

	"go.gmountie.dev/gmountie/pkg/client/backend"
	"go.gmountie.dev/gmountie/pkg/client/backend/cache/persist"
	"go.gmountie.dev/gmountie/pkg/client/metrics"
	"go.gmountie.dev/gmountie/pkg/proto"

	"google.golang.org/grpc"
)

// cachedBackend decorates an inner FileSystemBackend with three
// sub-caches sharing one accountant. Construct via NewCachedBackend;
// implements backend.FileSystemBackend.
//
// It embeds backend.PassthroughBackend (which holds the Inner backend and forwards
// every op) and overrides the read-through, invalidating, handle-wrapping, and
// lifecycle ops below. The pure unwrap-forward handle ops (Flush/Fsync/locks/
// Lseek) and the non-cached forwards (GetAttrIfChanged/Readlink) ride the
// embedded passthrough: both resolveHandle impls (memfs + gRPC) walk
// FileHandle.Unwrap(), so forwarding a wrapped *cachedHandle reaches the same
// leaf the old explicit unwrapHandle(fh) reached. A future interface method is
// caught by backend.TestFileSystemBackendMethodSet, which forces a per-layer review.
type cachedBackend struct {
	backend.PassthroughBackend
	cfg        Config
	acct       *accountant
	attr       *attrCache
	dir        *dirCache
	data       *dataCache
	xattr      *xattrCache
	getxattr   *getXAttrCache
	statfs     *statfsCache
	access     *accessCache
	validity   *validityTracker
	subscriber *subscribeConsumer
	subCancel  context.CancelFunc
	// rec is the injected metrics sink. NewCachedBackend defaults a nil rec to
	// metrics.NopRecorder{}, so it is never nil and the emission sites (and the
	// sub-components it's threaded into) never have to nil-check.
	rec metrics.Recorder
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
// Subscribe-based invalidation goroutine; pass nil client to disable it. rec is
// the metrics sink; a nil rec is replaced with metrics.NopRecorder{} so the
// cache and its sub-components never nil-check the recorder.
func NewCachedBackend(inner backend.FileSystemBackend, cfg Config, p *persist.Persist, client invalidationSource, volume string, rec metrics.Recorder) backend.FileSystemBackend {
	if rec == nil {
		rec = metrics.NopRecorder{}
	}
	acct := newAccountant(cfg.MemoryMaxBytes, deriveMaxEntries(cfg.MemoryMaxBytes))
	b := &cachedBackend{
		PassthroughBackend: backend.PassthroughBackend{Inner: inner},
		cfg:                cfg,
		acct:               acct,
		attr:               newAttrCacheWithPersist(acct, cfg.AttrTTL, cfg.NegativeTTL, nil, p, rec),
		dir:                newDirCacheWithPersist(acct, cfg.DirTTL, nil, p, rec),
		data:               newDataCacheWithPersist(acct, cfg.ChunkSizeBytes, p, rec),
		xattr:              newXAttrCache(acct, cfg.XAttrTTL, nil, rec),
		getxattr:           newGetXAttrCache(cfg.XAttrTTL, nil),
		statfs:             newStatfsCache(cfg.StatFsTTL, nil),
		access:             newAccessCache(cfg.AttrTTL, nil),
		validity:           newValidityTracker(),
		persist:            p,
		rec:                rec,
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
		b.subscriber = newSubscribeConsumer(client, volume, &subscribeBackendAdapter{b}, b.validity, rec)
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
	if err := b.Inner.Close(); err != nil {
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

func (a *subscribeBackendAdapter) invalidateAttr(p string) {
	a.b.attr.invalidate(p)
	a.b.access.invalidate(p) // a remote attr (perm) change invalidates the access decision
}
func (a *subscribeBackendAdapter) invalidateData(p string) { a.b.data.invalidatePath(p) }
func (a *subscribeBackendAdapter) invalidateDir(p string)  { a.b.dir.invalidate(p) }
func (a *subscribeBackendAdapter) invalidateXAttr(p string) {
	a.b.xattr.invalidate(p)
	a.b.getxattr.invalidate(p)
}
func (a *subscribeBackendAdapter) putNegative(p string)       { a.b.attr.putNegative(p) }
func (a *subscribeBackendAdapter) invalidateSubtree(p string) { a.b.invalidateSubtree(p) }

// invalidateSubtree drops every cached entry for path and all its descendants
// across all sub-caches. A single directory rename or recursive delete moves or
// removes a whole subtree in one op; per-key invalidation leaves descendant
// entries behind, which then resurface as phantom "already exists" results (and
// potentially stale data) when a path of the same name is recreated (issue
// #159).
func (b *cachedBackend) invalidateSubtree(path string) {
	b.attr.invalidatePrefix(path)
	b.dir.invalidatePrefix(path)
	b.data.invalidatePrefix(path)
	b.xattr.invalidatePrefix(path)
	b.getxattr.invalidatePrefix(path)
	b.access.invalidatePrefix(path)
}

// revalidateResult carries the outcome of a GetAttrIfChanged revalidation
// call made by the gating logic in Stat/Lookup/ListDir/Read.
type revalidateResult struct {
	notModified bool          // server confirmed version unchanged
	enoent      bool          // path is gone on the server
	freshAttrs  *backend.Attr // new attrs when version changed
	fallback    bool          // revalidation RPC itself failed; caller falls through to inner
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
	attrs, notMod, st := b.Inner.GetAttrIfChanged(ctx, path, cachedVersion)
	if st != proto.FsError_FS_OK && st != proto.FsError_FS_ENOENT {
		b.rec.CacheRevalidationInc("error")
		return revalidateResult{fallback: true}
	}
	if notMod {
		b.validity.markPathVerified(path, epoch)
		b.rec.CacheRevalidationInc("not_modified")
		return revalidateResult{notModified: true}
	}
	// Version changed or path gone: flush all three caches for this path.
	b.attr.invalidate(path)
	b.data.invalidatePath(path)
	b.dir.invalidate(pathParent(path))
	if st == proto.FsError_FS_ENOENT {
		b.attr.putNegative(path)
		b.rec.CacheRevalidationInc("enoent")
		return revalidateResult{enoent: true}
	}
	b.rec.CacheRevalidationInc("changed")
	return revalidateResult{freshAttrs: attrs}
}

// --- Read path ---

// GetAttrIfChanged is NOT overridden: it forwards to Inner via the embedded
// PassthroughBackend. It is the mechanism the gating logic in Stat/Lookup/
// ListDir/Read drives (via b.Inner.GetAttrIfChanged in revalidate), not a
// cacheable operation itself, so a bare forward is correct.

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
	fromInner func() (*backend.Attr, proto.FsError),
) (*backend.Attr, proto.FsError) {
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

func (b *cachedBackend) Stat(ctx context.Context, p string) (*backend.Attr, proto.FsError) {
	return b.cachedAttrLookup(ctx, p, func() (*backend.Attr, proto.FsError) {
		return b.statFromInner(ctx, p)
	})
}

// statFromInner fetches attrs from inner and populates the attr cache.
func (b *cachedBackend) statFromInner(ctx context.Context, p string) (*backend.Attr, proto.FsError) {
	b.rec.CacheMissInc("attr")
	a, st := b.Inner.Stat(ctx, p)
	if st == proto.FsError_FS_OK && a != nil {
		b.attr.putPositive(p, a)
	} else if st == proto.FsError_FS_ENOENT {
		b.attr.putNegative(p)
	}
	return a, st
}

func (b *cachedBackend) Lookup(ctx context.Context, parent, name string) (*backend.Attr, proto.FsError) {
	full := joinPath(parent, name)
	return b.cachedAttrLookup(ctx, full, func() (*backend.Attr, proto.FsError) {
		return b.lookupFromInner(ctx, parent, name, full)
	})
}

// lookupFromInner fetches from inner and populates the attr cache.
func (b *cachedBackend) lookupFromInner(ctx context.Context, parent, name, full string) (*backend.Attr, proto.FsError) {
	b.rec.CacheMissInc("attr")
	a, st := b.Inner.Lookup(ctx, parent, name)
	if st == proto.FsError_FS_OK && a != nil {
		b.attr.putPositive(full, a)
	} else if st == proto.FsError_FS_ENOENT {
		b.attr.putNegative(full)
	}
	return a, st
}

func (b *cachedBackend) ListDir(ctx context.Context, p string) ([]backend.DirEntryPlus, proto.FsError) {
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
func (b *cachedBackend) listDirFromInner(ctx context.Context, p string) ([]backend.DirEntryPlus, proto.FsError) {
	b.rec.CacheMissInc("dir")
	entries, st := b.Inner.ListDir(ctx, p)
	if st == proto.FsError_FS_OK {
		// Prime the attr cache (standard positive TTL, same as Stat) from each
		// entry that carries attrs — this is the READDIRPLUS win: the kernel's
		// per-child LOOKUP after a readdir is served from cache with zero
		// backend calls. Entries with nil Attr (plus disabled, or the per-entry
		// stat failed server-side) prime nothing.
		stripped := make([]backend.DirEntry, len(entries))
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
// []backend.DirEntryPlus shape. Cached listings carry no attrs (see
// listDirFromInner), so Attr is nil on every element — by the time a listing
// is served from cache, its attrs were already primed into the attr cache.
func plusFromEntries(entries []backend.DirEntry) []backend.DirEntryPlus {
	out := make([]backend.DirEntryPlus, len(entries))
	for i, e := range entries {
		out[i] = backend.DirEntryPlus{DirEntry: e}
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

func (b *cachedBackend) Read(ctx context.Context, fh backend.FileHandle, off int64, dest []byte) (int, proto.FsError) {
	ch, ok := fh.(*cachedHandle)
	if !ok {
		return b.Inner.Read(ctx, fh, off, dest)
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
		b.rec.CacheMissInc("data")
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
		n, st := b.Inner.Read(ctx, ch.inner, chunkStart, buf)
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
	if st, ok := b.access.get(p, mode); ok {
		return st
	}
	st := b.Inner.Access(ctx, p, mode)
	// Cache only stable access decisions; transient errors (EIO, timeouts, etc.)
	// must re-evaluate. The cache is invalidated on any perm/existence change.
	switch st {
	case proto.FsError_FS_OK, proto.FsError_FS_EACCES, proto.FsError_FS_EPERM, proto.FsError_FS_ENOENT:
		b.access.put(p, mode, st)
	}
	return st
}

// Readlink is NOT overridden: it forwards to Inner via the embedded
// PassthroughBackend. The link target is content of the link inode, not the
// inode's attrs — it's small (PATH_MAX) and rarely re-read, so we don't add a
// target cache yet.

// Symlink creates a new dirent (the link). Invalidates the parent dir +
// attr caches like Mkdir, and drops any negative-cached entry for the new
// path. The reply attrs prime the attr cache (like Create) so the kernel's
// immediate Lookup on the new link is a cache hit; nil attrs (server stat
// failed) leave the entry invalidated so the next Stat refetches.
func (b *cachedBackend) Symlink(ctx context.Context, target, linkPath string) (*backend.Attr, proto.FsError) {
	a, st := b.Inner.Symlink(ctx, target, linkPath)
	if st != proto.FsError_FS_OK {
		return nil, st
	}
	parent := pathParent(linkPath)
	b.dir.invalidate(parent)
	b.attr.bumpParentEntryAdded(parent) // #158: symlink create doesn't change parent nlink (see Create)
	b.attr.invalidate(linkPath)
	b.access.invalidate(linkPath)
	if a != nil {
		b.attr.putPositive(linkPath, a)
	}
	return a, proto.FsError_FS_OK
}

func (b *cachedBackend) StatFs(ctx context.Context, p string) (*backend.StatFs, proto.FsError) {
	if v, ok := b.statfs.get(p); ok {
		return v, proto.FsError_FS_OK
	}
	v, st := b.Inner.StatFs(ctx, p)
	if st == proto.FsError_FS_OK {
		b.statfs.put(p, v)
	}
	return v, st
}

// GetXAttr is read-through against the advisory getxattr cache. The big win is
// caching the NEGATIVE answer (ENO_XATTR / ENOTSUP / ENOSYS): the kernel and
// tools like npm probe security.capability / system.posix_acl_* on nearly every
// file, and absent those caches each probe was one ~WAN-RTT RPC. Caching the
// full value also collapses the app's two-call size-probe (size=0 then real
// read) — node.go re-derives ERANGE from len(data), so it still works from a
// cached value. Only stable statuses are cached; transient errors re-evaluate.
func (b *cachedBackend) GetXAttr(ctx context.Context, p, attr string) ([]byte, proto.FsError) {
	if data, st, ok := b.getxattr.get(p, attr); ok {
		return data, st
	}
	data, st := b.Inner.GetXAttr(ctx, p, attr)
	switch st {
	case proto.FsError_FS_OK, proto.FsError_FS_ENO_XATTR, proto.FsError_FS_ENOTSUP, proto.FsError_FS_ENOSYS:
		b.getxattr.put(p, attr, data, st)
	}
	return data, st
}

// SetXAttr stores an extended attribute, then drops the cached names list, the
// per-attr value cache, and the attr entry: an xattr write bumps the inode
// ctime, so the cached attr version is now stale too.
func (b *cachedBackend) SetXAttr(ctx context.Context, p, attr string, data []byte, flags uint32) proto.FsError {
	st := b.Inner.SetXAttr(ctx, p, attr, data, flags)
	if st == proto.FsError_FS_OK {
		b.xattr.invalidate(p)
		b.getxattr.invalidate(p)
		b.attr.invalidate(p)
	}
	return st
}

// RemoveXAttr deletes an extended attribute; same invalidation as SetXAttr.
func (b *cachedBackend) RemoveXAttr(ctx context.Context, p, attr string) proto.FsError {
	st := b.Inner.RemoveXAttr(ctx, p, attr)
	if st == proto.FsError_FS_OK {
		b.xattr.invalidate(p)
		b.getxattr.invalidate(p)
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
	names, st := b.Inner.ListXAttr(ctx, p)
	if st == proto.FsError_FS_OK {
		b.xattr.put(p, names)
	}
	return names, st
}

// GetLk/SetLk/SetLkw are NOT overridden: they forward to Inner via the embedded
// PassthroughBackend. The cache holds no lock state, and forwarding the wrapped
// *cachedHandle reaches the leaf because Inner's resolveHandle walks
// FileHandle.Unwrap() (same leaf the old explicit unwrapHandle reached).

// --- Open / Create / file-handle ops ---

func (b *cachedBackend) Open(ctx context.Context, p string, flags uint32) (backend.FileHandle, proto.FsError) {
	h, st := b.Inner.Open(ctx, p, flags)
	if st != proto.FsError_FS_OK {
		return nil, st
	}
	return newCachedHandle(h, p), proto.FsError_FS_OK
}

func (b *cachedBackend) Create(ctx context.Context, parent, name string, flags, mode uint32) (backend.FileHandle, *backend.Attr, proto.FsError) {
	full := joinPath(parent, name)
	h, a, st := b.Inner.Create(ctx, parent, name, flags, mode)
	if st != proto.FsError_FS_OK {
		return nil, nil, st
	}
	b.dir.invalidate(parent)
	// #158: refresh the parent's mtime/ctime instead of evicting it. Adding a
	// regular-file entry doesn't change the parent's mode/uid/gid (what
	// default_permissions checks) or nlink, so the cached parent stays correct
	// and the kernel's ancestor permission checks + the app's dir stats keep
	// hitting cache rather than taking a full GetAttr per created child.
	b.attr.bumpParentEntryAdded(parent)
	b.attr.invalidate(full) // drop any negative entry from a prior failed Stat
	b.access.invalidate(full)
	// #160: a brand-new file provably has no file capabilities, so prime a
	// negative security.capability so the kernel's per-write killpriv probe is a
	// local cache hit instead of one RPC per written file. A later SetXAttr on
	// this path invalidates the getxattr cache, so this can't go stale.
	b.getxattr.put(full, securityCapabilityXAttr, nil, proto.FsError_FS_ENO_XATTR)
	if a != nil {
		b.attr.putPositive(full, a)
	}
	return newCachedHandle(h, full), a, proto.FsError_FS_OK
}

func (b *cachedBackend) Write(ctx context.Context, fh backend.FileHandle, off int64, data []byte) (uint32, proto.FsError) {
	n, st := b.Inner.Write(ctx, unwrapHandle(fh), off, data)
	if st != proto.FsError_FS_OK {
		return n, st
	}
	if ch, ok := fh.(*cachedHandle); ok {
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

func (b *cachedBackend) Release(ctx context.Context, fh backend.FileHandle) proto.FsError {
	// Deliberately does NOT invalidate the attr for written handles. The
	// optimistic write-time attr (see Write) carries the pre-write version, so
	// the next unverified Stat's GetAttrIfChanged already detects the change and
	// refetches; Subscribe push reconciles when verified. Invalidating here
	// instead *removed* the persisted attr, so a cold restart saw an attr miss
	// (full GetAttr, not revalidation) and served stale data chunks that the
	// revalidation path would have invalidated (regression in e2e
	// TestRestartRevalidatesAfterMutation).
	return b.Inner.Release(ctx, unwrapHandle(fh))
}

// Flush/Fsync are NOT overridden: they forward to Inner via the embedded
// PassthroughBackend. The cache has no write buffer of its own to drain (the
// transport owns durability), and forwarding the wrapped *cachedHandle reaches
// the leaf via Inner's Unwrap-walking resolveHandle.

func (b *cachedBackend) Allocate(ctx context.Context, fh backend.FileHandle, off, size uint64, mode uint32) proto.FsError {
	st := b.Inner.Allocate(ctx, unwrapHandle(fh), off, size, mode)
	if st != proto.FsError_FS_OK {
		return st
	}
	if ch, ok := fh.(*cachedHandle); ok {
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

// SetLk/SetLkw: see GetLk — not overridden, forwarded via PassthroughBackend.

// CopyFileRange passes through, then invalidates the DESTINATION like a
// Write of the copied range: the data cache for [offOut, offOut+n) and
// the attr entry (size/mtime moved). Source is untouched (atime only).
func (b *cachedBackend) CopyFileRange(ctx context.Context, fhIn backend.FileHandle, offIn uint64, fhOut backend.FileHandle, offOut uint64, length, flags uint64) (uint64, proto.FsError) {
	n, st := b.Inner.CopyFileRange(ctx, unwrapHandle(fhIn), offIn, unwrapHandle(fhOut), offOut, length, flags)
	if st != proto.FsError_FS_OK {
		return n, st
	}
	if ch, ok := fhOut.(*cachedHandle); ok && n > 0 {
		b.data.invalidateRange(ch.path, int64(offOut), int64(n))
		b.attr.invalidate(ch.path)
	}
	return n, proto.FsError_FS_OK
}

// Lseek is NOT overridden — hole geometry isn't cached, so it forwards to Inner
// via the embedded PassthroughBackend (wrapped handle reaches the leaf via the
// Unwrap-walking resolveHandle).

// --- Path-level mutating ops ---

// Mkdir invalidates the parent (new dirent bumps its mtime) and primes the
// attr cache from the reply attrs (like Create); nil attrs (server stat
// failed) just drop any negative-cached entry so the next Stat refetches.
func (b *cachedBackend) Mkdir(ctx context.Context, p string, mode uint32) (*backend.Attr, proto.FsError) {
	a, st := b.Inner.Mkdir(ctx, p, mode)
	if st != proto.FsError_FS_OK {
		return nil, st
	}
	parent := pathParent(p)
	b.dir.invalidate(parent)
	b.attr.invalidate(parent)
	b.attr.invalidate(p) // drop any negative-cached entry for the just-created path
	b.access.invalidate(p)
	if a != nil {
		b.attr.putPositive(p, a)
	}
	return a, proto.FsError_FS_OK
}

func (b *cachedBackend) Rmdir(ctx context.Context, p string) proto.FsError {
	st := b.Inner.Rmdir(ctx, p)
	if st != proto.FsError_FS_OK {
		return st
	}
	b.attr.invalidate(p)
	b.access.invalidate(p)
	b.getxattr.invalidate(p)
	b.dir.invalidate(p)
	parent := pathParent(p)
	b.dir.invalidate(parent)
	b.attr.invalidate(parent) // Rmdir changes the parent's mtime
	b.attr.putNegative(p)
	return proto.FsError_FS_OK
}

func (b *cachedBackend) Unlink(ctx context.Context, p string) proto.FsError {
	st := b.Inner.Unlink(ctx, p)
	if st != proto.FsError_FS_OK {
		return st
	}
	b.attr.invalidate(p)
	b.access.invalidate(p)
	b.getxattr.invalidate(p)
	b.data.invalidatePath(p)
	parent := pathParent(p)
	b.dir.invalidate(parent)
	b.attr.invalidate(parent) // Unlink changes the parent's mtime
	b.attr.putNegative(p)
	return proto.FsError_FS_OK
}

func (b *cachedBackend) Rename(ctx context.Context, oldPath, newPath string) proto.FsError {
	st := b.Inner.Rename(ctx, oldPath, newPath)
	if st != proto.FsError_FS_OK {
		return st
	}
	// #159: a rename moves a whole subtree in one op. Invalidate the full
	// subtree under BOTH old and new paths across every cache (memory + disk),
	// not just the two path keys — otherwise descendants like oldPath/.git
	// survive and resurface as phantom "already exists" when a directory of the
	// same name is recreated. Cheap O(n) here is fine: renames are rare relative
	// to the per-entry ops (unlink/create) that must stay O(1).
	b.invalidateSubtree(oldPath)
	b.invalidateSubtree(newPath)
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
func (b *cachedBackend) SetAttr(ctx context.Context, p string, in backend.SetAttrIn) (*backend.Attr, proto.FsError) {
	a, st := b.Inner.SetAttr(ctx, p, in)
	// chmod/chown change the access decision; invalidate even on partial failure
	// (the server applies size→mode→owner→times in order before stopping).
	b.access.invalidate(p)
	// A requested size change makes every cached chunk suspect on success
	// AND on failure: size applies first server-side, so even a failed call
	// may already have truncated the file.
	if in.Valid&backend.FATTR_SIZE != 0 {
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

// unwrapHandle returns the inner backend.FileHandle if fh is a *cachedHandle,
// otherwise fh itself. Pass-through file ops (Read, Write, Release, ...)
// use this so the gRPC backend's resolveHandle reaches the leaf
// *grpcFileHandle.
func unwrapHandle(fh backend.FileHandle) backend.FileHandle {
	if ch, ok := fh.(*cachedHandle); ok {
		return ch.inner
	}
	return fh
}

// joinPath joins parent and name into the cache key. It MUST mirror the io
// layer's join byte-for-byte (pkg/client/backend/backend_grpc.go joinPath: raw
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
