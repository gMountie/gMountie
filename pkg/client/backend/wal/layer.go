// Package wal — layer.go: WAL backend layer (Task 10b).
//
// NewLayer wraps an inner FileSystemBackend with the WAL read-your-own-writes
// seam. It sits OUTER of the cache and INNER of the observer in the compose
// stack (posWAL in mount/compose.go).
//
// # Read routing
//
// For a path that is both delegated AND has pending state in the overlay, the
// layer serves reads from the merged view (base ⊕ pending):
//   - Stat/Lookup/GetAttrIfChanged:  mergedStat helper (base-delta merge).
//   - ListDir:                        coord.ListMerge over inner.ListDir base.
//   - Read (via fh):                  coord.ReadMerge over inner.Read base.
//   - GetXAttr:                        coord.Xattr → pending set/removal, else inner.
//
// All other reads (non-delegated or no pending state) fall through to inner.
//
// # Write routing
//
// Mutating ops (Create/Mkdir/Unlink/Rmdir/Rename/SetAttr/Symlink/SetXAttr/
// RemoveXAttr) record a wal.Op via coord.RecordOp (through the recordDeferred
// helper for everything except Rename) and return optimistic success without
// forwarding to inner. Admission is owned by the Coordinator: RecordOp refuses
// with ErrNotDelegated when the path (or NewPath) is not write-delegated,
// checked atomically with the append. recordDeferred handles the recall race
// — it resolves every refusal from ONE atomic mgr.AdmissionState snapshot:
// draining → park in mgr.WaitDrained and re-decide; delegated (a drain ended
// grant-RETAINED, e.g. a failed recall flush) → retry, which re-admits; both
// false → the grant is gone (completed handoff) and the caller's synchronous
// inner fallback is safe.
//
// Write (byte-level) dispatches on handle type (Task 14b):
//   - syntheticHandle, still covered by its delegation: Write is recorded via
//     recordDeferred(OpWrite) into the overlay.
//   - syntheticHandle, orphaned (recordDeferred refused with ErrNotDelegated —
//     its grant was recalled while the handle stayed open): writeThrough opens
//     a transient inner handle and writes directly, since there is nowhere
//     left to defer to.
//   - All other handles: the write flows through the transport layer's walHandle
//     → Coordinator.Drain (Task 10a). Writing here for non-synthetic handles
//     would double-record bytes in the overlay, so we pass through to inner.
//
// Read (byte-level) mirrors this for the synthetic branch: a syntheticHandle
// whose overlay node no longer fully owns the file (flushed, tombstoned, or
// base-delta) reads through a transient inner handle (readThrough) merged with
// any still-pending overlay bytes, instead of merging over a nil base (which
// would silently serve empty data after a flush cleared the overlay).
//
// Open: for overlay-created paths (Has && !baseDelta) under delegation, Open
// returns a syntheticHandle instead of calling inner (inner would return ENOENT).
// For base-delta paths (pre-existing file with pending mutations) Open falls
// through to inner so the caller gets a transport-backed handle for base reads.
//
// Flush/Fsync/Release on a syntheticHandle: Flush returns FS_OK (the interval
// flusher handles durability); Fsync calls coord.Fsync for a synchronous flush;
// Release is a no-op (no inner state to clean up).
// For all other handles these lifecycle ops passthrough to inner.
//
// # Rename correctness
//
// A rename defers via the WAL only when the source is entirely the overlay's
// own creation (a full-create node, not a tombstone, not a base-delta) — the
// atomic-write `create tmp → rename over` fast path. The ownership decision is
// made by RecordOp under recordMu, atomically with the append (ErrNotOwned
// otherwise), so a concurrent flush cannot clear the node between decision and
// append. Any other source (base-only, or base-delta with pending mutations)
// cannot be re-homed by the overlay at the destination, so it runs
// synchronously against inner — under a BeginDrain admission barrier over both
// endpoints, first flushing any pending state touching either endpoint's
// subtree (coord.HasSubtree), so a deferred op is never left stranded against
// a path the rename has moved away from (up to the path-capture caveat, design
// doc §7.7 gap d).
//
// # Base-delta Stat merge (the keystone)
//
// overlay.Stat can return a "base-delta" node — a node that records mutations
// on a path that existed only in base (not created by this overlay). In that
// case the overlay attr carries only the touched fields (FATTR_* bitmask in
// valid); the full attr is formed by:
//  1. Fetching base from inner.
//  2. OR-ing the base's S_IFMT type bits into the overlay mode.
//  3. Applying only the fields whose FATTR_* bit is set in valid.
//  4. Size: if FATTR_SIZE is set → use overlay.Size; else → max(base, overlay).
//
// This merge is implemented in mergedStat to be reused by Stat, Lookup,
// GetAttrIfChanged, and SetAttr.
package wal

import (
	"context"
	stderrors "errors"
	"syscall"

	"go.gmountie.dev/gmountie/pkg/client/backend"
	"go.gmountie.dev/gmountie/pkg/client/backend/delegation"
	"go.gmountie.dev/gmountie/pkg/proto"
	"go.gmountie.dev/gmountie/pkg/utils/log"

	"go.uber.org/zap"
)

// syntheticHandle is a minimal FileHandle for overlay-only files (e.g., a
// delegated Create that never reached inner). The handle keeps the path and
// wraps itself (leaf).
//
// R/W operations on a syntheticHandle are routed through the coordinator
// (RecordOp for writes, ReadMerge for reads) rather than through inner,
// because inner has no record of the file until the WAL is flushed.
type syntheticHandle struct{ path string }

func (h *syntheticHandle) Path() string               { return h.path }
func (h *syntheticHandle) Unwrap() backend.FileHandle { return h }

// asSynthetic walks the FileHandle Unwrap chain and returns the first
// *syntheticHandle found, or nil if none is present. The walk terminates when
// Unwrap() returns the handle itself (leaf).
func asSynthetic(fh backend.FileHandle) *syntheticHandle {
	if fh == nil {
		return nil
	}
	cur := fh
	for {
		if sh, ok := cur.(*syntheticHandle); ok {
			return sh
		}
		next := cur.Unwrap()
		if next == cur {
			return nil
		}
		cur = next
	}
}

// Layer wraps inner with WAL read-your-own-writes semantics.
type Layer struct {
	backend.PassthroughBackend
	mgr   *delegation.Manager
	coord *Coordinator
}

// NewLayer constructs the WAL backend layer.
//
// inner is the next backend inward (typically the cache or the transport
// itself in tests). mgr is the delegation oracle. coord is the WAL
// Coordinator that holds the durable log and in-memory overlay.
func NewLayer(inner backend.FileSystemBackend, mgr *delegation.Manager, coord *Coordinator) backend.FileSystemBackend {
	l := &Layer{mgr: mgr, coord: coord}
	l.Inner = inner
	// On each flush, invalidate the inner cache for the flushed paths so a
	// post-flush read falls through to the authoritative server instead of a
	// stale cache hit (see Coordinator.onFlushed / cache.InvalidatePath).
	coord.onFlushed = func(sent []Op) {
		for i := range sent {
			l.invalidateInner(sent[i].Path)
			if sent[i].NewPath != "" {
				l.invalidateInner(sent[i].NewPath)
			}
		}
	}
	return l
}

// cacheInvalidator is the optional inner-cache capability to drop a single
// path's cached entries (plus its parent's dir listing) in O(1). The cache
// layer implements it; the transport doesn't — so we type-assert.
type cacheInvalidator interface{ InvalidatePath(path string) }

// invalidateInner drops the inner cache entry for path. Called once per FLUSHED
// op (per-flush, not per recorded op) so the cache stays warm between flushes;
// only just-flushed paths go cold and re-fetch once. A no-op when the inner
// backend has no cache (transport-only, tests).
func (l *Layer) invalidateInner(path string) {
	if inv, ok := l.Inner.(cacheInvalidator); ok {
		inv.InvalidatePath(path)
	}
}

// ── Read ops ──────────────────────────────────────────────────────────────────

// Stat implements the read-your-own-writes view for the delegated subtree.
func (l *Layer) Stat(ctx context.Context, path string) (*backend.Attr, proto.FsError) {
	if l.mgr.IsDelegated(path) && l.coord.Has(path) {
		return l.mergedStat(ctx, path)
	}
	return l.Inner.Stat(ctx, path)
}

// Lookup resolves a child entry, merging overlay state when delegated.
func (l *Layer) Lookup(ctx context.Context, parent, name string) (*backend.Attr, proto.FsError) {
	path := joinPath(parent, name)
	if l.mgr.IsDelegated(path) && l.coord.Has(path) {
		return l.mergedStat(ctx, path)
	}
	return l.Inner.Lookup(ctx, parent, name)
}

// GetAttrIfChanged is forwarded; on a pending delegated path we always return
// the merged attr (never "unchanged") so callers don't serve stale base attrs.
func (l *Layer) GetAttrIfChanged(ctx context.Context, path string, knownVersion uint64) (*backend.Attr, bool, proto.FsError) {
	if l.mgr.IsDelegated(path) && l.coord.Has(path) {
		attr, ferr := l.mergedStat(ctx, path)
		if ferr != proto.FsError_FS_OK {
			return nil, false, ferr
		}
		return attr, false, proto.FsError_FS_OK
	}
	return l.Inner.GetAttrIfChanged(ctx, path, knownVersion)
}

// ListDir overlays pending creates/deletes over the base listing.
func (l *Layer) ListDir(ctx context.Context, path string) ([]backend.DirEntryPlus, proto.FsError) {
	if l.mgr.IsDelegated(path) {
		base, ferr := l.Inner.ListDir(ctx, path)
		if ferr != proto.FsError_FS_OK {
			// If path itself is an overlay-created dir, inner returns ENOENT;
			// treat as empty base.
			if ferr != proto.FsError_FS_ENOENT {
				return nil, ferr
			}
			base = nil
		}
		return l.coord.ListMerge(path, base), proto.FsError_FS_OK
	}
	return l.Inner.ListDir(ctx, path)
}

// Read serves the overlay-merged byte view for delegated files.
//
// Handle-type dispatch:
//   - syntheticHandle whose overlay node still fully owns the file (ok &&
//     !tombstoned && !baseDelta): serve purely from the overlay using an
//     empty base — coord.ReadMerge with nil base returns the bytes recorded
//     via RecordOp/Write. No inner call.
//   - syntheticHandle whose overlay node no longer fully owns the file (!ok,
//     tombstoned, or baseDelta — flushed by an interval/recall flush,
//     deleted, or partially flushed): route through readThrough, a transient
//     inner handle merged with any still-pending overlay bytes.
//   - All other handles (transport-backed, memfs-backed, etc.): read the base
//     bytes from inner first, then merge any pending overlay bytes on top.
func (l *Layer) Read(ctx context.Context, fh backend.FileHandle, off int64, dest []byte) (int, proto.FsError) {
	path := fh.Path()

	if sh := asSynthetic(fh); sh != nil {
		_, ok, tombstoned, baseDelta, _ := l.coord.Stat(sh.path)
		if !ok || tombstoned || baseDelta {
			// The overlay no longer fully owns this file: it was flushed
			// (interval/recall — bytes now live on the server), deleted, or
			// partially flushed (base-delta). Serve via a transient inner
			// handle, merging any still-pending bytes on top.
			return l.readThrough(ctx, sh.path, off, dest)
		}
		merged := l.coord.ReadMerge(sh.path, off, nil)
		copied := copy(dest, merged)
		return copied, proto.FsError_FS_OK
	}

	if l.mgr.IsDelegated(path) && l.coord.Has(path) {
		// Read the base bytes from inner into dest first.
		n, ferr := l.Inner.Read(ctx, fh, off, dest)
		if ferr != proto.FsError_FS_OK && ferr != proto.FsError_FS_ENOENT {
			return 0, ferr
		}
		// Merge pending bytes over the base slice.
		merged := l.coord.ReadMerge(path, off, dest[:n])
		// merged may be longer than dest if pending data extends beyond base.
		copied := copy(dest, merged)
		return copied, proto.FsError_FS_OK
	}
	return l.Inner.Read(ctx, fh, off, dest)
}

// readThrough opens a transient inner handle for path, reads the base range,
// merges any pending overlay bytes, and releases the handle. Used for
// synthetic handles that outlived their overlay node (flushed / recalled).
func (l *Layer) readThrough(ctx context.Context, path string, off int64, dest []byte) (int, proto.FsError) {
	ih, ferr := l.Inner.Open(ctx, path, uint32(syscall.O_RDONLY))
	if ferr != proto.FsError_FS_OK {
		return 0, ferr
	}
	defer func() { _ = l.Inner.Release(ctx, ih) }()
	n, ferr := l.Inner.Read(ctx, ih, off, dest)
	if ferr != proto.FsError_FS_OK && ferr != proto.FsError_FS_ENOENT {
		return 0, ferr
	}
	merged := l.coord.ReadMerge(path, off, dest[:n])
	return copy(dest, merged), proto.FsError_FS_OK
}

// Write records a byte-write op for overlay-only files, or passes through to
// inner for transport-backed handles.
//
// No-double-record invariant: only syntheticHandle writes go through
// coord.RecordOp (via recordDeferred) here. All other writes are routed by the
// transport layer via Coordinator.Drain (which calls RecordOp for delegated
// paths), so we must not intercept them here.
//
// Orphaned synthetic handle: when the covering delegation is gone
// (ErrNotDelegated — a recall handoff completed while the handle was still
// open), recordDeferred's retry is refused again and there is nowhere left to
// defer to; write through a transient inner handle instead.
func (l *Layer) Write(ctx context.Context, fh backend.FileHandle, off int64, data []byte) (uint32, proto.FsError) {
	if sh := asSynthetic(fh); sh != nil {
		op := Op{Kind: OpWrite, Path: sh.path, Offset: off, Data: data}
		switch err := l.recordDeferred(ctx, op); {
		case err == nil:
			return uint32(len(data)), proto.FsError_FS_OK
		case stderrors.Is(err, ErrNotDelegated):
			// Orphaned synthetic handle: its delegation was recalled while
			// open. The recall flush materialised the file on the server;
			// write through a transient inner handle.
			return l.writeThrough(ctx, sh.path, off, data)
		default:
			return 0, proto.FsError_FS_EIO
		}
	}
	return l.Inner.Write(ctx, fh, off, data)
}

// writeThrough writes to path via a transient inner handle (open→write→flush→
// release). Used for orphaned synthetic handles after a recall handoff.
func (l *Layer) writeThrough(ctx context.Context, path string, off int64, data []byte) (uint32, proto.FsError) {
	ih, ferr := l.Inner.Open(ctx, path, uint32(syscall.O_WRONLY))
	if ferr != proto.FsError_FS_OK {
		return 0, ferr
	}
	defer func() { _ = l.Inner.Release(ctx, ih) }()
	n, ferr := l.Inner.Write(ctx, ih, off, data)
	if ferr != proto.FsError_FS_OK {
		return 0, ferr
	}
	if st := l.Inner.Flush(ctx, ih); st != proto.FsError_FS_OK {
		return 0, st
	}
	return n, proto.FsError_FS_OK
}

// Open returns a syntheticHandle for overlay-created files (delegated + Has +
// not baseDelta), since inner has no record of them yet and would return ENOENT.
// For base-delta paths (pre-existing file with pending mutations) and all
// non-delegated paths, Open falls through to inner.
func (l *Layer) Open(ctx context.Context, path string, flags uint32) (backend.FileHandle, proto.FsError) {
	if l.mgr.IsDelegated(path) {
		_, ok, tombstoned, baseDelta, _ := l.coord.Stat(path)
		if ok && !tombstoned && !baseDelta {
			// Full overlay-created node: inner cannot serve it.
			return &syntheticHandle{path: path}, proto.FsError_FS_OK
		}
	}
	return l.Inner.Open(ctx, path, flags)
}

// Flush for a syntheticHandle is a no-op success: the interval flusher and
// explicit Fsync handle durability. For all other handles, flush is delegated
// to inner (transport handles coalesce dirty data there).
func (l *Layer) Flush(ctx context.Context, fh backend.FileHandle) proto.FsError {
	if asSynthetic(fh) != nil {
		return proto.FsError_FS_OK
	}
	return l.Inner.Flush(ctx, fh)
}

// Fsync is the hard durability barrier. For a syntheticHandle everything lives
// in the WAL, so a synchronous WAL flush IS the fsync. For transport-backed
// handles the inner fsync only covers bytes that already reached the server —
// if this path ALSO has pending WAL state (a deferred tail from a previous
// close, a deferred setattr), that state must be flushed too, or fsync returns
// OK while data exists only in the local wal.db (lost on machine death).
func (l *Layer) Fsync(ctx context.Context, fh backend.FileHandle, flags int64) proto.FsError {
	if asSynthetic(fh) != nil {
		return l.coord.Fsync(ctx)
	}
	if l.coord.Has(fh.Path()) {
		if st := l.coord.Fsync(ctx); st != proto.FsError_FS_OK {
			return st
		}
	}
	return l.Inner.Fsync(ctx, fh, flags)
}

// Release for a syntheticHandle is a no-op: there is no inner file descriptor
// or transport handle to close. For all other handles, release is delegated
// to inner.
func (l *Layer) Release(ctx context.Context, fh backend.FileHandle) proto.FsError {
	if asSynthetic(fh) != nil {
		return proto.FsError_FS_OK
	}
	return l.Inner.Release(ctx, fh)
}

// GetXAttr serves pending xattr state, falling through to inner when absent.
func (l *Layer) GetXAttr(ctx context.Context, path, attr string) ([]byte, proto.FsError) {
	if l.mgr.IsDelegated(path) && l.coord.Has(path) {
		val, set, removed := l.coord.Xattr(path, attr)
		if set {
			return val, proto.FsError_FS_OK
		}
		if removed {
			return nil, proto.FsError_FS_ENO_XATTR
		}
		// No pending state for this xattr — fall through to inner.
	}
	return l.Inner.GetXAttr(ctx, path, attr)
}

// ── Write ops ─────────────────────────────────────────────────────────────────

// recordDeferred records op via the Coordinator, handling recall races. Every
// admission refusal is resolved from ONE atomic mgr.AdmissionState snapshot —
// never from separate IsDraining/IsWriteDelegated reads, which a drain ending
// grant-RETAINED (failed recall flush) between them can make look like a safe
// synchronous fallback while the retained deferred ops still sit in the WAL:
//
//   - draining → park in WaitDrained and re-decide — looping, because the
//     retry itself can be refused while ANOTHER (or a re-entered) drain is in
//     flight, and falling back synchronously mid-drain would race the
//     in-flight recall Apply stream (same hazard as Coordinator.Drain).
//   - delegated, non-rename → transient refusal (drain ended grant-retained
//     between the refusal and the snapshot): retry, never fall back.
//   - otherwise → return the refusal; the caller's synchronous fallback is
//     safe (grant gone ⇒ the recall flush SUCCEEDED, or an uncovered rename
//     endpoint handled by Rename's barrier path).
//
// A dead ctx exits while draining, returning the admission refusal: the
// caller's synchronous fallback fails fast on the same ctx anyway, so this
// escape trades the mid-drain guarantee for not spinning on a wait that can
// no longer park.
func (l *Layer) recordDeferred(ctx context.Context, op Op) error {
	for {
		err := l.coord.RecordOp(op)
		if !stderrors.Is(err, ErrNotDelegated) {
			return err // admitted, ErrNotOwned, or append failure
		}
		delegated, draining := l.mgr.AdmissionState(op.Path)
		switch {
		case draining:
			if ctx.Err() != nil {
				return err // dead ctx: sync fallback fails fast on it anyway
			}
			l.mgr.WaitDrained(ctx, op.Path)
		case delegated && op.NewPath == "":
			// Transient: a drain ended grant-RETAINED between the refusal
			// and this snapshot. Non-rename admission depends only on
			// op.Path, so the next attempt admits — retry, never fall back
			// synchronously while the grant (and possibly pending ops) live.
		default:
			// Grant gone (single-snapshot ⇒ recall flush succeeded ⇒ sync is
			// safe), or a rename whose NewPath side is uncovered — the
			// caller's synchronous path handles both (rename via the
			// BeginDrain barrier + HasSubtree flush).
			return err
		}
	}
}

// deferOrForward is the shared status-only body: defer via the WAL, or forward
// to inner when not write-delegated, or EIO on a durable-append failure.
func (l *Layer) deferOrForward(ctx context.Context, op Op, forward func() proto.FsError) proto.FsError {
	switch err := l.recordDeferred(ctx, op); {
	case err == nil:
		return proto.FsError_FS_OK
	case stderrors.Is(err, ErrNotDelegated):
		return forward()
	default:
		return proto.FsError_FS_EIO
	}
}

// Create records a pending create in the WAL for delegated parents and returns
// a synthetic handle + the overlay's attr. Falls back to inner when the path
// is not write-delegated (never was, or a recall just handed it off).
//
// The returned syntheticHandle is R/W-capable via the overlay (Task 14b):
// Write routes through coord.RecordOp; Read serves from coord.ReadMerge with
// an empty base. Open on the same path (e.g. after close+reopen) returns a
// fresh syntheticHandle for the same overlay-created file.
func (l *Layer) Create(ctx context.Context, parent, name string, flags, mode uint32) (backend.FileHandle, *backend.Attr, proto.FsError) {
	path := joinPath(parent, name)
	op := Op{Kind: OpCreate, Path: path, Mode: mode, Flags: flags}
	switch err := l.recordDeferred(ctx, op); {
	case err == nil:
		attr, ok, _, _, _ := l.coord.Stat(path)
		if !ok {
			return nil, nil, proto.FsError_FS_EIO
		}
		return &syntheticHandle{path: path}, attr, proto.FsError_FS_OK
	case stderrors.Is(err, ErrNotDelegated):
		return l.Inner.Create(ctx, parent, name, flags, mode)
	default:
		return nil, nil, proto.FsError_FS_EIO
	}
}

// Mkdir records a pending mkdir in the WAL for delegated paths. Falls back to
// inner when the path is not write-delegated.
func (l *Layer) Mkdir(ctx context.Context, path string, mode uint32) (*backend.Attr, proto.FsError) {
	op := Op{Kind: OpMkdir, Path: path, Mode: mode}
	switch err := l.recordDeferred(ctx, op); {
	case err == nil:
		attr, ok, _, _, _ := l.coord.Stat(path)
		if !ok {
			log.Log.Warn("wal: deferred Mkdir overlay Stat !ok -> EIO", zap.String("path", path))
			return nil, proto.FsError_FS_EIO
		}
		return attr, proto.FsError_FS_OK
	case stderrors.Is(err, ErrNotDelegated):
		return l.Inner.Mkdir(ctx, path, mode)
	default:
		log.Log.Warn("wal: deferred Mkdir RecordOp failed -> EIO", zap.String("path", path))
		return nil, proto.FsError_FS_EIO
	}
}

// Unlink records a pending delete for delegated paths. Falls back to inner
// when the path is not write-delegated.
func (l *Layer) Unlink(ctx context.Context, path string) proto.FsError {
	return l.deferOrForward(ctx, Op{Kind: OpUnlink, Path: path}, func() proto.FsError {
		return l.Inner.Unlink(ctx, path)
	})
}

// Rmdir records a pending rmdir for delegated paths. Falls back to inner when
// the path is not write-delegated.
func (l *Layer) Rmdir(ctx context.Context, path string) proto.FsError {
	return l.deferOrForward(ctx, Op{Kind: OpRmdir, Path: path}, func() proto.FsError {
		return l.Inner.Rmdir(ctx, path)
	})
}

// Rename defers via the WAL only when the source is ENTIRELY the overlay's
// creation (a full-create node — the atomic-write `create tmp → rename over`
// fast path). A base or base-delta source cannot be represented by the overlay
// at the destination (the server holds content the overlay cannot re-home), so
// those renames run synchronously — under an admission barrier, after flushing
// any pending state touching either endpoint, so no deferred op is left
// targeting a path the rename has moved away (a stranded op would ENOENT on
// flush = ordered-halt data loss).
//
// The ownership decision is NOT made here: RecordOp decides it under recordMu,
// atomically with the append (ErrNotOwned otherwise). An unlocked gate here
// could race a concurrent commitFlushed clearing the full-create node between
// the check and the append — applyRename would then tombstone the source and
// synthesize nothing at the destination (both paths ENOENT locally until the
// next flush). recordDeferred passes ErrNotOwned straight through (its loop
// only parks on ErrNotDelegated while a drain is in flight).
func (l *Layer) Rename(ctx context.Context, oldPath, newPath string) proto.FsError {
	op := Op{Kind: OpRename, Path: oldPath, NewPath: newPath}
	switch err := l.recordDeferred(ctx, op); {
	case err == nil:
		return proto.FsError_FS_OK
	case stderrors.Is(err, ErrNotOwned), stderrors.Is(err, ErrNotDelegated):
		// Not deferrable: run synchronously below.
	default:
		return proto.FsError_FS_EIO
	}
	// Admission barrier over BOTH endpoints for the compound flush-then-rename:
	// without it, a racing thread could defer a new op referencing the
	// pre-rename path between coord.Fsync and Inner.Rename — stranded against a
	// path the rename has moved away, so a later flush ENOENTs it (ordered-halt
	// loss). BeginDrain reuses the recall drain machinery: racing deferrals are
	// refused admission, park in WaitDrained, and re-decide after release.
	// Residual path-capture caveat: an op whose path was captured BEFORE the
	// rename (an already-open fd under oldPath) can still be deferred after
	// release, referencing the moved path — see design doc §7.7 gap (d).
	// The barrier is taken even for fully-undelegated renames: concurrent
	// Drain/recordDeferred calls under the endpoints briefly park instead of
	// going straight to the wire — harmless serialization, and release never
	// depends on the parked ops.
	releaseOld := l.mgr.BeginDrain(oldPath)
	defer releaseOld()
	releaseNew := l.mgr.BeginDrain(newPath)
	defer releaseNew()
	if l.coord.HasSubtree(oldPath) || l.coord.HasSubtree(newPath) {
		if st := l.coord.Fsync(ctx); st != proto.FsError_FS_OK {
			return st
		}
	}
	return l.Inner.Rename(ctx, oldPath, newPath)
}

// SetAttr records a pending setattr for delegated paths and returns merged
// attrs. Falls back to inner when the path is not write-delegated.
func (l *Layer) SetAttr(ctx context.Context, path string, in backend.SetAttrIn) (*backend.Attr, proto.FsError) {
	op := Op{
		Kind:  OpSetAttr,
		Path:  path,
		Valid: in.Valid,
		Mode:  in.Mode,
		UID:   in.Uid,
		GID:   in.Gid,
	}
	if in.Valid&backend.FATTR_SIZE != 0 {
		op.Size = in.Size
	}
	if in.Valid&backend.FATTR_ATIME != 0 && in.Atime != nil {
		op.AtimeSec = in.Atime.Unix()
		op.AtimeNsec = uint32(in.Atime.Nanosecond())
	}
	if in.Valid&backend.FATTR_MTIME != 0 && in.Mtime != nil {
		op.MtimeSec = in.Mtime.Unix()
		op.MtimeNsec = uint32(in.Mtime.Nanosecond())
	}
	switch err := l.recordDeferred(ctx, op); {
	case err == nil:
		return l.mergedStat(ctx, path)
	case stderrors.Is(err, ErrNotDelegated):
		return l.Inner.SetAttr(ctx, path, in)
	default:
		return nil, proto.FsError_FS_EIO
	}
}

// Symlink records a pending symlink create for delegated paths. Falls back to
// inner when the path is not write-delegated.
func (l *Layer) Symlink(ctx context.Context, target, linkPath string) (*backend.Attr, proto.FsError) {
	op := Op{Kind: OpSymlink, Path: linkPath, Data: []byte(target)}
	switch err := l.recordDeferred(ctx, op); {
	case err == nil:
		attr, ok, _, _, _ := l.coord.Stat(linkPath)
		if !ok {
			return nil, proto.FsError_FS_EIO
		}
		return attr, proto.FsError_FS_OK
	case stderrors.Is(err, ErrNotDelegated):
		return l.Inner.Symlink(ctx, target, linkPath)
	default:
		return nil, proto.FsError_FS_EIO
	}
}

// SetXAttr records a pending xattr set for delegated paths. Falls back to
// inner when the path is not write-delegated.
func (l *Layer) SetXAttr(ctx context.Context, path, attr string, data []byte, flags uint32) proto.FsError {
	op := Op{
		Kind:       OpSetXAttr,
		Path:       path,
		XattrName:  attr,
		XattrValue: data,
		XattrFlags: flags,
	}
	return l.deferOrForward(ctx, op, func() proto.FsError {
		return l.Inner.SetXAttr(ctx, path, attr, data, flags)
	})
}

// RemoveXAttr records a pending xattr removal for delegated paths. Falls back
// to inner when the path is not write-delegated.
func (l *Layer) RemoveXAttr(ctx context.Context, path, attr string) proto.FsError {
	op := Op{Kind: OpRemoveXAttr, Path: path, XattrName: attr}
	return l.deferOrForward(ctx, op, func() proto.FsError {
		return l.Inner.RemoveXAttr(ctx, path, attr)
	})
}

// ── mergedStat ───────────────────────────────────────────────────────────────

// mergedStat computes the authoritative Attr for a delegated path with pending
// overlay state. It handles both full-create nodes (authoritative) and
// base-delta nodes (require merge with inner).
//
// Base-delta merge rules (baseDelta=true):
//   - Fetch base attrs from inner.Stat; copy into a local value (never mutate
//     inner's returned pointer — it may be shared by the cache above).
//   - OR the base S_IFMT type bits into the overlay mode (the overlay only
//     carries permission bits on a baseDelta SetAttr).
//   - Overlay only FATTR_*-masked fields (the valid bitmask).
//   - Size: if FATTR_SIZE is set → overlay.Size (authoritative truncate/extend);
//     else → max(base.Size, overlay.Size) (write-past-EOF append case).
func (l *Layer) mergedStat(ctx context.Context, path string) (*backend.Attr, proto.FsError) {
	attr, ok, tombstoned, baseDelta, valid := l.coord.Stat(path)
	if !ok {
		// No pending state — fall through to inner.
		return l.Inner.Stat(ctx, path)
	}
	if tombstoned {
		return nil, proto.FsError_FS_ENOENT
	}
	if !baseDelta {
		// Full overlay-created node: attr is authoritative.
		return attr, proto.FsError_FS_OK
	}

	// Base-delta: fetch base from inner, merge.
	baseAttr, ferr := l.Inner.Stat(ctx, path)
	if ferr != proto.FsError_FS_OK {
		// If inner can't stat it either (e.g., newly created path the inner
		// doesn't know about yet), fall back to the overlay attr as-is.
		return attr, proto.FsError_FS_OK
	}

	// Copy base into a local struct so we never mutate the pointer returned
	// by inner (the cache may have returned a shared value).
	merged := *baseAttr

	// OR in the base's S_IFMT type bits. The overlay's mode for a baseDelta
	// SetAttr carries only permission bits (the type bits are 0 there), so
	// we must restore the base type.
	if valid&backend.FATTR_MODE != 0 {
		typeBits := baseAttr.Mode & uint32(syscall.S_IFMT)
		permBits := attr.Mode & 0o7777
		merged.Mode = typeBits | permBits
	}
	if valid&backend.FATTR_UID != 0 {
		merged.Uid = attr.Uid
	}
	if valid&backend.FATTR_GID != 0 {
		merged.Gid = attr.Gid
	}
	if valid&backend.FATTR_SIZE != 0 {
		// Authoritative truncate/extend.
		merged.Size = attr.Size
		merged.Blocks = (attr.Size + 511) / 512
	} else if attr.Size > merged.Size {
		// Write-past-EOF: pending writes extended beyond base EOF.
		merged.Size = attr.Size
		merged.Blocks = (attr.Size + 511) / 512
	}
	if valid&backend.FATTR_ATIME != 0 {
		merged.Atime = attr.Atime
		merged.Atimensec = attr.Atimensec
	}
	if valid&backend.FATTR_MTIME != 0 {
		merged.Mtime = attr.Mtime
		merged.Mtimensec = attr.Mtimensec
	}
	// Always bump ctime and version to the overlay's values (they're updated
	// on every Apply).
	merged.Ctime = attr.Ctime
	merged.Version = attr.Version

	return &merged, proto.FsError_FS_OK
}

// ── lifecycle ─────────────────────────────────────────────────────────────────

// Close stops the interval flusher and closes the WAL log, then closes the
// inner backend. The Overlay and delegation.Manager have their own lifecycles
// and are NOT closed here.
func (l *Layer) Close() error {
	coordErr := l.coord.Close()
	innerErr := l.Inner.Close()
	if coordErr != nil {
		return coordErr
	}
	return innerErr
}

// ── path helpers ──────────────────────────────────────────────────────────────

// joinPath mirrors memfs / cache conventions: root parent "" means no prefix.
func joinPath(parent, name string) string {
	if parent == "" {
		return name
	}
	return parent + "/" + name
}
