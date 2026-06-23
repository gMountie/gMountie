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
// For delegated paths, mutating ops (Create/Mkdir/Unlink/Rmdir/Rename/
// SetAttr/Symlink/SetXAttr/RemoveXAttr) record a wal.Op via coord.RecordOp
// and return optimistic success without forwarding to inner.
//
// Write (byte-level) is intentionally NOT intercepted here: delegated
// byte-writes flow through the transport layer's walHandle → Coordinator.Drain
// (Task 10a). Intercepting Write here would double-record bytes in the overlay
// (Drain already applies them). Open/Flush/Fsync/Release all passthrough so
// the transport handle lifecycle is unaffected.
//
// # Cross-subtree rename
//
// A rename where either old or new is NOT delegated is forced synchronous
// (passthrough to inner). Only when both paths are delegated is the rename
// deferred via the WAL.
//
// # Base-delta Stat merge (the keystone)
//
// overlay.Stat can return a "base-delta" node — a node that records mutations
// on a path that existed only in base (not created by this overlay). In that
// case the overlay attr carries only the touched fields (FATTR_* bitmask in
// valid); the full attr is formed by:
//   1. Fetching base from inner.
//   2. OR-ing the base's S_IFMT type bits into the overlay mode.
//   3. Applying only the fields whose FATTR_* bit is set in valid.
//   4. Size: if FATTR_SIZE is set → use overlay.Size; else → max(base, overlay).
//
// This merge is implemented in mergedStat to be reused by Stat, Lookup,
// GetAttrIfChanged, and SetAttr.
package wal

import (
	"context"
	"syscall"

	"go.gmountie.dev/gmountie/pkg/client/backend"
	"go.gmountie.dev/gmountie/pkg/client/backend/delegation"
	"go.gmountie.dev/gmountie/pkg/proto"
)

// syntheticHandle is a minimal FileHandle for overlay-only files (e.g., a
// delegated Create that never reached inner). The handle keeps the path and
// wraps itself (leaf).
type syntheticHandle struct{ path string }

func (h *syntheticHandle) Path() string              { return h.path }
func (h *syntheticHandle) Unwrap() backend.FileHandle { return h }

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
	l.PassthroughBackend.Inner = inner
	return l
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
func (l *Layer) Read(ctx context.Context, fh backend.FileHandle, off int64, dest []byte) (int, proto.FsError) {
	path := fh.Path()
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

// Create records a pending create in the WAL for delegated parents and returns
// a synthetic handle + the overlay's attr. Inner is NOT called for delegated
// creates (the create is deferred until delegation is recalled and the WAL is
// replayed to the server).
//
// NOTE: the returned syntheticHandle is NOT read/write-capable through inner
// (inner cannot resolve it). Read/Write on a freshly-created delegated file
// requires Task 14's handle-seam wiring (transport-backed handle for the
// overlay-created path). Until then, Stat visibility works; data I/O does not.
func (l *Layer) Create(ctx context.Context, parent, name string, flags, mode uint32) (backend.FileHandle, *backend.Attr, proto.FsError) {
	path := joinPath(parent, name)
	if l.mgr.IsDelegated(path) {
		op := Op{Kind: OpCreate, Path: path, Mode: mode, Flags: flags}
		if err := l.coord.RecordOp(op); err != nil {
			return nil, nil, proto.FsError_FS_EIO
		}
		attr, ok, _, _, _ := l.coord.Stat(path)
		if !ok {
			return nil, nil, proto.FsError_FS_EIO
		}
		return &syntheticHandle{path: path}, attr, proto.FsError_FS_OK
	}
	return l.Inner.Create(ctx, parent, name, flags, mode)
}

// Mkdir records a pending mkdir in the WAL for delegated paths.
func (l *Layer) Mkdir(ctx context.Context, path string, mode uint32) (*backend.Attr, proto.FsError) {
	if l.mgr.IsDelegated(path) {
		op := Op{Kind: OpMkdir, Path: path, Mode: mode}
		if err := l.coord.RecordOp(op); err != nil {
			return nil, proto.FsError_FS_EIO
		}
		attr, ok, _, _, _ := l.coord.Stat(path)
		if !ok {
			return nil, proto.FsError_FS_EIO
		}
		return attr, proto.FsError_FS_OK
	}
	return l.Inner.Mkdir(ctx, path, mode)
}

// Unlink records a pending delete for delegated paths.
func (l *Layer) Unlink(ctx context.Context, path string) proto.FsError {
	if l.mgr.IsDelegated(path) {
		op := Op{Kind: OpUnlink, Path: path}
		if err := l.coord.RecordOp(op); err != nil {
			return proto.FsError_FS_EIO
		}
		return proto.FsError_FS_OK
	}
	return l.Inner.Unlink(ctx, path)
}

// Rmdir records a pending rmdir for delegated paths.
func (l *Layer) Rmdir(ctx context.Context, path string) proto.FsError {
	if l.mgr.IsDelegated(path) {
		op := Op{Kind: OpRmdir, Path: path}
		if err := l.coord.RecordOp(op); err != nil {
			return proto.FsError_FS_EIO
		}
		return proto.FsError_FS_OK
	}
	return l.Inner.Rmdir(ctx, path)
}

// Rename defers the rename via the WAL only when both the old and new paths
// are delegated. A cross-subtree rename (either path not delegated) is forced
// synchronous (passthrough to inner) so that non-delegated side-effects
// (directory additions, cache invalidation) happen promptly.
func (l *Layer) Rename(ctx context.Context, oldPath, newPath string) proto.FsError {
	if l.mgr.IsDelegated(oldPath) && l.mgr.IsDelegated(newPath) {
		op := Op{Kind: OpRename, Path: oldPath, NewPath: newPath}
		if err := l.coord.RecordOp(op); err != nil {
			return proto.FsError_FS_EIO
		}
		return proto.FsError_FS_OK
	}
	return l.Inner.Rename(ctx, oldPath, newPath)
}

// SetAttr records a pending setattr for delegated paths and returns merged attrs.
func (l *Layer) SetAttr(ctx context.Context, path string, in backend.SetAttrIn) (*backend.Attr, proto.FsError) {
	if l.mgr.IsDelegated(path) {
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
		if err := l.coord.RecordOp(op); err != nil {
			return nil, proto.FsError_FS_EIO
		}
		return l.mergedStat(ctx, path)
	}
	return l.Inner.SetAttr(ctx, path, in)
}

// Symlink records a pending symlink create for delegated paths.
func (l *Layer) Symlink(ctx context.Context, target, linkPath string) (*backend.Attr, proto.FsError) {
	if l.mgr.IsDelegated(linkPath) {
		op := Op{Kind: OpSymlink, Path: linkPath, Data: []byte(target)}
		if err := l.coord.RecordOp(op); err != nil {
			return nil, proto.FsError_FS_EIO
		}
		attr, ok, _, _, _ := l.coord.Stat(linkPath)
		if !ok {
			return nil, proto.FsError_FS_EIO
		}
		return attr, proto.FsError_FS_OK
	}
	return l.Inner.Symlink(ctx, target, linkPath)
}

// SetXAttr records a pending xattr set for delegated paths.
func (l *Layer) SetXAttr(ctx context.Context, path, attr string, data []byte, flags uint32) proto.FsError {
	if l.mgr.IsDelegated(path) {
		op := Op{
			Kind:       OpSetXAttr,
			Path:       path,
			XattrName:  attr,
			XattrValue: data,
			XattrFlags: flags,
		}
		if err := l.coord.RecordOp(op); err != nil {
			return proto.FsError_FS_EIO
		}
		return proto.FsError_FS_OK
	}
	return l.Inner.SetXAttr(ctx, path, attr, data, flags)
}

// RemoveXAttr records a pending xattr removal for delegated paths.
func (l *Layer) RemoveXAttr(ctx context.Context, path, attr string) proto.FsError {
	if l.mgr.IsDelegated(path) {
		op := Op{Kind: OpRemoveXAttr, Path: path, XattrName: attr}
		if err := l.coord.RecordOp(op); err != nil {
			return proto.FsError_FS_EIO
		}
		return proto.FsError_FS_OK
	}
	return l.Inner.RemoveXAttr(ctx, path, attr)
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

// ── path helpers ──────────────────────────────────────────────────────────────

// joinPath mirrors memfs / cache conventions: root parent "" means no prefix.
func joinPath(parent, name string) string {
	if parent == "" {
		return name
	}
	return parent + "/" + name
}

