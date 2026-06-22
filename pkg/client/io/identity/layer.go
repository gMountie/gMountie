// Package identity provides a FileSystemBackend decorator that performs the
// client's UID/GID namespace rewrite (server identity ↔ local mounting user),
// previously inlined in the FUSE adapters (node.go / cgofs). Hoisting it into
// its own backend layer keeps the adapters as pure type-translation and gives
// the rewrite a single, testable home.
//
// Placement: the layer is the OUTERMOST backend layer (above observer + cache),
// so the FUSE adapters see LOCAL display ids and the cache (plus the Subscribe
// invalidation stream it consumes) keeps storing SERVER ids. Inbound rewrites
// server→local on attrs flowing UP; Outbound rewrites local→server on the
// SetAttr request flowing DOWN — exactly the direction/fields/conditions the
// adapters applied before this refactor.
//
// The layer is a FULL-SURFACE SEMANTIC decorator: it implements every
// FileSystemBackend method explicitly (it does NOT embed io.PassthroughBackend)
// so a future interface method forces an explicit Inbound/Outbound/forward
// decision here rather than silently passing identity-bearing attrs through.
package identity

import (
	"context"

	"go.gmountie.dev/gmountie/pkg/client/io"
	"go.gmountie.dev/gmountie/pkg/proto"

	"github.com/hanwen/go-fuse/v2/fuse"
)

// setAttrIDMask is the set of SetAttrIn.Valid bits that carry an ownership
// change. It mirrors the (uidOK || gidOK) gate in node.go's Setattr: if EITHER
// the uid or the gid valid bit is set, both Uid and Gid are run through
// Outbound. Rewriting a half whose bit is unset is harmless — that half never
// reaches the wire because the inner SetAttr ignores value fields whose bit is
// clear.
const setAttrIDMask = fuse.FATTR_UID | fuse.FATTR_GID

// layer is the identity-rewrite decorator over an inner FileSystemBackend.
type layer struct {
	inner io.FileSystemBackend
	rw    *io.IDRewriter
}

// NewLayer wraps inner so attrs flowing up have their uid/gid rewritten
// server→local (Inbound) and SetAttr ownership changes flowing down are
// rewritten local→server (Outbound).
//
// When rw is nil the rewrite is the identity transform, so NewLayer returns
// inner unchanged — no decorator is interposed (raw_ids mounts, or when WhoAmI
// returned no identity). io.IDRewriter is itself nil-safe, but returning inner
// avoids a redundant pass-through layer entirely.
func NewLayer(inner io.FileSystemBackend, rw *io.IDRewriter) io.FileSystemBackend {
	if rw == nil {
		return inner
	}
	return &layer{inner: inner, rw: rw}
}

// rewriteInbound applies the server→local rewrite to a returned attr in place.
// nil-safe: a nil attr (server-omitted reply, or the not-modified
// GetAttrIfChanged case) is left untouched.
func (l *layer) rewriteInbound(a *io.Attr) {
	if a == nil {
		return
	}
	a.Uid, a.Gid = l.rw.Inbound(a.Uid, a.Gid)
}

// --- Inbound (rewrite attrs flowing up) ---

func (l *layer) Stat(ctx context.Context, path string) (*io.Attr, proto.FsError) {
	a, st := l.inner.Stat(ctx, path)
	l.rewriteInbound(a)
	return a, st
}

func (l *layer) GetAttrIfChanged(ctx context.Context, path string, knownVersion uint64) (*io.Attr, bool, proto.FsError) {
	a, notModified, st := l.inner.GetAttrIfChanged(ctx, path, knownVersion)
	// a is nil on the not-modified path and on errors; rewriteInbound no-ops.
	l.rewriteInbound(a)
	return a, notModified, st
}

func (l *layer) Lookup(ctx context.Context, parent, name string) (*io.Attr, proto.FsError) {
	a, st := l.inner.Lookup(ctx, parent, name)
	l.rewriteInbound(a)
	return a, st
}

func (l *layer) ListDir(ctx context.Context, path string) ([]io.DirEntryPlus, proto.FsError) {
	entries, st := l.inner.ListDir(ctx, path)
	// Rewrite each per-entry attr so a cache decorator BELOW us still stored the
	// server ids (it's inner), while the listing attrs surfaced upward carry
	// local ids — upward coherence with Stat/Lookup. Entries without a plus
	// attr (Attr nil) are left as-is.
	for i := range entries {
		l.rewriteInbound(entries[i].Attr)
	}
	return entries, st
}

func (l *layer) Create(ctx context.Context, parent, name string, flags, mode uint32) (io.FileHandle, *io.Attr, proto.FsError) {
	fh, a, st := l.inner.Create(ctx, parent, name, flags, mode)
	l.rewriteInbound(a)
	return fh, a, st
}

func (l *layer) Mkdir(ctx context.Context, path string, mode uint32) (*io.Attr, proto.FsError) {
	a, st := l.inner.Mkdir(ctx, path, mode)
	l.rewriteInbound(a)
	return a, st
}

func (l *layer) Symlink(ctx context.Context, target, linkPath string) (*io.Attr, proto.FsError) {
	a, st := l.inner.Symlink(ctx, target, linkPath)
	l.rewriteInbound(a)
	return a, st
}

// --- Outbound (rewrite the SetAttr request flowing down) ---

func (l *layer) SetAttr(ctx context.Context, path string, in io.SetAttrIn) (*io.Attr, proto.FsError) {
	if in.Valid&setAttrIDMask != 0 {
		// Mirror node.go's gidOK semantics: when EITHER ownership bit is set,
		// run both Uid and Gid through Outbound. The half whose bit is clear is
		// ignored downstream, so an unconditional rewrite of both is harmless.
		in.Uid, in.Gid = l.rw.Outbound(in.Uid, in.Gid)
	}
	a, st := l.inner.SetAttr(ctx, path, in)
	// The reply carries the resulting attrs; rewrite them back server→local so
	// the caller sees local display ids (matches node.go applying Inbound to
	// the SetAttr reply via setAttrFromBackend).
	l.rewriteInbound(a)
	return a, st
}

// --- Forwarded unchanged (no identity-bearing payload) ---

func (l *layer) Access(ctx context.Context, path string, mode uint32) proto.FsError {
	return l.inner.Access(ctx, path, mode)
}

func (l *layer) StatFs(ctx context.Context, path string) (*io.StatFs, proto.FsError) {
	return l.inner.StatFs(ctx, path)
}

func (l *layer) GetXAttr(ctx context.Context, path, attr string) ([]byte, proto.FsError) {
	return l.inner.GetXAttr(ctx, path, attr)
}

func (l *layer) SetXAttr(ctx context.Context, path, attr string, data []byte, flags uint32) proto.FsError {
	return l.inner.SetXAttr(ctx, path, attr, data, flags)
}

func (l *layer) RemoveXAttr(ctx context.Context, path, attr string) proto.FsError {
	return l.inner.RemoveXAttr(ctx, path, attr)
}

func (l *layer) ListXAttr(ctx context.Context, path string) ([]string, proto.FsError) {
	return l.inner.ListXAttr(ctx, path)
}

func (l *layer) Open(ctx context.Context, path string, flags uint32) (io.FileHandle, proto.FsError) {
	return l.inner.Open(ctx, path, flags)
}

func (l *layer) Read(ctx context.Context, fh io.FileHandle, off int64, dest []byte) (int, proto.FsError) {
	return l.inner.Read(ctx, fh, off, dest)
}

func (l *layer) Write(ctx context.Context, fh io.FileHandle, off int64, data []byte) (uint32, proto.FsError) {
	return l.inner.Write(ctx, fh, off, data)
}

func (l *layer) Release(ctx context.Context, fh io.FileHandle) proto.FsError {
	return l.inner.Release(ctx, fh)
}

func (l *layer) Flush(ctx context.Context, fh io.FileHandle) proto.FsError {
	return l.inner.Flush(ctx, fh)
}

func (l *layer) Fsync(ctx context.Context, fh io.FileHandle, flags int64) proto.FsError {
	return l.inner.Fsync(ctx, fh, flags)
}

func (l *layer) Allocate(ctx context.Context, fh io.FileHandle, off, size uint64, mode uint32) proto.FsError {
	return l.inner.Allocate(ctx, fh, off, size, mode)
}

func (l *layer) GetLk(ctx context.Context, fh io.FileHandle, owner uint64, lk *fuse.FileLock, flags uint32, out *fuse.FileLock) proto.FsError {
	return l.inner.GetLk(ctx, fh, owner, lk, flags, out)
}

func (l *layer) SetLk(ctx context.Context, fh io.FileHandle, owner uint64, lk *fuse.FileLock, flags uint32) proto.FsError {
	return l.inner.SetLk(ctx, fh, owner, lk, flags)
}

func (l *layer) SetLkw(ctx context.Context, fh io.FileHandle, owner uint64, lk *fuse.FileLock, flags uint32) proto.FsError {
	return l.inner.SetLkw(ctx, fh, owner, lk, flags)
}

func (l *layer) CopyFileRange(ctx context.Context, fhIn io.FileHandle, offIn uint64, fhOut io.FileHandle, offOut uint64, length, flags uint64) (uint64, proto.FsError) {
	return l.inner.CopyFileRange(ctx, fhIn, offIn, fhOut, offOut, length, flags)
}

func (l *layer) Lseek(ctx context.Context, fh io.FileHandle, offset uint64, whence uint32) (uint64, proto.FsError) {
	return l.inner.Lseek(ctx, fh, offset, whence)
}

func (l *layer) Rmdir(ctx context.Context, path string) proto.FsError {
	return l.inner.Rmdir(ctx, path)
}

func (l *layer) Unlink(ctx context.Context, path string) proto.FsError {
	return l.inner.Unlink(ctx, path)
}

func (l *layer) Rename(ctx context.Context, oldPath, newPath string) proto.FsError {
	return l.inner.Rename(ctx, oldPath, newPath)
}

func (l *layer) Readlink(ctx context.Context, path string) (string, proto.FsError) {
	return l.inner.Readlink(ctx, path)
}

// Close delegates to the inner backend.
func (l *layer) Close() error { return l.inner.Close() }

// Compile-time assertion: the layer must satisfy the full backend surface. If a
// method is added to FileSystemBackend, this breaks here, forcing a decision.
var _ io.FileSystemBackend = (*layer)(nil)
