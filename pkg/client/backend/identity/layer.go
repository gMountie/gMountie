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
// The layer EMBEDS backend.PassthroughBackend and overrides only the ops that carry
// identity-bearing payload: the inbound attr methods (Stat, GetAttrIfChanged,
// Lookup, Create, Mkdir, Symlink, ListDir) and the outbound SetAttr. Every
// other op forwards unchanged via the embedded passthrough. The safety net for
// a future interface method is the central backend.TestFileSystemBackendMethodSet
// guard, which fails when the method set changes and forces a review of every
// embedding layer (cache + identity) — replacing the old per-layer full-surface
// implementation that this layer used to carry.
package identity

import (
	"context"

	"go.gmountie.dev/gmountie/pkg/client/backend"
	"go.gmountie.dev/gmountie/pkg/proto"
)

// setAttrIDMask is the set of SetAttrIn.Valid bits that carry an ownership
// change. It mirrors the (uidOK || gidOK) gate in node.go's Setattr: if EITHER
// the uid or the gid valid bit is set, both Uid and Gid are run through
// Outbound. Rewriting a half whose bit is unset is harmless — that half never
// reaches the wire because the inner SetAttr ignores value fields whose bit is
// clear.
const setAttrIDMask = backend.FATTR_UID | backend.FATTR_GID

// layer is the identity-rewrite decorator over an inner FileSystemBackend. It
// embeds backend.PassthroughBackend (which holds the Inner backend and forwards every
// op) and overrides only the identity-bearing ops below.
type layer struct {
	backend.PassthroughBackend
	rw *backend.IDRewriter
}

// NewLayer wraps inner so attrs flowing up have their uid/gid rewritten
// server→local (Inbound) and SetAttr ownership changes flowing down are
// rewritten local→server (Outbound).
//
// When rw is nil the rewrite is the identity transform, so NewLayer returns
// inner unchanged — no decorator is interposed (raw_ids mounts, or when WhoAmI
// returned no identity). backend.IDRewriter is itself nil-safe, but returning inner
// avoids a redundant pass-through layer entirely.
func NewLayer(inner backend.FileSystemBackend, rw *backend.IDRewriter) backend.FileSystemBackend {
	if rw == nil {
		return inner
	}
	return &layer{PassthroughBackend: backend.PassthroughBackend{Inner: inner}, rw: rw}
}

// inbound returns a COPY of a with its uid/gid rewritten server→local. It must
// NOT mutate a in place: a is owned by the inner backend, and a cache layer
// below us returns (and stores) the very pointer it holds — mutating it would
// corrupt the cache entry AND double-rewrite it on the next hit (a 500 that no
// longer matches the server uid would then map to nobody). This mirrors the old
// adapters, which copied fields into a fresh fuse.Attr / Stat_t and rewrote that
// copy, never the backend's attr. nil-safe: a nil attr (server-omitted reply or
// the not-modified GetAttrIfChanged case) returns nil unchanged.
func (l *layer) inbound(a *backend.Attr) *backend.Attr {
	if a == nil {
		return nil
	}
	cp := *a
	cp.Uid, cp.Gid = l.rw.Inbound(cp.Uid, cp.Gid)
	return &cp
}

// --- Inbound (rewrite attrs flowing up) ---

func (l *layer) Stat(ctx context.Context, path string) (*backend.Attr, proto.FsError) {
	a, st := l.Inner.Stat(ctx, path)
	return l.inbound(a), st
}

func (l *layer) GetAttrIfChanged(ctx context.Context, path string, knownVersion uint64) (*backend.Attr, bool, proto.FsError) {
	a, notModified, st := l.Inner.GetAttrIfChanged(ctx, path, knownVersion)
	// a is nil on the not-modified path and on errors; inbound returns nil.
	return l.inbound(a), notModified, st
}

func (l *layer) Lookup(ctx context.Context, parent, name string) (*backend.Attr, proto.FsError) {
	a, st := l.Inner.Lookup(ctx, parent, name)
	return l.inbound(a), st
}

func (l *layer) ListDir(ctx context.Context, path string) ([]backend.DirEntryPlus, proto.FsError) {
	entries, st := l.Inner.ListDir(ctx, path)
	if st != proto.FsError_FS_OK {
		return entries, st
	}
	// Return a fresh slice whose per-entry Attr is a rewritten COPY: a cache
	// decorator below us stored (and may still hold) the entry attr pointers,
	// so we must not mutate them in place (that would corrupt the cache and
	// double-rewrite). The DirEntry value part is copied by the struct copy;
	// only Attr is replaced. Entries without a plus attr (Attr nil) are left
	// nil. Surfaced attrs carry local ids — upward coherence with Stat/Lookup.
	out := make([]backend.DirEntryPlus, len(entries))
	for i := range entries {
		out[i] = entries[i]
		out[i].Attr = l.inbound(entries[i].Attr)
	}
	return out, st
}

func (l *layer) Create(ctx context.Context, parent, name string, flags, mode uint32) (backend.FileHandle, *backend.Attr, proto.FsError) {
	fh, a, st := l.Inner.Create(ctx, parent, name, flags, mode)
	return fh, l.inbound(a), st
}

func (l *layer) Mkdir(ctx context.Context, path string, mode uint32) (*backend.Attr, proto.FsError) {
	a, st := l.Inner.Mkdir(ctx, path, mode)
	return l.inbound(a), st
}

func (l *layer) Symlink(ctx context.Context, target, linkPath string) (*backend.Attr, proto.FsError) {
	a, st := l.Inner.Symlink(ctx, target, linkPath)
	return l.inbound(a), st
}

// --- Outbound (rewrite the SetAttr request flowing down) ---

func (l *layer) SetAttr(ctx context.Context, path string, in backend.SetAttrIn) (*backend.Attr, proto.FsError) {
	if in.Valid&setAttrIDMask != 0 {
		// Mirror node.go's gidOK semantics: when EITHER ownership bit is set,
		// run both Uid and Gid through Outbound. The half whose bit is clear is
		// ignored downstream, so an unconditional rewrite of both is harmless.
		in.Uid, in.Gid = l.rw.Outbound(in.Uid, in.Gid)
	}
	a, st := l.Inner.SetAttr(ctx, path, in)
	// The reply carries the resulting attrs; surface a COPY rewritten back
	// server→local so the caller sees local display ids (matches node.go
	// applying Inbound to the SetAttr reply via setAttrFromBackend). Copy, not
	// in-place, for the same cache-aliasing reason as the other inbound methods.
	return l.inbound(a), st
}

// All other ops (Access, StatFs, xattr, Open/Read/Write/Release/Flush/Fsync/
// Allocate, locks, CopyFileRange/Lseek, Rmdir/Unlink/Rename, Readlink, Close)
// carry no identity-bearing payload and forward unchanged via the embedded
// backend.PassthroughBackend.

// Compile-time assertion: the layer must satisfy the full backend surface
// (here via promotion from the embedded PassthroughBackend + the overrides
// above). A new interface method is caught by backend.TestFileSystemBackendMethodSet.
var _ backend.FileSystemBackend = (*layer)(nil)
