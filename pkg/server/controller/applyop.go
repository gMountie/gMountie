package controller

// applyop.go — shared fs-op + emit dispatch called by both unary handlers and
// the Apply streaming RPC (Task 5). This file contains only the inner closure
// body: the fs call + the appropriate emit helper. The surrounding machinery
// (resolveSession, BindIdentity, withIdempotency, arbitrateContention, grant
// stamping) stays in the unary handlers untouched; Apply provides its own
// equivalents for those.
//
// Design notes:
//
// The 8 path-mutation handlers on RpcServerImpl split into three natural groups
// that differ only in which emit helper they use:
//
//   createStyle  (Mkdir, Symlink): fs call → statAttrsAndEmit → (OK, *proto.Attr)
//   deleteStyle  (Rmdir, Unlink):  deleteEmit(fs call) → (status, nil)
//   mutateStyle  (SetXAttr, RemoveXAttr): mutateEmit(fs call) → (status, nil)
//   renameStyle  (Rename):         renameEmit(fs call, oldName, newName) → (status, nil)
//   setAttrStyle (SetAttr):        applySetAttr + conditional mutated-guarded emit
//
// PathOp is a typed descriptor that covers groups 1–4 and is parametric enough
// for Apply to reconstruct from a WalOp oneof without carrying closures.
// SetAttr is handled by applySetAttrOp (separate function) because its emit
// path carries a mutated guard and a partial-failure contract that does not fit
// the common shape.

import (
	"context"
	"syscall"

	fserr "go.gmountie.dev/gmountie/pkg/common/fserr"
	"go.gmountie.dev/gmountie/pkg/proto"
	"go.gmountie.dev/gmountie/pkg/server/service"

	"github.com/hanwen/go-fuse/v2/fuse"
	"github.com/hanwen/go-fuse/v2/fuse/pathfs"
)

// pathOpKind identifies which filesystem operation PathOp describes.
type pathOpKind uint8

const (
	opMkdir       pathOpKind = iota + 1
	opRmdir
	opUnlink
	opSymlink
	opRename
	opSetXAttr
	opRemoveXAttr
)

// PathOp is a typed descriptor of a path-based mutating filesystem operation
// whose emit pattern is uniform enough to be dispatched by applyPathOp.
// It carries all per-kind arguments in-line; unused fields are zero/nil.
//
// Mapping to proto messages (for Apply reconstruction from WalOp):
//
//	MkdirRequest     → Kind=opMkdir,       Path=path,     Mode=mode
//	RmdirRequest     → Kind=opRmdir,       Path=path
//	UnlinkRequest    → Kind=opUnlink,      Path=path
//	SymlinkRequest   → Kind=opSymlink,     Path=link_path, Target=target
//	RenameRequest    → Kind=opRename,      OldName=old_name, NewName=new_name
//	SetXAttrRequest  → Kind=opSetXAttr,    Path=path, XAttr=attribute, XData=data, XFlags=flags
//	RemoveXAttrRequest → Kind=opRemoveXAttr, Path=path, XAttr=attribute
type PathOp struct {
	Kind pathOpKind

	// Rename uses OldName/NewName; all others use Path.
	Path    string
	OldName string // rename only
	NewName string // rename only

	Mode   uint32 // Mkdir
	Target string // Symlink: link target

	XAttr  string // SetXAttr, RemoveXAttr: attribute name
	XData  []byte // SetXAttr: data
	XFlags int    // SetXAttr: flags (XATTR_CREATE / XATTR_REPLACE)

	// Caller is forwarded to createContext / emit helpers.
	Caller *proto.Caller
}

// PathOpResult is the outcome of applyPathOp.
// Attr is non-nil only for create-style ops (Mkdir, Symlink) on success.
type PathOpResult struct {
	Status fuse.Status
	Attr   *proto.Attr
}

// applyPathOp executes the fs-op + emit portion of a path-mutation handler.
// It is called AFTER arbitrateContention and BEFORE grant stamping.
//
// Preconditions (caller's responsibility):
//   - fs is already identity-bound via VolumeService.BindIdentity.
//   - id is the resolved identity from the same BindIdentity call.
//   - ctx carries the session identity (for sessionIDFromContext in emit helpers).
//
// This function does NOT call resolveSession, BindIdentity, withIdempotency,
// arbitrateContention, or grantFor — those stay in the unary handlers (and
// Apply's own loop). This is the pure "do the mutation + fire the bus event"
// layer.
func (r *RpcServerImpl) applyPathOp(
	ctx context.Context,
	fs pathfs.FileSystem,
	id *service.Identity,
	volume string,
	op PathOp,
) PathOpResult {
	fctx := createContext(ctx, op.Caller)

	switch op.Kind {
	case opMkdir:
		if s := fs.Mkdir(op.Path, op.Mode, fctx); s != fuse.OK {
			return PathOpResult{Status: s}
		}
		return PathOpResult{
			Status: fuse.OK,
			Attr:   r.statAttrsAndEmit(ctx, fs, volume, op.Path, id, fctx),
		}

	case opSymlink:
		if s := fs.Symlink(op.Target, op.Path, fctx); s != fuse.OK {
			return PathOpResult{Status: s}
		}
		return PathOpResult{
			Status: fuse.OK,
			Attr:   r.statAttrsAndEmit(ctx, fs, volume, op.Path, id, fctx),
		}

	case opRmdir:
		s := r.deleteEmit(ctx, volume, op.Path, func() fuse.Status {
			return fs.Rmdir(op.Path, fctx)
		})
		return PathOpResult{Status: s}

	case opUnlink:
		s := r.deleteEmit(ctx, volume, op.Path, func() fuse.Status {
			return fs.Unlink(op.Path, fctx)
		})
		return PathOpResult{Status: s}

	case opRename:
		s := r.renameEmit(ctx, fs, volume, op.OldName, op.NewName, op.Caller, func() fuse.Status {
			return fs.Rename(op.OldName, op.NewName, fctx)
		})
		return PathOpResult{Status: s}

	case opSetXAttr:
		s := r.mutateEmit(ctx, fs, volume, op.Path, op.Caller, func() fuse.Status {
			return fs.SetXAttr(op.Path, op.XAttr, op.XData, op.XFlags, fctx)
		})
		return PathOpResult{Status: s}

	case opRemoveXAttr:
		s := r.mutateEmit(ctx, fs, volume, op.Path, op.Caller, func() fuse.Status {
			return fs.RemoveXAttr(op.Path, op.XAttr, fctx)
		})
		return PathOpResult{Status: s}

	default:
		// Unknown op kind: treat as EIO (programming error, not a protocol error).
		return PathOpResult{Status: fuse.EIO}
	}
}

// SetAttrOpResult carries the result of applySetAttrOp.
// Attr is non-nil only when the op succeeded and the trailing stat also succeeded.
type SetAttrOpResult struct {
	Status fuse.Status
	Attr   *proto.Attr
}

// applySetAttrOp applies the fs-attribute changes described by request, runs
// the correct emit path (preserving the mutated guard and partial-failure
// contract of the SetAttr handler), and returns the final attrs.
//
// Like applyPathOp, this function operates AFTER arbitrateContention and does
// NOT touch session/idempotency/grant machinery.
//
// The exact emit contract:
//   - partial failure (s != OK, mutated == true): emitMutatedAttr(nil) to push
//     a version-0 invalidation even though we have no fresh attr.
//   - success (s == OK): stat once; on stat-ok emit with the real attr and fill
//     reply.Attributes; on stat-fail emit version-0 (clients revalidate via
//     GetAttrIfChanged) and leave reply.Attributes nil.
//   - no-op (mutated == false, any s): no emit at all.
func (r *RpcServerImpl) applySetAttrOp(
	ctx context.Context,
	fs pathfs.FileSystem,
	id *service.Identity,
	volume string,
	request *proto.SetAttrRequest,
) SetAttrOpResult {
	fctx := createContext(ctx, request.Caller)
	s, mutated := applySetAttr(fs, request, fctx)
	if s != fuse.OK {
		if mutated {
			r.emitMutatedAttr(ctx, volume, request.Path, nil)
		}
		return SetAttrOpResult{Status: s}
	}
	res := SetAttrOpResult{Status: fuse.OK}
	if attr, gst := fs.GetAttr(request.Path, fctx); gst.Ok() {
		res.Attr = toProtoAttr(attr, id)
		if mutated {
			r.emitMutatedAttr(ctx, volume, request.Path, attr)
		}
	} else if mutated {
		r.emitMutatedAttr(ctx, volume, request.Path, nil)
	}
	return res
}

// applyOpProtoStatus converts a fuse.Status from applyPathOp / applySetAttrOp
// to the proto wire enum. Provided as a convenience so callers do not need to
// import syscall directly.
func applyOpProtoStatus(s fuse.Status) proto.FsError {
	return fserr.FromErrno(syscall.Errno(s))
}
