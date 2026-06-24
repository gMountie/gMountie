package controller

// apply.go — server-side Apply streaming RPC handler (Task 5).
//
// Apply is a client-streaming RPC: the client sends WalOp messages in seq order
// and half-closes; the server applies them in order and replies with a single
// terminal ApplyAck. It is the corruption-critical core of the WAL flush path.
//
// Invariants (a reviewer checks all of these):
//
//  1. Persist-before-ack: store.Advance(committed) completes (durable) BEFORE
//     stream.SendAndClose. The client drops WAL entries ≤ acked watermark; if
//     the server acked without persisting and then crashed, those entries would
//     be silently lost.
//
//  2. Dedup: an op whose seq ≤ the store watermark loaded at stream start is
//     skipped (no-op). Replaying a non-idempotent op (rename, exclusive create)
//     without dedup = corruption.
//
//  3. Ordered halt: on the first failing op, ops after it are NOT applied. The
//     ack carries the committed prefix + the failing seq; the caller replays
//     from (committed+1) on the next attempt.
//
//  4. Single (identity,volume) key per stream: seq is monotone per
//     (identity,volume) and the ack carries ONE watermark, so a batch must
//     belong to exactly one key. The key is derived from the first received op
//     and cached for the stream's lifetime.
//
//  5. Gen-fencing seam (Task 6): the WalOp.Gen field is loaded here but not
//     checked yet. Task 6 will add the RevokedGens check against store.Get()
//     after the watermark load.
//
// Create, WriteOp, and all path ops are path-based (no client fd): the server
// opens/writes/closes a transient handle internally, using nodefs.File.Flush
// then Release. All three emit cache-invalidation events so subscribers see
// the updated content when deferred writes land (same policy as unary handlers).
//
// ReleaseOp is a no-op marker today: it advances the watermark but has no
// filesystem side effect. The create/write ops already carry all the bytes;
// ReleaseOp merely records the full deferred lifecycle.

import (
	"context"
	"io"
	"syscall"

	"github.com/hanwen/go-fuse/v2/fuse"
	"github.com/hanwen/go-fuse/v2/fuse/pathfs"
	"github.com/pkg/errors"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	fserr "go.gmountie.dev/gmountie/pkg/common/fserr"
	"go.gmountie.dev/gmountie/pkg/proto"
	"go.gmountie.dev/gmountie/pkg/server/service"
	"go.gmountie.dev/gmountie/pkg/server/watermark"
	"go.gmountie.dev/gmountie/pkg/utils/log"

	"go.uber.org/zap"
)

// Apply is the server handler for the Apply client-streaming RPC.
// See the package-level doc for the invariant contract.
func (r *RpcServerImpl) Apply(stream proto.RpcFs_ApplyServer) error {
	ctx := stream.Context()

	// Ownership check: derive session from stream context (same path as Recall).
	sessionID := sessionIDFromContext(ctx)
	if sessionID == "" {
		return status.Error(codes.Unauthenticated, "apply: no session")
	}
	if _, err := resolveSession(ctx, r.sessions, sessionID); err != nil {
		return err
	}

	// Watermark store is required for Apply to persist committed progress.
	if r.watermark == nil {
		return status.Error(codes.Internal, "watermark store not configured")
	}

	// committed tracks the highest seq successfully applied in this stream.
	// It is initialised to the store watermark once the first op is received
	// (and the (identity,volume) key is known). Before the first op it is 0,
	// but we do not call Advance with 0 before we have a key.
	var (
		committed   uint64
		wmKey       watermark.Key
		keyResolved bool
		revokedGens []uint64 // gen-fencing: loaded once from store per stream
		fs          pathfs.FileSystem
		id          service.Identity
	)

	// sendAck is the single exit path — always persists before acking.
	// It covers both the EOF success path and the ordered-halt failure path.
	sendAck := func(failedSeq uint64, fserror proto.FsError) error {
		if keyResolved {
			if advErr := r.watermark.Advance(wmKey, committed); advErr != nil {
				// Advance failed: do not ack — the caller will retry. Return
				// a transport error so the client knows the stream failed.
				return errors.Wrap(advErr, "apply: advance watermark")
			}
		}
		return stream.SendAndClose(&proto.ApplyAck{
			Watermark: committed,
			FailedSeq: failedSeq,
			Fserr:     fserror,
		})
	}

	for {
		op, err := stream.Recv()
		if err != nil {
			if errors.Is(err, io.EOF) {
				// Normal half-close: all ops received. Persist then ack.
				return sendAck(0, proto.FsError_FS_OK)
			}
			// Transport error — no ack.
			return errors.Wrap(err, "apply: recv")
		}

		// On the first op, resolve the (identity,volume) key and load the
		// durable watermark. All subsequent ops in this stream must use the
		// same key (single-watermark ack precludes multi-key batches).
		if !keyResolved {
			vol, caller := walOpVolumeCaller(op)
			if vol == "" {
				return status.Error(codes.InvalidArgument, "apply: first op has no volume")
			}
			var bindErr error
			fs, id, bindErr = r.fsService.BindIdentity(ctx, vol, CallerFromProto(caller))
			if bindErr != nil {
				return bindErr
			}
			// Namespace the dedup watermark + revoked-gen fence by the client's
			// wal-epoch (carried on every WalOp): a fresh wal.db gets its own seq
			// space and is never dedup-skipped against a prior epoch's watermark.
			// All ops in one Apply stream share one epoch (one client wal.db).
			wmKey = watermark.Key{Identity: id.Principal, Volume: vol, Epoch: op.GetWalEpoch()}

			// Load the durable watermark once. committed starts at the
			// persisted value so Advance at EOF is always monotone (a
			// full-replay batch that was already applied acks the last seq
			// again and Advance is a no-op). RevokedGens fences superseded
			// replays: any op tagged with a revoked gen is halted below.
			rec, getErr := r.watermark.Get(wmKey)
			if getErr != nil {
				return errors.Wrap(getErr, "apply: get watermark")
			}
			committed = rec.Watermark
			revokedGens = rec.RevokedGens
			keyResolved = true
		}

		seq := op.Seq

		// Dedup: ops with seq ≤ committed (the durable watermark loaded at
		// stream start) are already applied — skip silently to prevent
		// double-apply of non-idempotent ops (rename, exclusive create).
		//
		// NOTE: "committed" holds the STORE watermark at stream start, not
		// the in-stream high-water mark. We do not re-advance the dedup
		// threshold mid-stream; the in-stream progress is tracked by
		// updating committed on each success below.
		if seq <= committed {
			continue
		}

		// Gen-fencing (Task 6): if this op's delegation gen is in the durable
		// revoked-gens set, the client was superseded (machine-death + handoff)
		// and is replaying a stale WAL segment. Halt the batch at this point:
		// ack the committed prefix and signal the fence with FS_ESTALE so the
		// client discards its WAL from this op onward.
		//
		// Fence is checked AFTER dedup: ops already acked (seq ≤ watermark)
		// are skipped first — a recalling session's acked ops carry the
		// now-revoked gen but must not be fenced (they're already committed).
		//
		// gen == 0 is untagged (pre-fencing client) and is never fenced.
		if opGen := op.Gen; opGen > 0 && isRevokedGen(opGen, revokedGens) {
			return sendAck(seq, proto.FsError_FS_ESTALE)
		}

		// Dispatch the op.
		st := r.applyWalOp(ctx, fs, &id, wmKey.Volume, op)
		if st != fuse.OK {
			kind, path := walOpKindPath(op)
			if r.idempotentSkip(ctx, fs, op, st) {
				// The op's desired end-state already holds: a duplicate mkdir of an
				// existing directory (concurrent extraction racing a shared parent,
				// e.g. npm), a symlink of an existing symlink, or an unlink/rmdir of
				// an already-absent path. SKIP and advance — ordered-halting here
				// would discard this op AND every later op in the batch: the false
				// data loss + ENOENT cascade that broke `npm install`.
				log.Log.Debug("wal apply: idempotent op skipped (desired state already holds)",
					zap.Uint64("seq", seq), zap.String("op", kind), zap.String("path", path),
					zap.String("fserr", fserr.FromErrno(syscall.Errno(st)).String()))
				committed = seq
				continue
			}
			// Ordered halt: persist committed prefix, then ack with failure info.
			// Logged loudly: a halt discards this op AND every op after it in the
			// batch, so a single genuine failure can drop thousands of un-flushed ops.
			log.Log.Warn("wal apply: op failed -> ordered halt (this op + all later ops in the batch are discarded)",
				zap.Uint64("seq", seq), zap.String("op", kind), zap.String("path", path),
				zap.String("fserr", fserr.FromErrno(syscall.Errno(st)).String()), zap.Int("errno", int(st)))
			return sendAck(seq, fserr.FromErrno(syscall.Errno(st)))
		}
		committed = seq
	}
}

// applyWalOp dispatches a single WalOp to the appropriate fs-layer call.
// Returns fuse.OK on success; any other status triggers ordered halt.
//
// Path-based ops that the unary handlers handle via session fds (Create,
// WriteOp) are applied as transient open/write/close sequences here.
// ReleaseOp is a no-op marker (watermark advance only).
//
// The caller (applyWalOp's caller = Apply loop) is responsible for dedup,
// session ownership, and watermark advance — this function only does the
// fs call + event emission.
func (r *RpcServerImpl) applyWalOp(
	ctx context.Context,
	fs pathfs.FileSystem,
	id *service.Identity,
	volume string,
	op *proto.WalOp,
) fuse.Status {
	switch v := op.Op.(type) {

	case *proto.WalOp_Create:
		return r.applyCreate(ctx, fs, id, volume, v.Create)

	case *proto.WalOp_Write:
		return r.applyWrite(ctx, fs, id, volume, v.Write)

	case *proto.WalOp_Release:
		// ReleaseOp is a no-op marker: the bytes were already written by
		// prior Create/Write ops. The watermark advance in the Apply loop
		// records the lifecycle; no fs call is needed.
		return fuse.OK

	case *proto.WalOp_Mkdir:
		res := r.applyPathOp(ctx, fs, id, volume, PathOp{
			Kind:   opMkdir,
			Path:   v.Mkdir.Path,
			Mode:   v.Mkdir.Mode,
			Caller: v.Mkdir.Caller,
		})
		return res.Status

	case *proto.WalOp_Rmdir:
		res := r.applyPathOp(ctx, fs, nil, volume, PathOp{
			Kind:   opRmdir,
			Path:   v.Rmdir.Path,
			Caller: v.Rmdir.Caller,
		})
		return res.Status

	case *proto.WalOp_Unlink:
		res := r.applyPathOp(ctx, fs, nil, volume, PathOp{
			Kind:   opUnlink,
			Path:   v.Unlink.Path,
			Caller: v.Unlink.Caller,
		})
		return res.Status

	case *proto.WalOp_Rename:
		res := r.applyPathOp(ctx, fs, nil, volume, PathOp{
			Kind:    opRename,
			OldName: v.Rename.OldName,
			NewName: v.Rename.NewName,
			Caller:  v.Rename.Caller,
		})
		return res.Status

	case *proto.WalOp_Symlink:
		res := r.applyPathOp(ctx, fs, id, volume, PathOp{
			Kind:   opSymlink,
			Path:   v.Symlink.LinkPath,
			Target: v.Symlink.Target,
			Caller: v.Symlink.Caller,
		})
		return res.Status

	case *proto.WalOp_SetAttr:
		res := r.applySetAttrOp(ctx, fs, id, volume, v.SetAttr)
		return res.Status

	case *proto.WalOp_SetXattr:
		res := r.applyPathOp(ctx, fs, nil, volume, PathOp{
			Kind:   opSetXAttr,
			Path:   v.SetXattr.Path,
			XAttr:  v.SetXattr.Attribute,
			XData:  v.SetXattr.Data,
			XFlags: int(v.SetXattr.Flags),
			Caller: v.SetXattr.Caller,
		})
		return res.Status

	case *proto.WalOp_RemoveXattr:
		res := r.applyPathOp(ctx, fs, nil, volume, PathOp{
			Kind:   opRemoveXAttr,
			Path:   v.RemoveXattr.Path,
			XAttr:  v.RemoveXattr.Attribute,
			Caller: v.RemoveXattr.Caller,
		})
		return res.Status

	default:
		// Unknown oneof variant — treat as EIO (programming error, not a
		// client protocol error). Ordered halt will fire.
		return fuse.EIO
	}
}

// walOpKindPath returns a human-readable (kind, path) for a WalOp, for halt and
// idempotent-skip logging. Nil-safe via the generated proto getters.
func walOpKindPath(op *proto.WalOp) (string, string) {
	switch v := op.Op.(type) {
	case *proto.WalOp_Create:
		return "create", v.Create.GetPath()
	case *proto.WalOp_Write:
		return "write", v.Write.GetPath()
	case *proto.WalOp_Mkdir:
		return "mkdir", v.Mkdir.GetPath()
	case *proto.WalOp_Rmdir:
		return "rmdir", v.Rmdir.GetPath()
	case *proto.WalOp_Unlink:
		return "unlink", v.Unlink.GetPath()
	case *proto.WalOp_Rename:
		return "rename", v.Rename.GetOldName() + " -> " + v.Rename.GetNewName()
	case *proto.WalOp_Symlink:
		return "symlink", v.Symlink.GetLinkPath()
	case *proto.WalOp_SetAttr:
		return "setattr", v.SetAttr.GetPath()
	case *proto.WalOp_SetXattr:
		return "setxattr", v.SetXattr.GetPath()
	case *proto.WalOp_RemoveXattr:
		return "removexattr", v.RemoveXattr.GetPath()
	case *proto.WalOp_Release:
		return "release", ""
	}
	return "unknown", ""
}

// idempotentSkip reports whether a failed apply is a BENIGN idempotent no-op
// whose desired end-state already holds, so the Apply loop should skip it and
// advance the watermark rather than ordered-halt (which discards this op AND
// every later op in the batch). Scoped tightly to never mask a real failure:
//
//   - mkdir EEXIST  → only when a DIRECTORY already exists at the path (a non-dir
//     there is a real type conflict and still halts). Duplicate mkdirs are normal
//     under concurrent extraction (npm) racing to create a shared parent dir.
//   - symlink EEXIST → only when a SYMLINK already exists at the path.
//   - unlink/rmdir ENOENT → the path is already absent: deletion's desired state.
//
// rename and every other (kind, errno) pair still ordered-halt. The extra
// GetAttr runs only on the (rare) apply-error path, so its cost is negligible.
func (r *RpcServerImpl) idempotentSkip(ctx context.Context, fs pathfs.FileSystem, op *proto.WalOp, st fuse.Status) bool {
	errno := syscall.Errno(st)
	switch v := op.Op.(type) {
	case *proto.WalOp_Mkdir:
		return errno == syscall.EEXIST &&
			existingFileMode(ctx, fs, v.Mkdir.GetPath(), v.Mkdir.GetCaller())&syscall.S_IFMT == syscall.S_IFDIR
	case *proto.WalOp_Symlink:
		return errno == syscall.EEXIST &&
			existingFileMode(ctx, fs, v.Symlink.GetLinkPath(), v.Symlink.GetCaller())&syscall.S_IFMT == syscall.S_IFLNK
	case *proto.WalOp_Unlink:
		return errno == syscall.ENOENT
	case *proto.WalOp_Rmdir:
		return errno == syscall.ENOENT
	}
	return false
}

// existingFileMode returns the full mode (including S_IFMT type bits) of path,
// or 0 if it cannot be stat'd. fs.GetAttr is lstat-style (does not follow the
// final symlink), so a symlink reports S_IFLNK.
func existingFileMode(ctx context.Context, fs pathfs.FileSystem, path string, caller *proto.Caller) uint32 {
	attr, gst := fs.GetAttr(path, createContext(ctx, caller))
	if !gst.Ok() {
		return 0
	}
	return attr.Mode
}

// applyCreate applies a path-based file creation (the deferred form of the
// unary Create RPC). It opens a transient handle via fs.Create, then
// immediately Flush+Release — the file is created and closed in one step.
// No session fd is registered (Apply is path-based).
//
// On success, a cache-invalidation event is emitted (same policy as the
// unary Create handler in file.go).
func (r *RpcServerImpl) applyCreate(
	ctx context.Context,
	fs pathfs.FileSystem,
	id *service.Identity,
	volume string,
	req *proto.CreateRequest,
) fuse.Status {
	fctx := createContext(ctx, req.Caller)
	file, st := fs.Create(req.Path, req.Flags, req.Mode, fctx)
	if st != fuse.OK {
		return st
	}
	// Flush ensures pending writes are visible; Release closes the handle.
	// We ignore Flush errors on a freshly-created empty file — the file
	// was created successfully and the content (if any) is written by
	// subsequent WriteOp entries.
	_ = file.Flush()
	file.Release()

	// Emit cache-invalidation (same contract as unary Create).
	if attr, gst := fs.GetAttr(req.Path, fctx); gst.Ok() {
		r.emitMutatedAttr(ctx, volume, req.Path, attr)
		_ = toProtoAttr(attr, id) // populate version token (return discarded here)
	} else {
		r.emitMutatedAttr(ctx, volume, req.Path, nil)
	}
	return fuse.OK
}

// applyWrite applies a path-based write (the deferred form of WriteAndFlush).
// It opens the file O_WRONLY (no O_CREAT — a preceding Create op must have
// materialised it; absent file → ENOENT → ordered halt is correct), writes
// the payload at the given offset, then Flush+Release.
//
// The open flag is O_WRONLY (syscall.O_WRONLY = 1). No O_TRUNC: WAL writes
// are positioned and the Create op already set the file to zero length.
//
// A cache-invalidation event is emitted after a successful write (same policy
// as applyCreate and applyPathOp): any subscriber holding the file read-only
// must see the updated content when deferred writes land.
func (r *RpcServerImpl) applyWrite(
	ctx context.Context,
	fs pathfs.FileSystem,
	id *service.Identity,
	volume string,
	req *proto.WriteOp,
) fuse.Status {
	fctx := createContext(ctx, req.Caller)
	file, st := fs.Open(req.Path, syscall.O_WRONLY, fctx)
	if st != fuse.OK {
		return st
	}
	defer func() {
		_ = file.Flush()
		file.Release()
	}()

	n, wst := file.Write(req.Data, req.Offset)
	if wst != fuse.OK {
		return wst
	}
	if int(n) != len(req.Data) {
		// Short write — treat as EIO; the caller will retry the whole batch.
		return fuse.EIO
	}

	// Emit cache-invalidation so subscribers see the updated content.
	if attr, gst := fs.GetAttr(req.Path, fctx); gst.Ok() {
		r.emitMutatedAttr(ctx, volume, req.Path, attr)
		_ = toProtoAttr(attr, id)
	} else {
		r.emitMutatedAttr(ctx, volume, req.Path, nil)
	}
	return fuse.OK
}

// walOpVolumeCaller extracts the volume and caller from the first op in a
// WalOp batch. Returns ("", nil) for an unknown/nil oneof variant.
func walOpVolumeCaller(op *proto.WalOp) (string, *proto.Caller) {
	if op == nil {
		return "", nil
	}
	switch v := op.Op.(type) {
	case *proto.WalOp_Create:
		return v.Create.GetVolume(), v.Create.GetCaller()
	case *proto.WalOp_Write:
		return v.Write.GetVolume(), v.Write.GetCaller()
	case *proto.WalOp_Release:
		return v.Release.GetVolume(), v.Release.GetCaller()
	case *proto.WalOp_Mkdir:
		return v.Mkdir.GetVolume(), v.Mkdir.GetCaller()
	case *proto.WalOp_Rmdir:
		return v.Rmdir.GetVolume(), v.Rmdir.GetCaller()
	case *proto.WalOp_Unlink:
		return v.Unlink.GetVolume(), v.Unlink.GetCaller()
	case *proto.WalOp_Rename:
		return v.Rename.GetVolume(), v.Rename.GetCaller()
	case *proto.WalOp_Symlink:
		return v.Symlink.GetVolume(), v.Symlink.GetCaller()
	case *proto.WalOp_SetAttr:
		return v.SetAttr.GetVolume(), v.SetAttr.GetCaller()
	case *proto.WalOp_SetXattr:
		return v.SetXattr.GetVolume(), v.SetXattr.GetCaller()
	case *proto.WalOp_RemoveXattr:
		return v.RemoveXattr.GetVolume(), v.RemoveXattr.GetCaller()
	default:
		return "", nil
	}
}

// isRevokedGen reports whether gen appears in the revoked set.
// The set is expected to be small (one entry per handoff), so a linear
// scan is correct and avoids allocation for the common case (empty set).
func isRevokedGen(gen uint64, revoked []uint64) bool {
	for _, r := range revoked {
		if r == gen {
			return true
		}
	}
	return false
}
