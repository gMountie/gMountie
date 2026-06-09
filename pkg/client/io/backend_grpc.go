// backend_grpc.go implements FileSystemBackend against the gRPC layer.
// It owns the wiring previously spread across fs.go (path-level ops) and
// file.go (per-fd ops + streaming Read/Write). Behaviour mirrors the
// legacy implementation: same retry shape, same per-call Snappy on
// Read/Write, same session/fd/request_id discipline from Phase 1.
package io

import (
	"context"
	stdio "io"
	"sync"
	"sync/atomic"
	"time"

	grpcclient "go.gmountie.dev/gmountie/pkg/client/grpc"
	"go.gmountie.dev/gmountie/pkg/proto"
	"go.gmountie.dev/gmountie/pkg/utils/log"

	"github.com/google/uuid"
	"github.com/hanwen/go-fuse/v2/fuse"
	"github.com/pkg/errors"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	grpcstatus "google.golang.org/grpc/status"
)

// readResult is the accumulated outcome of a single streaming Read attempt:
// how many bytes landed in dest and the terminal FUSE status reported by the
// server. The two are tracked together so retryOp can replace them
// wholesale on each attempt without partial-state bleed-through.
type readResult struct {
	written int
	status  fuse.Status
}

// writeFrameSizeBytes bounds a single WriteFrame's data slice. Hardcoded
// here for now; Task 7 of the Phase 3 plan negotiates this value with the
// server. 1 MiB matches the server's default FrameSizeBytes.
const writeFrameSizeBytes = 1 << 20

// BackendClient implements FileSystemBackend against the gRPC layer.
// Per-fd state lives on grpcFileHandle, returned by Open/Create and
// passed back into Read/Write/Flush/Fsync/Release.
type BackendClient struct {
	client grpcclient.Client
	volume string
}

// NewBackendClient returns a BackendClient that talks to client on volume.
func NewBackendClient(client grpcclient.Client, volume string) *BackendClient {
	return &BackendClient{client: client, volume: volume}
}

// callerFromCtx returns a proto.Caller for the request, pulled from the
// per-op ctx that go-fuse populates with the kernel-reported caller
// (uid/gid/pid of the syscalling process). Stamping this correctly is
// load-bearing: the server resolves these fields into an identity and runs
// setfsuid / setfsgid / setgroups from it on every op, so a zero Caller would
// have the server side run as root.
//
// The zero-Caller fallback covers the rare case where no Caller is in
// ctx — typically unit tests with a bare context.Background(). It must
// not fire in production; if it does, that's a bug in the node adapter
// (not propagating the FUSE ctx through to the backend).
func callerFromCtx(ctx context.Context) *proto.Caller {
	if c, ok := fuse.FromContext(ctx); ok {
		return &proto.Caller{
			Owner: &proto.Owner{Uid: c.Uid, Gid: c.Gid},
			Pid:   c.Pid,
		}
	}
	return &proto.Caller{Owner: &proto.Owner{}}
}

// attrFromProto maps a proto.Attr (server wire type) to the package-local
// Attr struct returned by the FileSystemBackend interface.
func attrFromProto(p *proto.Attr) *Attr {
	if p == nil {
		return nil
	}
	out := &Attr{
		Ino:       p.Ino,
		Size:      p.Size,
		Blocks:    p.Blocks,
		Atime:     p.Atime,
		Mtime:     p.Mtime,
		Ctime:     p.Ctime,
		Atimensec: p.Atimensec,
		Mtimensec: p.Mtimensec,
		Ctimensec: p.Ctimensec,
		Mode:      p.Mode,
		Nlink:     p.Nlink,
		Rdev:      p.Rdev,
		Blksize:   p.Blksize,
		Version:   p.Version,
	}
	// The legacy fs.go.GetAttr maps server-side ownership from Owner, not
	// from the top-level Uid/Gid fields. Preserve that behaviour, falling
	// back to the top-level fields if Owner is unset.
	if p.Owner != nil {
		out.Uid = p.Owner.Uid
		out.Gid = p.Owner.Gid
	} else {
		out.Uid = p.Uid
		out.Gid = p.Gid
	}
	return out
}

// joinPath joins parent and name with a "/" separator. An empty parent
// returns name unchanged, matching Lookup's expectation when the caller
// already passes the root.
func joinPath(parent, name string) string {
	if parent == "" {
		return name
	}
	return parent + "/" + name
}

// ioCtx bounds an fd-level RPC with the per-handle I/O timeout AND detaches it
// from the caller's cancellation: a FUSE_INTERRUPT from Go's async-preemption
// SIGURG (which the kernel fires on a long-running op) must not abort an
// in-flight RPC with a spurious EIO (confirmed: the failures vanish under
// GODEBUG=asyncpreemptoff=1). context.WithoutCancel keeps the caller's values
// (the kernel uid/gid/pid the server resolves into an identity) while ignoring
// its cancellation, so the RPC runs to completion or its own timeout.
//
// retryOp now provides this same detach-and-bound behaviour for every retried
// op (reads, mutations, fd-ops, Open/Create). ioCtx survives only for
// CopyFileRange, which is deliberately NOT retried (a partially-applied copy
// that lost its reply must not be replayed). The blocking lock wait (SetLkw)
// keeps a cancellable context (plain context.WithTimeout) so a signal can
// still interrupt it; the readahead prefetch path bounds h.lifeCtx the same way.
func ioCtx(parent context.Context, timeout time.Duration) (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.WithoutCancel(parent), timeout)
}

// statusFromRPCError maps a transport-level gRPC error to the closest FUSE
// errno, instead of the blanket EIO that loses errno fidelity. The mapping
// mirrors what the server actually produces (controller/*.go, grpc/auth.go):
//
//	NotFound         → ENOENT  (path gone, fd/session not found — e.g. reaped)
//	PermissionDenied → EACCES  (ACL/identity denial, session ownership)
//	Unauthenticated  → EACCES  (revoked cert, bad credentials)
//	anything else    → EIO     (honest "transport-level unknown")
//
// In-band failures still arrive as fuse.Status in the reply body; this helper
// only covers the error arm of each RPC.
func statusFromRPCError(err error) fuse.Status {
	s, ok := grpcstatus.FromError(err)
	if !ok || s == nil {
		return fuse.EIO
	}
	switch s.Code() {
	case codes.NotFound:
		return fuse.ENOENT
	case codes.PermissionDenied, codes.Unauthenticated:
		return fuse.EACCES
	default:
		return fuse.EIO
	}
}

// Stat returns the attributes of path. Idempotent; no request_id stamping.
func (b *BackendClient) Stat(ctx context.Context, path string) (*Attr, fuse.Status) {
	res, err := retryOp(b.client, ctx, "GetAttr", classIdempotentRead, b.client.MetaTimeout(),
		func(ctx context.Context) (*proto.GetAttrReply, error) {
			return b.client.Fs().GetAttr(ctx, &proto.GetAttrRequest{
				Volume: b.volume,
				Caller: callerFromCtx(ctx),
				Path:   path,
			}, grpc.WaitForReady(true))
		})
	if err != nil || res == nil {
		log.Log.Error("error in call: GetAttr", zap.String("path", path), zap.Error(err))
		return nil, statusFromRPCError(err)
	}
	if res.GetAttributes() == nil {
		return nil, fuse.Status(res.Status)
	}
	return attrFromProto(res.GetAttributes()), fuse.Status(res.Status)
}

// GetAttrIfChanged issues the lightweight revalidation RPC. Returns
// (nil, true, OK) on NotModified, (newAttr, false, OK) on version change,
// (nil, false, ENOENT) if the path is gone, (nil, false, EIO) on error.
func (b *BackendClient) GetAttrIfChanged(ctx context.Context, path string, knownVersion uint64) (*Attr, bool, fuse.Status) {
	reply, err := retryOp(b.client, ctx, "GetAttrIfChanged", classIdempotentRead, b.client.MetaTimeout(),
		func(ctx context.Context) (*proto.GetAttrIfChangedReply, error) {
			return b.client.Fs().GetAttrIfChanged(ctx, &proto.GetAttrIfChangedRequest{
				Volume:       b.volume,
				Caller:       callerFromCtx(ctx),
				Path:         path,
				KnownVersion: knownVersion,
			}, grpc.WaitForReady(true))
		})
	if err != nil {
		st := statusFromRPCError(err)
		if st == fuse.ENOENT {
			// Path gone is an expected revalidation outcome — no error log.
			return nil, false, fuse.ENOENT
		}
		log.Log.Error("error in call: GetAttrIfChanged", zap.String("path", path), zap.Error(err))
		return nil, false, st
	}
	if reply.GetNotModified() {
		return nil, true, fuse.OK
	}
	return attrFromProto(reply.GetAttrs()), false, fuse.OK
}

// Close is a no-op for BackendClient: the gRPC connection lifetime is
// managed by the caller (grpc.Client). Implements io.FileSystemBackend.
func (b *BackendClient) Close() error { return nil }

// Lookup resolves a child by name under parent. The server exposes no
// dedicated Lookup RPC; this is implemented as GetAttr on the joined
// path. The inode is folded into Attr.Ino.
func (b *BackendClient) Lookup(ctx context.Context, parent, name string) (*Attr, fuse.Status) {
	return b.Stat(ctx, joinPath(parent, name))
}

// ListDir returns the directory entries at path. Idempotent.
func (b *BackendClient) ListDir(ctx context.Context, path string) ([]DirEntry, fuse.Status) {
	res, err := retryOp(b.client, ctx, "OpenDir", classIdempotentRead, b.client.MetaTimeout(),
		func(ctx context.Context) (*proto.OpenDirReply, error) {
			return b.client.Fs().OpenDir(ctx, &proto.OpenDirRequest{
				Volume: b.volume,
				Caller: callerFromCtx(ctx),
				Path:   path,
			}, grpc.WaitForReady(true))
		})
	if err != nil || res == nil {
		log.Log.Error("error in call: OpenDir", zap.String("path", path), zap.Error(err))
		return nil, statusFromRPCError(err)
	}
	var entries []DirEntry
	for _, entry := range res.Entries {
		entries = append(entries, DirEntry{
			Ino:  entry.Ino,
			Mode: entry.Mode,
			Name: entry.Name,
		})
	}
	return entries, fuse.Status(res.Status)
}

// Access mirrors access(2). Idempotent.
func (b *BackendClient) Access(ctx context.Context, path string, mode uint32) fuse.Status {
	res, err := retryOp(b.client, ctx, "Access", classIdempotentRead, b.client.MetaTimeout(),
		func(ctx context.Context) (*proto.AccessReply, error) {
			return b.client.Fs().Access(ctx, &proto.AccessRequest{
				Volume: b.volume,
				Caller: callerFromCtx(ctx),
				Path:   path,
				Mode:   mode,
			}, grpc.WaitForReady(true))
		})
	if err != nil || res == nil {
		log.Log.Error("error in call: Access", zap.String("path", path), zap.Error(err))
		return statusFromRPCError(err)
	}
	return fuse.Status(res.Status)
}

// Readlink returns the target string of a symbolic link. Idempotent.
func (b *BackendClient) Readlink(ctx context.Context, path string) (string, fuse.Status) {
	res, err := retryOp(b.client, ctx, "Readlink", classIdempotentRead, b.client.MetaTimeout(),
		func(ctx context.Context) (*proto.ReadlinkReply, error) {
			return b.client.Fs().Readlink(ctx, &proto.ReadlinkRequest{
				Volume: b.volume, Caller: callerFromCtx(ctx), Path: path,
			}, grpc.WaitForReady(true))
		})
	if err != nil || res == nil {
		log.Log.Error("error in call: Readlink", zap.String("path", path), zap.Error(err))
		return "", statusFromRPCError(err)
	}
	return res.Target, fuse.Status(res.Status)
}

// mutatePath is the shared body for all single-path mutating RPCs. It
// allocates a request_id ONCE outside the retry closure so that all
// attempts within the same session carry the same ID and hit the server's
// idempotency cache. Routes through retryOp with classPathMutation so a
// session-id change (Create-fallback recovery) stops the retry immediately
// — the new session's idempotency cache is empty and a replay could surface
// a spurious EEXIST/ENOENT. Returns statusFromRPCError's errno mapping on a
// gRPC error, otherwise fuse.Status as returned by statusOf.
// logFields carries the per-op correlation fields (path(s)) into the error
// log, alongside the request_id — without them a user-facing errno from
// Mkdir/Unlink/Chmod/... could not be tied to a file or to the server-side
// idempotency record.
func mutatePath[Rep any](
	b *BackendClient,
	ctx context.Context,
	op string,
	fn func(ctx context.Context, requestID string) (Rep, error),
	statusOf func(Rep) int32,
	logFields ...zap.Field,
) fuse.Status {
	requestID := uuid.NewString() // outside retryOp: stable across attempts for idempotency
	res, err := retryOp(b.client, ctx, op, classPathMutation, b.client.MetaTimeout(),
		func(ctx context.Context) (Rep, error) {
			return fn(ctx, requestID)
		})
	if err != nil {
		fields := append([]zap.Field{zap.String("request_id", requestID), zap.Error(err)}, logFields...)
		log.Log.Error("error in call: "+op, fields...)
		return statusFromRPCError(err)
	}
	return fuse.Status(statusOf(res))
}

// Symlink creates a new symbolic link at linkPath pointing at target.
// Mutating — request_id stamped outside retry for idempotency.
func (b *BackendClient) Symlink(ctx context.Context, target, linkPath string) fuse.Status {
	return mutatePath(b, ctx, "Symlink",
		func(ctx context.Context, requestID string) (*proto.SymlinkReply, error) {
			return b.client.Fs().Symlink(ctx, &proto.SymlinkRequest{
				Volume:    b.volume,
				Caller:    callerFromCtx(ctx),
				Target:    target,
				LinkPath:  linkPath,
				SessionId: b.client.SessionID(),
				RequestId: requestID,
			}, grpc.WaitForReady(true))
		},
		func(r *proto.SymlinkReply) int32 { return r.Status },
		zap.String("target", target), zap.String("link_path", linkPath),
	)
}

// StatFs returns filesystem statistics for the volume containing path.
func (b *BackendClient) StatFs(ctx context.Context, path string) (*StatFs, fuse.Status) {
	res, err := retryOp(b.client, ctx, "StatFs", classIdempotentRead, b.client.MetaTimeout(),
		func(ctx context.Context) (*proto.StatFsReply, error) {
			return b.client.Fs().StatFs(ctx, &proto.StatFsRequest{Volume: b.volume, Path: path},
				grpc.WaitForReady(true))
		})
	if err != nil || res == nil {
		log.Log.Error("error in call: StatFs", zap.String("path", path), zap.Error(err))
		return nil, statusFromRPCError(err)
	}
	return &StatFs{
		Blocks:  res.Blocks,
		Bfree:   res.Bfree,
		Bavail:  res.Bavail,
		Files:   res.Files,
		Ffree:   res.Ffree,
		Bsize:   res.Bsize,
		Namelen: res.Namelen,
		Frsize:  res.Frsize,
	}, fuse.OK
}

// GetXAttr returns extended-attribute bytes. Idempotent.
func (b *BackendClient) GetXAttr(ctx context.Context, path, attr string) ([]byte, fuse.Status) {
	res, err := retryOp(b.client, ctx, "GetXAttr", classIdempotentRead, b.client.MetaTimeout(),
		func(ctx context.Context) (*proto.GetXAttrReply, error) {
			return b.client.Fs().GetXAttr(ctx, &proto.GetXAttrRequest{
				Volume: b.volume, Caller: callerFromCtx(ctx), Path: path, Attribute: attr,
			}, grpc.WaitForReady(true))
		})
	if err != nil || res == nil {
		log.Log.Error("error in call: GetXAttr", zap.String("path", path), zap.Error(err))
		return nil, statusFromRPCError(err)
	}
	return res.Data, fuse.Status(res.Status)
}

// SetXAttr stores an extended attribute. Mutating — request_id stamped
// outside retry for idempotency, like Chmod.
func (b *BackendClient) SetXAttr(ctx context.Context, path, attr string, data []byte, flags uint32) fuse.Status {
	return mutatePath(b, ctx, "SetXAttr",
		func(ctx context.Context, requestID string) (*proto.SetXAttrReply, error) {
			return b.client.Fs().SetXAttr(ctx, &proto.SetXAttrRequest{
				Volume:    b.volume,
				Caller:    callerFromCtx(ctx),
				Path:      path,
				Attribute: attr,
				Data:      data,
				Flags:     flags,
				SessionId: b.client.SessionID(),
				RequestId: requestID,
			}, grpc.WaitForReady(true))
		},
		func(r *proto.SetXAttrReply) int32 { return r.Status },
		zap.String("path", path),
	)
}

// RemoveXAttr deletes an extended attribute. Mutating — see SetXAttr.
func (b *BackendClient) RemoveXAttr(ctx context.Context, path, attr string) fuse.Status {
	return mutatePath(b, ctx, "RemoveXAttr",
		func(ctx context.Context, requestID string) (*proto.RemoveXAttrReply, error) {
			return b.client.Fs().RemoveXAttr(ctx, &proto.RemoveXAttrRequest{
				Volume:    b.volume,
				Caller:    callerFromCtx(ctx),
				Path:      path,
				Attribute: attr,
				SessionId: b.client.SessionID(),
				RequestId: requestID,
			}, grpc.WaitForReady(true))
		},
		func(r *proto.RemoveXAttrReply) int32 { return r.Status },
		zap.String("path", path),
	)
}

// ListXAttr returns extended-attribute names. Idempotent.
func (b *BackendClient) ListXAttr(ctx context.Context, path string) ([]string, fuse.Status) {
	res, err := retryOp(b.client, ctx, "ListXAttr", classIdempotentRead, b.client.MetaTimeout(),
		func(ctx context.Context) (*proto.ListXAttrReply, error) {
			return b.client.Fs().ListXAttr(ctx, &proto.ListXAttrRequest{
				Volume: b.volume, Caller: callerFromCtx(ctx), Path: path,
			}, grpc.WaitForReady(true))
		})
	if err != nil || res == nil {
		log.Log.Error("error in call: ListXAttr", zap.String("path", path), zap.Error(err))
		return nil, statusFromRPCError(err)
	}
	return res.Attributes, fuse.Status(res.Status)
}

// Mkdir creates a directory. Mutating — request_id stamped outside retry.
func (b *BackendClient) Mkdir(ctx context.Context, path string, mode uint32) fuse.Status {
	return mutatePath(b, ctx, "Mkdir",
		func(ctx context.Context, requestID string) (*proto.MkdirReply, error) {
			return b.client.Fs().Mkdir(ctx, &proto.MkdirRequest{
				Volume:    b.volume,
				Caller:    callerFromCtx(ctx),
				Path:      path,
				Mode:      mode,
				SessionId: b.client.SessionID(),
				RequestId: requestID,
			}, grpc.WaitForReady(true))
		},
		func(r *proto.MkdirReply) int32 { return r.Status },
		zap.String("path", path),
	)
}

// Rmdir removes an empty directory.
func (b *BackendClient) Rmdir(ctx context.Context, path string) fuse.Status {
	return mutatePath(b, ctx, "Rmdir",
		func(ctx context.Context, requestID string) (*proto.RmdirReply, error) {
			return b.client.Fs().Rmdir(ctx, &proto.RmdirRequest{
				Volume:    b.volume,
				Caller:    callerFromCtx(ctx),
				Path:      path,
				SessionId: b.client.SessionID(),
				RequestId: requestID,
			}, grpc.WaitForReady(true))
		},
		func(r *proto.RmdirReply) int32 { return r.Status },
		zap.String("path", path),
	)
}

// Unlink removes a non-directory.
func (b *BackendClient) Unlink(ctx context.Context, path string) fuse.Status {
	return mutatePath(b, ctx, "Unlink",
		func(ctx context.Context, requestID string) (*proto.UnlinkReply, error) {
			return b.client.Fs().Unlink(ctx, &proto.UnlinkRequest{
				Volume:    b.volume,
				Caller:    callerFromCtx(ctx),
				Path:      path,
				SessionId: b.client.SessionID(),
				RequestId: requestID,
			}, grpc.WaitForReady(true))
		},
		func(r *proto.UnlinkReply) int32 { return r.Status },
		zap.String("path", path),
	)
}

// Rename moves a file/directory.
func (b *BackendClient) Rename(ctx context.Context, oldPath, newPath string) fuse.Status {
	return mutatePath(b, ctx, "Rename",
		func(ctx context.Context, requestID string) (*proto.RenameReply, error) {
			return b.client.Fs().Rename(ctx, &proto.RenameRequest{
				Volume:    b.volume,
				Caller:    callerFromCtx(ctx),
				OldName:   oldPath,
				NewName:   newPath,
				SessionId: b.client.SessionID(),
				RequestId: requestID,
			}, grpc.WaitForReady(true))
		},
		func(r *proto.RenameReply) int32 { return r.Status },
		zap.String("old_path", oldPath), zap.String("new_path", newPath),
	)
}

// Truncate changes a file's length.
func (b *BackendClient) Truncate(ctx context.Context, path string, size uint64) fuse.Status {
	return mutatePath(b, ctx, "Truncate",
		func(ctx context.Context, requestID string) (*proto.TruncateReply, error) {
			return b.client.Fs().Truncate(ctx, &proto.TruncateRequest{
				Volume:    b.volume,
				Caller:    callerFromCtx(ctx),
				Path:      path,
				Size:      size,
				SessionId: b.client.SessionID(),
				RequestId: requestID,
			}, grpc.WaitForReady(true))
		},
		func(r *proto.TruncateReply) int32 { return r.Status },
		zap.String("path", path),
	)
}

// Chmod changes file permissions.
func (b *BackendClient) Chmod(ctx context.Context, path string, mode uint32) fuse.Status {
	return mutatePath(b, ctx, "Chmod",
		func(ctx context.Context, requestID string) (*proto.ChmodReply, error) {
			return b.client.Fs().Chmod(ctx, &proto.ChmodRequest{
				Volume:    b.volume,
				Caller:    callerFromCtx(ctx),
				Path:      path,
				Mode:      mode,
				SessionId: b.client.SessionID(),
				RequestId: requestID,
			}, grpc.WaitForReady(true))
		},
		func(r *proto.ChmodReply) int32 { return r.Status },
		zap.String("path", path),
	)
}

// Chown changes ownership.
func (b *BackendClient) Chown(ctx context.Context, path string, uid, gid uint32) fuse.Status {
	return mutatePath(b, ctx, "Chown",
		func(ctx context.Context, requestID string) (*proto.ChownReply, error) {
			return b.client.Fs().Chown(ctx, &proto.ChownRequest{
				Volume:    b.volume,
				Caller:    callerFromCtx(ctx),
				Path:      path,
				Uid:       uid,
				Gid:       gid,
				SessionId: b.client.SessionID(),
				RequestId: requestID,
			}, grpc.WaitForReady(true))
		},
		func(r *proto.ChownReply) int32 { return r.Status },
		zap.String("path", path),
	)
}

// timeToFileTime maps a Go time to the wire FileTime. A nil input yields a
// nil FileTime — UTIME_OMIT (leave that timestamp unchanged).
func timeToFileTime(t *time.Time) *proto.FileTime {
	if t == nil {
		return nil
	}
	return &proto.FileTime{Sec: uint64(t.Unix()), Nsec: uint32(t.Nanosecond())}
}

// Utimens sets atime and/or mtime. A nil pointer leaves that timestamp
// unchanged (UTIME_OMIT).
func (b *BackendClient) Utimens(ctx context.Context, path string, atime, mtime *time.Time) fuse.Status {
	return mutatePath(b, ctx, "Utimens",
		func(ctx context.Context, requestID string) (*proto.UtimensReply, error) {
			return b.client.Fs().Utimens(ctx, &proto.UtimensRequest{
				Volume:    b.volume,
				Caller:    callerFromCtx(ctx),
				Path:      path,
				Atime:     timeToFileTime(atime),
				Mtime:     timeToFileTime(mtime),
				SessionId: b.client.SessionID(),
				RequestId: requestID,
			}, grpc.WaitForReady(true))
		},
		func(r *proto.UtimensReply) int32 { return r.Status },
		zap.String("path", path),
	)
}

// Open opens an existing file. The returned FileHandle is a
// *grpcFileHandle holding fd + session + per-file knobs.
func (b *BackendClient) Open(ctx context.Context, path string, flags uint32) (FileHandle, fuse.Status) {
	// classFdOp: Open establishes an fd. On a session-id change that fd would be
	// dead/leaked on the (new) server session, so retryOp stops retrying. The
	// request_id is allocated OUTSIDE retryOp so same-session retries dedup
	// against the server's Open idempotency cache. Path-level timeout (MetaTimeout).
	requestID := uuid.NewString()
	res, err := retryOp(b.client, ctx, "Open", classFdOp, b.client.MetaTimeout(),
		func(ctx context.Context) (*proto.OpenReply, error) {
			return b.client.File().Open(ctx, &proto.OpenRequest{
				Volume:    b.volume,
				Caller:    callerFromCtx(ctx),
				Path:      path,
				Flags:     flags,
				SessionId: b.client.SessionID(),
				RequestId: requestID,
			}, grpc.WaitForReady(true))
		})
	if err != nil || res == nil {
		log.Log.Error("error in call: Open", zap.String("path", path), zap.Error(err))
		return nil, statusFromRPCError(err)
	}
	if fuse.Status(res.Status) != fuse.OK {
		return nil, fuse.Status(res.Status)
	}
	return newGrpcFileHandle(
		b.client, b.volume, path, res.Fd,
		b.client.IOTimeout(), b.client.SessionID(),
		b.client.PerFileConfig(),
	), fuse.OK
}

// Create creates a new file. When CreateReply carries an Attributes field the
// mapped *Attr is returned so the node can populate the kernel's EntryOut
// without a follow-up Stat RPC. A nil Attr is returned when the server omits
// Attributes (older servers); in that case the node adapter falls back to Stat.
func (b *BackendClient) Create(ctx context.Context, parent, name string, flags, mode uint32) (FileHandle, *Attr, fuse.Status) {
	path := joinPath(parent, name)
	// classPathMutation: Create mutates the namespace, so a session-id change
	// makes a replay unsafe (the new session's idempotency cache is empty —
	// replay could surface a spurious EEXIST). The request_id is allocated
	// OUTSIDE retryOp so same-session retries dedup against the server cache
	// (like mutatePath). Path-level timeout (MetaTimeout).
	requestID := uuid.NewString()
	res, err := retryOp(b.client, ctx, "Create", classPathMutation, b.client.MetaTimeout(),
		func(ctx context.Context) (*proto.CreateReply, error) {
			return b.client.File().Create(ctx, &proto.CreateRequest{
				Volume:    b.volume,
				Caller:    callerFromCtx(ctx),
				Path:      path,
				Flags:     flags,
				Mode:      mode,
				SessionId: b.client.SessionID(),
				RequestId: requestID,
			}, grpc.WaitForReady(true))
		})
	if err != nil || res == nil {
		log.Log.Error("error in call: Create", zap.String("path", path), zap.Error(err))
		return nil, nil, statusFromRPCError(err)
	}
	if fuse.Status(res.Status) != fuse.OK {
		return nil, nil, fuse.Status(res.Status)
	}
	h := newGrpcFileHandle(
		b.client, b.volume, path, res.Fd,
		b.client.IOTimeout(), b.client.SessionID(),
		b.client.PerFileConfig(),
	)
	return h, attrFromProto(res.Attributes), fuse.OK
}

// Read consumes the server-streaming Read RPC, accumulating frames into
// dest in order. Mirrors GrpcFile.Read verbatim — the only differences
// are the FileHandle boundary and the (int, fuse.Status) return shape.
//
// Idempotent: each retry attempt opens a fresh stream; no request_id.
func (b *BackendClient) Read(ctx context.Context, fh FileHandle, off int64, dest []byte) (int, fuse.Status) {
	h := resolveHandle(fh)
	if h == nil {
		return 0, fuse.EBADF
	}
	if h.readahead != nil {
		if n, hit := h.readahead.Serve(dest, off); hit {
			for _, prefetchOff := range h.readahead.Observe(off, n) {
				b.spawnPrefetch(h, prefetchOff)
			}
			return n, fuse.OK
		}
	}
	// classFdOp: server-streaming Read. WaitForReady parks the stream-open on a
	// CONNECTING channel instead of burning retry attempts on instant
	// Unavailable; the per-attempt deadline still bounds the wait. On a session
	// change the fd is dead, so retryOp stops retrying.
	res, err := retryOp(h.client, ctx, "Read", classFdOp, h.ioTimeout, func(ctx context.Context) (readResult, error) {
		stream, err := h.fileClient.Read(ctx, &proto.ReadRequest{
			Volume:    h.volume,
			Fd:        h.fd,
			Offset:    off,
			Size:      uint32(len(dest)),
			SessionId: h.sessionID,
		}, grpc.WaitForReady(true))
		if err != nil {
			return readResult{}, err
		}
		out := readResult{status: fuse.OK}
		for {
			frame, recvErr := stream.Recv()
			if errors.Is(recvErr, stdio.EOF) {
				return out, nil
			}
			if recvErr != nil {
				return readResult{}, recvErr
			}
			if st := fuse.Status(frame.GetStatus()); !st.Ok() {
				out.status = st
				return out, nil
			}
			data := frame.GetData()
			if len(data) == 0 {
				// Terminal OK frame: server signalled clean end-of-stream.
				continue
			}
			if out.written+len(data) > len(dest) {
				return readResult{}, errors.New("server sent more bytes than requested")
			}
			copy(dest[out.written:], data)
			out.written += len(data)
		}
	})
	if err != nil {
		log.Log.Error("error in call: Read", zap.String("path", h.path), zap.Error(err))
		return 0, statusFromRPCError(err)
	}
	if !res.status.Ok() {
		return 0, res.status
	}
	if h.readahead != nil {
		for _, prefetchOff := range h.readahead.Observe(off, res.written) {
			b.spawnPrefetch(h, prefetchOff)
		}
	}
	return res.written, fuse.OK
}

// spawnPrefetch launches doPrefetch in a goroutine tracked by the handle's
// WaitGroup so Release can wait for in-flight prefetches. It is a no-op once
// Release has begun: the released flag is flipped under prefetchMu before
// Release calls Wait, which guarantees no WaitGroup.Add can race the Wait
// (the documented Add/Wait discipline).
func (b *BackendClient) spawnPrefetch(h *grpcFileHandle, off int64) {
	h.prefetchMu.Lock()
	if h.released {
		h.prefetchMu.Unlock()
		return
	}
	h.prefetchWG.Add(1)
	h.prefetchMu.Unlock()
	go func() {
		defer h.prefetchWG.Done()
		b.doPrefetch(h, off)
	}()
}

// doPrefetch issues a streaming Read for the next chunk and parks the
// result in the readahead ring on success. Runs under h.lifeCtx so
// Release cancels in-flight prefetches. Errors are swallowed — the next
// synchronous Read will refetch if the ring is empty.
func (b *BackendClient) doPrefetch(h *grpcFileHandle, off int64) {
	if h.readahead == nil {
		return
	}
	chunk := h.readahead.chunkSize
	ctx, cancel := context.WithTimeout(h.lifeCtx, h.ioTimeout)
	defer cancel()
	stream, err := h.fileClient.Read(ctx, &proto.ReadRequest{
		Volume:    h.volume,
		Fd:        h.fd,
		Offset:    off,
		Size:      uint32(chunk),
		SessionId: h.sessionID,
	})
	if err != nil {
		log.Log.Debug("readahead prefetch: stream open failed", zap.String("path", h.path), zap.Int64("offset", off), zap.Error(err))
		return
	}
	buf := make([]byte, chunk)
	written := 0
	for {
		frame, recvErr := stream.Recv()
		if errors.Is(recvErr, stdio.EOF) {
			break
		}
		if recvErr != nil {
			log.Log.Debug("readahead prefetch: stream recv failed", zap.String("path", h.path), zap.Int64("offset", off), zap.Error(recvErr))
			return
		}
		if st := fuse.Status(frame.GetStatus()); !st.Ok() {
			log.Log.Debug("readahead prefetch: server returned non-OK status", zap.String("path", h.path), zap.Int64("offset", off), zap.Stringer("status", st))
			return
		}
		data := frame.GetData()
		if len(data) == 0 {
			continue
		}
		if written+len(data) > len(buf) {
			return
		}
		copy(buf[written:], data)
		written += len(data)
	}
	if written == 0 {
		return
	}
	h.readahead.Store(off, buf[:written])
}

// streamingWrite issues a single server-streaming Write RPC for data at
// off, stamped with the caller-supplied requestID. requestID flows
// through every retry attempt unchanged so the server's per-session
// idempotency LRU short-circuits the replay on the second attempt. The
// retry closure re-opens the stream from scratch on each attempt.
//
// Frame 1 carries the header (volume, fd, session_id, request_id,
// offset) plus the first writeFrameSizeBytes of data. Subsequent frames
// carry only the data slice.
func (b *BackendClient) streamingWrite(h *grpcFileHandle, data []byte, off int64, requestID string) (uint32, fuse.Status) {
	// classFdOp: client-streaming Write. requestID is allocated by the caller
	// (outside this retry) so every attempt replays the same id and the server's
	// idempotency LRU short-circuits the duplicate — the load-bearing Phase-1d
	// guard. This path runs off context.Background() (detached from the FUSE op
	// ctx), passed to retryOp as the fuseCtx arg; retryOp derives each attempt's
	// own deadline + lifetime cancellation from it. WaitForReady parks the
	// stream-open on a CONNECTING channel instead of burning retry attempts on
	// instant Unavailable; the per-attempt deadline still bounds the wait.
	res, err := retryOp(h.client, context.Background(), "Write", classFdOp, h.ioTimeout,
		func(ctx context.Context) (*proto.WriteReply, error) {
			stream, err := h.fileClient.Write(ctx, grpc.WaitForReady(true))
			if err != nil {
				return nil, err
			}
			first := writeFrameSizeBytes
			if first > len(data) {
				first = len(data)
			}
			header := &proto.WriteFrame{
				Volume:    h.volume,
				Fd:        h.fd,
				SessionId: h.sessionID,
				RequestId: requestID,
				Offset:    off,
				Data:      data[:first],
			}
			if sendErr := stream.Send(header); sendErr != nil {
				return nil, sendErr
			}
			for sent := first; sent < len(data); {
				end := sent + writeFrameSizeBytes
				if end > len(data) {
					end = len(data)
				}
				if sendErr := stream.Send(&proto.WriteFrame{Data: data[sent:end]}); sendErr != nil {
					return nil, sendErr
				}
				sent = end
			}
			return stream.CloseAndRecv()
		})
	if err != nil || res == nil {
		log.Log.Error("error in call: Write", zap.String("path", h.path), zap.Error(err))
		return 0, statusFromRPCError(err)
	}
	return res.Written, fuse.Status(res.Status)
}

// Write proxies a FUSE Write to the server-streaming Write RPC.
//
// When per-fd write coalescing is enabled (h.coalescer != nil), small
// contiguous writes accumulate up to h.coalesceThreshold and are
// returned as successful to FUSE optimistically — durability is
// established on Flush. Big writes (len(data) >= threshold) bypass the
// coalescer: pending bytes are drained first to preserve on-disk order.
//
// The handle's dirty flag is set on every accepted write so that Flush
// can skip the RPC on a truly clean handle.
func (b *BackendClient) Write(ctx context.Context, fh FileHandle, off int64, data []byte) (uint32, fuse.Status) {
	h := resolveHandle(fh)
	if h == nil {
		return 0, fuse.EBADF
	}
	// Mark dirty so the next Flush emits a WriteAndFlush. The kernel does NOT
	// guarantee that FUSE ops on one fd are serialized; the safety argument is:
	// h.dirty is an atomic bool and the coalescer locks internally, so nothing
	// tears. Setting dirty BEFORE buffering preserves the invariant "bytes in
	// the coalescer ⇒ dirty is true" — a Flush racing this Write can at worst
	// observe dirty=true with the bytes not yet appended and issue an empty
	// WriteAndFlush; those bytes then reach durability on the next
	// Flush/Fsync/Release (an application racing write(2) against close(2) has
	// no ordering guarantee to lose).
	h.dirty.Store(true)
	// Coalescing disabled: pass through to the streaming Write directly.
	if h.coalescer == nil {
		return b.streamingWrite(h, data, off, uuid.NewString())
	}
	// Big writes bypass the buffer. Drain any pending bytes first so the
	// on-disk order matches the call order.
	if len(data) >= h.coalesceThreshold {
		if pending := h.coalescer.Drain(); pending != nil {
			if _, st := b.streamingWrite(h, pending.Data, pending.Offset, uuid.NewString()); !st.Ok() {
				return 0, st
			}
		}
		return b.streamingWrite(h, data, off, uuid.NewString())
	}
	// Small write: append. If the coalescer hands back a batch, flush it.
	// Optimistic return: tell FUSE we wrote len(data) bytes even though
	// they may only be buffered. Durability arrives on Flush.
	if batch := h.coalescer.Append(data, off); batch != nil {
		if _, st := b.streamingWrite(h, batch.Data, batch.Offset, uuid.NewString()); !st.Ok() {
			return 0, st
		}
	}
	return uint32(len(data)), fuse.OK
}

// drainCoalescer flushes any pending coalesced bytes to the wire.
// Returns fuse.OK on a no-op or clean send; returns the failing status
// if the streaming Write reports one.
func (b *BackendClient) drainCoalescer(h *grpcFileHandle) fuse.Status {
	if h.coalescer == nil {
		return fuse.OK
	}
	pending := h.coalescer.Drain()
	if pending == nil {
		return fuse.OK
	}
	_, st := b.streamingWrite(h, pending.Data, pending.Offset, uuid.NewString())
	return st
}

// Release closes the open file. Cancels lifeCtx first so any in-flight
// prefetch goroutine bails out, WAITS for those goroutines to finish (they
// hold the server fd and write into the readahead ring — returning early
// would let `gmountie unmount` report success while the mount is still
// active), then drains the coalescer best-effort, then issues the
// server-side Release RPC. Always proceeds to the server-side Release even
// if drain fails.
func (b *BackendClient) Release(ctx context.Context, fh FileHandle) fuse.Status {
	h := resolveHandle(fh)
	if h == nil {
		return fuse.EBADF
	}
	// Stop new prefetch spawns before waiting; see spawnPrefetch for the
	// Add/Wait discipline.
	h.prefetchMu.Lock()
	h.released = true
	h.prefetchMu.Unlock()
	if h.lifeCancel != nil {
		h.lifeCancel()
	}
	h.prefetchWG.Wait()
	if st := b.drainCoalescer(h); !st.Ok() {
		log.Log.Error("error draining coalescer on Release",
			zap.String("path", h.path), zap.Stringer("status", st))
	}
	// classFdOp: a duplicate Release of an already-closed fd is benign, so
	// transient errors retry within the session; a session change makes the fd
	// (and its server-side resources) gone already, so retryOp stops.
	_, err := retryOp(h.client, ctx, "Release", classFdOp, h.ioTimeout,
		func(ctx context.Context) (*proto.ReleaseReply, error) {
			return h.fileClient.Release(ctx, &proto.ReleaseRequest{
				Volume:    h.volume,
				Fd:        h.fd,
				SessionId: h.sessionID,
			}, grpc.WaitForReady(true))
		})
	if err != nil {
		log.Log.Error("error in call: Release", zap.String("path", h.path), zap.Error(err))
		return statusFromRPCError(err)
	}
	return fuse.OK
}

// Flush drains the coalescer in memory and fuses the pending write with
// the flush into a single WriteAndFlush RPC (one RTT instead of the
// previous two: streaming Write + Flush). If the handle is clean (nothing
// written since the last successful Flush) the RPC is skipped entirely.
//
// Idempotent — routed through retryOp (classFdOp) so transient gRPC failures
// (Unavailable, DeadlineExceeded) don't surface to userspace as EIO.
// Re-running WriteAndFlush with the same (fd, offset, data) is safe
// server-side. No request_id needed: the server-side flush is idempotent.
//
// reply.FinalAttr is returned by the server but unused at the client;
// FUSE FLUSH carries no attributes back to the kernel. Available for
// future cache integration.
func (b *BackendClient) Flush(ctx context.Context, fh FileHandle) fuse.Status {
	h := resolveHandle(fh)
	if h == nil {
		return fuse.EBADF
	}
	// Clean-handle fast path: nothing written since last flush, coalescer
	// empty by definition. Skip the RPC entirely.
	if !h.dirty.Load() {
		return fuse.OK
	}
	// Drain the coalescer in memory; pending may be nil (write went
	// directly to the wire via streamingWrite on overflow/big-write).
	var offset int64
	var data []byte
	if h.coalescer != nil {
		if pending := h.coalescer.Drain(); pending != nil {
			offset = pending.Offset
			data = pending.Data
		}
	}
	// classFdOp: WriteAndFlush is idempotent (same fd/offset/data replayed) so
	// transient errors retry within the session; a session change kills the fd
	// and stops the retry.
	res, err := retryOp(h.client, ctx, "WriteAndFlush", classFdOp, h.ioTimeout,
		func(ctx context.Context) (*proto.WriteAndFlushReply, error) {
			return h.fileClient.WriteAndFlush(ctx, &proto.WriteAndFlushRequest{
				Volume:    h.volume,
				Fd:        h.fd,
				SessionId: h.sessionID,
				Offset:    offset,
				Data:      data,
			}, grpc.WaitForReady(true))
		})
	if err != nil {
		log.Log.Error("error in call: WriteAndFlush", zap.String("path", h.path), zap.Error(err))
		return statusFromRPCError(err)
	}
	st := fuse.Status(res.Status)
	if st.Ok() {
		h.dirty.Store(false)
	}
	return st
}

// Fsync drains coalesced writes then issues the server-side Fsync RPC.
// Idempotent — routed through retryOp (classFdOp) for the same reason as Flush
// (a second Fsync against an already-synced fd is a no-op).
func (b *BackendClient) Fsync(ctx context.Context, fh FileHandle, flags int64) fuse.Status {
	h := resolveHandle(fh)
	if h == nil {
		return fuse.EBADF
	}
	if st := b.drainCoalescer(h); !st.Ok() {
		return st
	}
	// classFdOp: a second Fsync against an already-synced fd is a no-op, so
	// transient errors retry within the session; a session change kills the fd.
	res, err := retryOp(h.client, ctx, "Fsync", classFdOp, h.ioTimeout,
		func(ctx context.Context) (*proto.FsyncReply, error) {
			return h.fileClient.Fsync(ctx, &proto.FsyncRequest{
				Volume:    h.volume,
				Fd:        h.fd,
				Flags:     flags,
				SessionId: h.sessionID,
			}, grpc.WaitForReady(true))
		})
	if err != nil {
		log.Log.Error("error in call: Fsync", zap.String("path", h.path), zap.Error(err))
		return statusFromRPCError(err)
	}
	st := fuse.Status(res.Status)
	if st.Ok() {
		h.dirty.Store(false)
	}
	return st
}

// Allocate proxies fallocate(2) to the server. classFdOp: within a session a
// replayed fallocate with identical (fd, off, size, mode) is a no-op, so a
// transient error retries; on a session change the fd is dead and retryOp
// stops. No request_id stamp — same-args replay is naturally idempotent.
func (b *BackendClient) Allocate(ctx context.Context, fh FileHandle, off, size uint64, mode uint32) fuse.Status {
	h := resolveHandle(fh)
	if h == nil {
		return fuse.EBADF
	}
	res, err := retryOp(h.client, ctx, "Allocate", classFdOp, h.ioTimeout,
		func(ctx context.Context) (*proto.AllocateReply, error) {
			return h.fileClient.Allocate(ctx, &proto.AllocateRequest{
				Volume:    h.volume,
				Caller:    callerFromCtx(ctx),
				Fd:        h.fd,
				Path:      h.path,
				Off:       off,
				Size:      size,
				Mode:      mode,
				SessionId: h.sessionID,
			}, grpc.WaitForReady(true))
		})
	if err != nil {
		log.Log.Error("error in call: Allocate", zap.String("path", h.path), zap.Error(err))
		return statusFromRPCError(err)
	}
	return fuse.Status(res.Status)
}

// CopyFileRange asks the server to copy length bytes from fhIn@offIn to
// fhOut@offOut entirely server-side. Pending coalesced writes on BOTH
// handles are drained first (same reason Fsync drains: the server must
// see current bytes). Not routed through retryOp — a partially-applied copy
// that lost its reply must not be replayed (it returns a bytes-copied count),
// and the kernel reissues on a short copy anyway.
func (b *BackendClient) CopyFileRange(ctx context.Context, fhIn FileHandle, offIn uint64, fhOut FileHandle, offOut uint64, length, flags uint64) (uint64, fuse.Status) {
	src := resolveHandle(fhIn)
	dst := resolveHandle(fhOut)
	if src == nil || dst == nil {
		return 0, fuse.EBADF
	}
	if st := b.drainCoalescer(src); !st.Ok() {
		return 0, st
	}
	if st := b.drainCoalescer(dst); !st.Ok() {
		return 0, st
	}
	ctx2, cancel := ioCtx(ctx, dst.ioTimeout)
	defer cancel()
	res, err := dst.fileClient.CopyFileRange(ctx2, &proto.CopyFileRangeRequest{
		Volume:    dst.volume,
		Caller:    callerFromCtx(ctx),
		FdIn:      src.fd,
		PathIn:    src.path,
		OffIn:     offIn,
		FdOut:     dst.fd,
		PathOut:   dst.path,
		OffOut:    offOut,
		Length:    length,
		Flags:     flags,
		SessionId: dst.sessionID,
	})
	if err != nil {
		log.Log.Error("error in call: CopyFileRange", zap.String("path", dst.path), zap.Error(err))
		return 0, statusFromRPCError(err)
	}
	st := fuse.Status(res.Status)
	if st.Ok() && res.BytesCopied > 0 {
		dst.dirty.Store(true) // close() should still Flush the destination
	}
	return res.BytesCopied, st
}

// Lseek probes hole geometry on the server fd. Idempotent — retried.
// The coalescer is drained first so pending writes shape the answer.
func (b *BackendClient) Lseek(ctx context.Context, fh FileHandle, offset uint64, whence uint32) (uint64, fuse.Status) {
	h := resolveHandle(fh)
	if h == nil {
		return 0, fuse.EBADF
	}
	if st := b.drainCoalescer(h); !st.Ok() {
		return 0, st
	}
	// classFdOp: Lseek is a pure hole-geometry probe (idempotent), so transient
	// errors retry within the session; a session change kills the fd.
	res, err := retryOp(h.client, ctx, "Lseek", classFdOp, h.ioTimeout,
		func(ctx context.Context) (*proto.LseekReply, error) {
			return h.fileClient.Lseek(ctx, &proto.LseekRequest{
				Volume:    h.volume,
				Caller:    callerFromCtx(ctx),
				Fd:        h.fd,
				Path:      h.path,
				Offset:    offset,
				Whence:    whence,
				SessionId: h.sessionID,
			}, grpc.WaitForReady(true))
		})
	if err != nil {
		log.Log.Error("error in call: Lseek", zap.String("path", h.path), zap.Error(err))
		return 0, statusFromRPCError(err)
	}
	return res.Offset, fuse.Status(res.Status)
}

// GetLk queries the lock state for a region of the file. classFdOp: a lock-state
// query is a pure read, idempotent within the session, so a transient error
// retries; on a session change the fd (and its locks) are gone and retryOp stops
// — so a missed reply can't leave us reasoning about a phantom lock on a dead fd.
func (b *BackendClient) GetLk(ctx context.Context, fh FileHandle, owner uint64, lk *fuse.FileLock, flags uint32, out *fuse.FileLock) fuse.Status {
	h := resolveHandle(fh)
	if h == nil {
		return fuse.EBADF
	}
	res, err := retryOp(h.client, ctx, "GetLk", classFdOp, h.ioTimeout,
		func(ctx context.Context) (*proto.GetLkReply, error) {
			return h.fileClient.GetLk(ctx, &proto.GetLkRequest{
				Volume:    h.volume,
				Fd:        h.fd,
				Owner:     owner,
				Flags:     flags,
				Lk:        &proto.FileLock{Start: lk.Start, End: lk.End, Typ: lk.Typ, Pid: lk.Pid},
				SessionId: h.sessionID,
			}, grpc.WaitForReady(true))
		})
	if err != nil {
		log.Log.Error("error in call: GetLk", zap.String("path", h.path), zap.Error(err))
		return statusFromRPCError(err)
	}
	if res.Lk != nil {
		*out = fuse.FileLock{Start: res.Lk.Start, End: res.Lk.End, Typ: res.Lk.Typ, Pid: res.Lk.Pid}
	}
	return fuse.Status(res.Status)
}

// SetLk attempts a non-blocking lock acquisition (fcntl(F_SETLK)). classFdOp:
// re-applying the same (owner, region, type) lock on the same fd within the
// session is idempotent, so a transient error retries; a session change kills
// the fd (and any locks it held) and retryOp stops — see GetLk.
func (b *BackendClient) SetLk(ctx context.Context, fh FileHandle, owner uint64, lk *fuse.FileLock, flags uint32) fuse.Status {
	h := resolveHandle(fh)
	if h == nil {
		return fuse.EBADF
	}
	res, err := retryOp(h.client, ctx, "SetLk", classFdOp, h.ioTimeout,
		func(ctx context.Context) (*proto.SetLkReply, error) {
			return h.fileClient.SetLk(ctx, &proto.SetLkRequest{
				Volume:    h.volume,
				Fd:        h.fd,
				Owner:     owner,
				Flags:     flags,
				Lk:        &proto.FileLock{Start: lk.Start, End: lk.End, Typ: lk.Typ, Pid: lk.Pid},
				SessionId: h.sessionID,
			}, grpc.WaitForReady(true))
		})
	if err != nil {
		log.Log.Error("error in call: SetLk", zap.String("path", h.path), zap.Error(err))
		return statusFromRPCError(err)
	}
	return fuse.Status(res.Status)
}

// SetLkw attempts a blocking lock acquisition (fcntl(F_SETLKW)). Not routed
// through retryOp: the blocking wait must stay cancellable (see the inline note
// below) rather than be detached and retried.
func (b *BackendClient) SetLkw(ctx context.Context, fh FileHandle, owner uint64, lk *fuse.FileLock, flags uint32) fuse.Status {
	h := resolveHandle(fh)
	if h == nil {
		return fuse.EBADF
	}
	// SetLkw is a BLOCKING lock acquisition (F_SETLKW): keep it cancellable so a
	// signal can interrupt a stuck wait, rather than detaching it via ioCtx.
	ctx2, cancel := context.WithTimeout(ctx, h.ioTimeout)
	defer cancel()
	res, err := h.fileClient.SetLkw(ctx2, &proto.SetLkwRequest{
		Volume:    h.volume,
		Fd:        h.fd,
		Owner:     owner,
		Flags:     flags,
		Lk:        &proto.FileLock{Start: lk.Start, End: lk.End, Typ: lk.Typ, Pid: lk.Pid},
		SessionId: h.sessionID,
	})
	if err != nil {
		log.Log.Error("error in call: SetLkw", zap.String("path", h.path), zap.Error(err))
		return statusFromRPCError(err)
	}
	return fuse.Status(res.Status)
}

// grpcFileHandle is the per-fd state returned by Open/Create. It mirrors
// the legacy GrpcFile minus the nodefs.File embedding — the FUSE adapter
// for the new go-fuse v2 fs.* node interface lives in node.go (Task 3).
type grpcFileHandle struct {
	// client is the live gRPC client the fd was opened against. fd-ops route
	// their retryable unary calls through retryOp(h.client, ...): the handle
	// needs the live client (not just the proto.RpcFileClient) so retryOp can
	// read the current SessionID()/RetryWindow()/Lifetime() — the session-change
	// guard that makes classFdOp retries safe. fileClient is retained as a
	// convenience accessor (== client.File()).
	client     grpcclient.Client
	fileClient proto.RpcFileClient
	volume     string
	path       string
	fd         uint64
	ioTimeout  time.Duration
	sessionID  string
	// dirty tracks whether a Write has been accepted since the last
	// successful Flush or Fsync. Flush skips the RPC entirely when dirty is
	// false (clean-handle fast path). Set atomically by Write; cleared
	// atomically by a successful Flush or Fsync.
	dirty atomic.Bool
	// readahead is non-nil when readahead is enabled for this fd.
	readahead *Readahead
	// coalescer is non-nil when per-fd small-write coalescing is enabled.
	// coalesceThreshold mirrors the coalescer's threshold so the big-write
	// short-circuit doesn't need a Coalescer accessor.
	coalescer         *WriteCoalescer
	coalesceThreshold int
	// lifeCtx is cancelled by Release so any in-flight prefetch goroutine
	// returns promptly instead of holding the file open on the server
	// past the FUSE close.
	lifeCtx    context.Context
	lifeCancel context.CancelFunc
	// prefetchWG tracks in-flight prefetch goroutines so Release can wait
	// for them after cancelling lifeCtx. prefetchMu guards released, which
	// is flipped before Release waits so no Add can race the Wait.
	prefetchWG sync.WaitGroup
	prefetchMu sync.Mutex
	released   bool
}

// Path returns the path this handle was opened against.
func (h *grpcFileHandle) Path() string { return h.path }

// Unwrap returns the receiver: *grpcFileHandle is the leaf in any
// FileHandle decorator chain. resolveHandle relies on the self-unwrap
// invariant to terminate its walk.
func (h *grpcFileHandle) Unwrap() FileHandle { return h }

// resolveHandle walks the FileHandle.Unwrap chain looking for the
// *grpcFileHandle leaf. Returns nil if no such leaf exists in the chain
// (the per-fd op then fails fast with EBADF). The walk terminates when
// a handle returns itself from Unwrap (leaf marker).
func resolveHandle(fh FileHandle) *grpcFileHandle {
	cur := fh
	for cur != nil {
		if h, ok := cur.(*grpcFileHandle); ok {
			return h
		}
		next := cur.Unwrap()
		if next == cur {
			// Self-unwrap on a non-*grpcFileHandle: leaf with no gRPC
			// handle behind it. Nothing more we can do.
			return nil
		}
		cur = next
	}
	return nil
}

// newGrpcFileHandle constructs a grpcFileHandle bound to fd on the named
// volume. client is the live gRPC client (the handle derives fileClient from
// client.File() and routes fd-op retries through it). cfg bundles the per-file
// knobs: ReadaheadChunkBytes of 0
// disables the readahead path entirely; otherwise prefetches arm after
// ReadaheadThreshold strictly-sequential reads. WriteCoalesceBytes of 0
// disables per-fd write coalescing; otherwise small contiguous writes
// accumulate up to that threshold before flushing.
func newGrpcFileHandle(
	client grpcclient.Client,
	volume, path string,
	fd uint64,
	ioTimeout time.Duration,
	sessionID string,
	cfg grpcclient.PerFileConfig,
) *grpcFileHandle {
	ctx, cancel := context.WithCancel(context.Background())
	h := &grpcFileHandle{
		client:            client,
		fileClient:        client.File(),
		volume:            volume,
		path:              path,
		fd:                fd,
		ioTimeout:         ioTimeout,
		sessionID:         sessionID,
		coalesceThreshold: cfg.WriteCoalesceBytes,
		lifeCtx:           ctx,
		lifeCancel:        cancel,
	}
	if cfg.ReadaheadChunkBytes > 0 && cfg.ReadaheadThreshold > 0 {
		h.readahead = NewReadahead(cfg.ReadaheadChunkBytes, cfg.ReadaheadThreshold, cfg.ReadaheadWindow)
	}
	if cfg.WriteCoalesceBytes > 0 {
		h.coalescer = NewWriteCoalescer(cfg.WriteCoalesceBytes)
	}
	return h
}
