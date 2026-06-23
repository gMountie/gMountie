package controller

import (
	"context"
	"strings"
	"syscall"
	"time"

	fserr "go.gmountie.dev/gmountie/pkg/common/fserr"
	"go.gmountie.dev/gmountie/pkg/proto"
	"go.gmountie.dev/gmountie/pkg/server/delegation"
	serverio "go.gmountie.dev/gmountie/pkg/server/io"
	"go.gmountie.dev/gmountie/pkg/server/metrics"
	"go.gmountie.dev/gmountie/pkg/server/service"

	"github.com/hanwen/go-fuse/v2/fuse"
	"github.com/hanwen/go-fuse/v2/fuse/pathfs"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type RpcServerImpl struct {
	fsService service.VolumeService
	sessions  service.SessionManager
	bus       serverio.EventBus
	metrics   *metrics.Metrics
	arbiter   *delegation.Arbiter
	recalls   *delegation.RecallRegistry
	proto.UnimplementedRpcFsServer
}

// Verify that RpcServerImpl implements proto.RpcFsServer
var _ proto.RpcFsServer = (*RpcServerImpl)(nil)

// NewGrpcServer creates a new gRPC server.
// m may be nil; subscribe metrics are no-ops when unset.
// arbiter and recalls may be nil for tests that do not exercise delegation.
func NewGrpcServer(fsService service.VolumeService, sessions service.SessionManager, bus serverio.EventBus, m *metrics.Metrics, arbiter *delegation.Arbiter, recalls *delegation.RecallRegistry) *RpcServerImpl {
	return &RpcServerImpl{
		fsService: fsService,
		sessions:  sessions,
		bus:       bus,
		metrics:   m,
		arbiter:   arbiter,
		recalls:   recalls,
	}
}

// Register registers the gRPC server
func (r *RpcServerImpl) Register(server *grpc.Server) {
	proto.RegisterRpcFsServer(server, r)
}

func (r *RpcServerImpl) GetAttr(ctx context.Context, request *proto.GetAttrRequest) (*proto.GetAttrReply, error) {
	fs, id, err := r.fsService.BindIdentity(ctx, request.Volume, CallerFromProto(request.Caller))
	if err != nil {
		return nil, err
	}
	attr, st := fs.GetAttr(request.Path, createContext(ctx, request.Caller))
	if attr == nil {
		return &proto.GetAttrReply{
			Status: fserr.FromErrno(syscall.Errno(st)),
		}, nil
	}
	reply := &proto.GetAttrReply{
		Attributes: toProtoAttr(attr, &id),
		Status:     fserr.FromErrno(syscall.Errno(st)),
	}
	return reply, nil
}

// Mkdir creates the directory and, on success, returns its attrs in the reply
// (create-style, like Create/SetAttr) so the client needs no trailing GetAttr.
func (r *RpcServerImpl) Mkdir(ctx context.Context, request *proto.MkdirRequest) (*proto.MkdirReply, error) {
	sess, err := resolveSession(ctx, r.sessions, request.SessionId)
	if err != nil {
		return nil, err
	}
	fs, id, err := r.fsService.BindIdentity(ctx, request.Volume, CallerFromProto(request.Caller))
	if err != nil {
		return nil, err
	}
	return withIdempotency(sess, request.RequestId, func() (*proto.MkdirReply, error) {
		if st := arbitrateContention(r.arbiter, request.SessionId, request.Path); st != proto.FsError_FS_OK {
			return &proto.MkdirReply{Status: st}, nil
		}
		res := r.applyPathOp(ctx, fs, &id, request.Volume, PathOp{
			Kind:   opMkdir,
			Path:   request.Path,
			Mode:   request.Mode,
			Caller: request.Caller,
		})
		if res.Status != fuse.OK {
			return &proto.MkdirReply{Status: applyOpProtoStatus(res.Status)}, nil
		}
		// ONE trailing stat serves both the reply attrs and the event seed —
		// see statAttrsAndEmit for the stat-fail partial-success contract.
		return &proto.MkdirReply{
			Status:     proto.FsError_FS_OK,
			Attributes: res.Attr,
			Grant:      grantFor(r.arbiter, request.SessionId, request.Delegation),
		}, nil
	})
}

func (r *RpcServerImpl) Rmdir(ctx context.Context, request *proto.RmdirRequest) (*proto.RmdirReply, error) {
	sess, err := resolveSession(ctx, r.sessions, request.SessionId)
	if err != nil {
		return nil, err
	}
	fs, _, err := r.fsService.BindIdentity(ctx, request.Volume, CallerFromProto(request.Caller))
	if err != nil {
		return nil, err
	}
	return withIdempotency(sess, request.RequestId, func() (*proto.RmdirReply, error) {
		if st := arbitrateContention(r.arbiter, request.SessionId, request.Path); st != proto.FsError_FS_OK {
			return &proto.RmdirReply{Status: st}, nil
		}
		res := r.applyPathOp(ctx, fs, nil, request.Volume, PathOp{
			Kind:   opRmdir,
			Path:   request.Path,
			Caller: request.Caller,
		})
		reply := &proto.RmdirReply{Status: applyOpProtoStatus(res.Status)}
		if res.Status == fuse.OK {
			reply.Grant = grantFor(r.arbiter, request.SessionId, request.Delegation)
		}
		return reply, nil
	})
}

func (r *RpcServerImpl) Rename(ctx context.Context, request *proto.RenameRequest) (*proto.RenameReply, error) {
	sess, err := resolveSession(ctx, r.sessions, request.SessionId)
	if err != nil {
		return nil, err
	}
	fs, _, err := r.fsService.BindIdentity(ctx, request.Volume, CallerFromProto(request.Caller))
	if err != nil {
		return nil, err
	}
	return withIdempotency(sess, request.RequestId, func() (*proto.RenameReply, error) {
		// Cross-subtree rename may contend two delegations — arbitrate both.
		if st := arbitrateContention(r.arbiter, request.SessionId, request.OldName); st != proto.FsError_FS_OK {
			return &proto.RenameReply{Status: st}, nil
		}
		if st := arbitrateContention(r.arbiter, request.SessionId, request.NewName); st != proto.FsError_FS_OK {
			return &proto.RenameReply{Status: st}, nil
		}
		res := r.applyPathOp(ctx, fs, nil, request.Volume, PathOp{
			Kind:    opRename,
			OldName: request.OldName,
			NewName: request.NewName,
			Caller:  request.Caller,
		})
		reply := &proto.RenameReply{Status: applyOpProtoStatus(res.Status)}
		if res.Status == fuse.OK {
			reply.Grant = grantFor(r.arbiter, request.SessionId, request.Delegation)
		}
		return reply, nil
	})
}

func (r *RpcServerImpl) Readlink(ctx context.Context, request *proto.ReadlinkRequest) (*proto.ReadlinkReply, error) {
	// Read-only path op, identity-bound like Access. Returns the link target
	// verbatim — confinement is enforced when something tries to FOLLOW it,
	// not when reading the link string itself.
	fs, _, err := r.fsService.BindIdentity(ctx, request.Volume, CallerFromProto(request.Caller))
	if err != nil {
		return nil, err
	}
	target, st := fs.Readlink(request.Path, createContext(ctx, request.Caller))
	return &proto.ReadlinkReply{Target: target, Status: fserr.FromErrno(syscall.Errno(st))}, nil
}

// Symlink creates the link and, on success, returns the LINK's own attrs in
// the reply (lstat semantics — the confined FS does not follow the target),
// same create-style contract and partial-failure rules as Mkdir above.
func (r *RpcServerImpl) Symlink(ctx context.Context, request *proto.SymlinkRequest) (*proto.SymlinkReply, error) {
	sess, err := resolveSession(ctx, r.sessions, request.SessionId)
	if err != nil {
		return nil, err
	}
	fs, id, err := r.fsService.BindIdentity(ctx, request.Volume, CallerFromProto(request.Caller))
	if err != nil {
		return nil, err
	}
	return withIdempotency(sess, request.RequestId, func() (*proto.SymlinkReply, error) {
		if st := arbitrateContention(r.arbiter, request.SessionId, request.LinkPath); st != proto.FsError_FS_OK {
			return &proto.SymlinkReply{Status: st}, nil
		}
		res := r.applyPathOp(ctx, fs, &id, request.Volume, PathOp{
			Kind:   opSymlink,
			Path:   request.LinkPath,
			Target: request.Target,
			Caller: request.Caller,
		})
		if res.Status != fuse.OK {
			return &proto.SymlinkReply{Status: applyOpProtoStatus(res.Status)}, nil
		}
		// ONE trailing stat for both reply attrs and event seed — see
		// statAttrsAndEmit for the stat-fail partial-success contract.
		return &proto.SymlinkReply{
			Status:     proto.FsError_FS_OK,
			Attributes: res.Attr,
			Grant:      grantFor(r.arbiter, request.SessionId, request.Delegation),
		}, nil
	})
}

// readDirBatchSize bounds a single ReadDirBatch message. 512 entries keeps a
// worst-case batch (long names + full attrs) comfortably under the default
// 4 MiB gRPC message limit while large directories still need few messages.
const readDirBatchSize = 512

// ReadDir streams a directory listing in batches, optionally with per-entry
// attributes (READDIRPLUS). Read-only — no session/request_id, mirroring
// GetAttr. The fuse status travels in-band: on a filesystem failure the
// stream carries ONE terminal batch with the status and the RPC itself
// succeeds; only bind errors surface as gRPC errors (matching GetAttr).
func (r *RpcServerImpl) ReadDir(req *proto.ReadDirRequest, stream proto.RpcFs_ReadDirServer) error {
	fs, id, err := r.fsService.BindIdentity(stream.Context(), req.Volume, CallerFromProto(req.Caller))
	if err != nil {
		return err
	}
	fctx := createContext(stream.Context(), req.Caller)
	var entries []serverio.DirEntryPlus
	var st fuse.Status
	// ReadDirPlus is a project-owned extension interface — pathfs.FileSystem
	// is go-fuse's, so the capability is type-asserted with a graceful
	// fallback to plain OpenDir + nil attrs.
	if rdp, ok := fs.(serverio.ReadDirPlusser); ok && req.Plus {
		entries, st = rdp.ReadDirPlus(req.Path, req.WithXattr, fctx)
	} else {
		var dirs []fuse.DirEntry
		dirs, st = fs.OpenDir(req.Path, fctx)
		if st.Ok() {
			entries = make([]serverio.DirEntryPlus, len(dirs))
			for i, d := range dirs {
				entries[i] = serverio.DirEntryPlus{Entry: d}
			}
		}
	}
	if !st.Ok() {
		return stream.Send(&proto.ReadDirBatch{Status: fserr.FromErrno(syscall.Errno(st))})
	}
	// Stream batches. An EMPTY directory still sends one empty OK batch so
	// the client can distinguish success-empty from a stream that produced
	// nothing.
	for first := true; first || len(entries) > 0; first = false {
		n := min(len(entries), readDirBatchSize)
		batch := &proto.ReadDirBatch{Entries: make([]*proto.DirEntryPlus, n)}
		for i, e := range entries[:n] {
			batch.Entries[i] = &proto.DirEntryPlus{
				Entry: &proto.DirEntry{
					Mode: e.Entry.Mode,
					Name: e.Entry.Name,
					Ino:  e.Entry.Ino,
					Off:  e.Entry.Off,
				},
				Attributes:  toProtoAttr(e.Attr, &id), // nil Attr -> nil Attributes
				XattrListed: e.XattrListed,
				XattrNames:  e.XattrNames,
			}
		}
		if err := stream.Send(batch); err != nil {
			return err
		}
		entries = entries[n:]
	}
	return nil
}

func (r *RpcServerImpl) StatFs(ctx context.Context, request *proto.StatFsRequest) (*proto.StatFsReply, error) {
	// StatFsRequest carries no Caller — statvfs is filesystem-wide and
	// identity-agnostic. Bind under the volume's default identity (nil caller).
	fs, _, err := r.fsService.BindIdentity(ctx, request.Volume, nil)
	if err != nil {
		return nil, err
	}
	statfs := fs.StatFs(request.Path)
	if statfs == nil {
		return nil, status.Errorf(codes.NotFound, "statfs: filesystem returned no data for path %q", request.Path)
	}
	reply := &proto.StatFsReply{
		Blocks:  statfs.Blocks,
		Bfree:   statfs.Bfree,
		Bavail:  statfs.Bavail,
		Files:   statfs.Files,
		Ffree:   statfs.Ffree,
		Bsize:   statfs.Bsize,
		Namelen: statfs.NameLen,
		Frsize:  statfs.Frsize,
	}
	return reply, nil
}

func (r *RpcServerImpl) Unlink(ctx context.Context, request *proto.UnlinkRequest) (*proto.UnlinkReply, error) {
	sess, err := resolveSession(ctx, r.sessions, request.SessionId)
	if err != nil {
		return nil, err
	}
	fs, _, err := r.fsService.BindIdentity(ctx, request.Volume, CallerFromProto(request.Caller))
	if err != nil {
		return nil, err
	}
	return withIdempotency(sess, request.RequestId, func() (*proto.UnlinkReply, error) {
		if st := arbitrateContention(r.arbiter, request.SessionId, request.Path); st != proto.FsError_FS_OK {
			return &proto.UnlinkReply{Status: st}, nil
		}
		res := r.applyPathOp(ctx, fs, nil, request.Volume, PathOp{
			Kind:   opUnlink,
			Path:   request.Path,
			Caller: request.Caller,
		})
		reply := &proto.UnlinkReply{Status: applyOpProtoStatus(res.Status)}
		if res.Status == fuse.OK {
			reply.Grant = grantFor(r.arbiter, request.SessionId, request.Delegation)
		}
		return reply, nil
	})
}

func (r *RpcServerImpl) Access(ctx context.Context, request *proto.AccessRequest) (*proto.AccessReply, error) {
	fs, _, err := r.fsService.BindIdentity(ctx, request.Volume, CallerFromProto(request.Caller))
	if err != nil {
		return nil, err
	}
	st := fs.Access(request.Path, request.Mode, createContext(ctx, request.Caller))
	return &proto.AccessReply{Status: fserr.FromErrno(syscall.Errno(st))}, nil
}

// fileTimeToTime maps a wire FileTime to a Go time pointer. A nil input yields
// nil (UTIME_OMIT — leave that timestamp unchanged).
func fileTimeToTime(ft *proto.FileTime) *time.Time {
	if ft == nil {
		return nil
	}
	t := time.Unix(int64(ft.Sec), int64(ft.Nsec))
	return &t
}

// SetAttr applies any combination of settable attributes in ONE RPC,
// replacing the removed per-field Truncate/Chmod/Chown/Utimens
// round-trips. request.Valid mirrors FUSE
// FATTR_* — the wire contract pins the exact values go-fuse exports
// (1=MODE 2=UID 4=GID 8=SIZE 16=ATIME 32=MTIME), so the fuse constants are
// used directly. Fields apply size→mode→owner→times; the first non-OK status
// aborts the rest and travels in-band in Status.
// On success the reply carries the final attrs (populated when the trailing
// stat succeeds, nil otherwise; Status stays OK either way), so the client
// needs no trailing GetAttr — and that same stat seeds the mutation event.
func (r *RpcServerImpl) SetAttr(ctx context.Context, request *proto.SetAttrRequest) (*proto.SetAttrReply, error) {
	sess, err := resolveSession(ctx, r.sessions, request.SessionId)
	if err != nil {
		return nil, err
	}
	fs, id, err := r.fsService.BindIdentity(ctx, request.Volume, CallerFromProto(request.Caller))
	if err != nil {
		return nil, err
	}
	return withIdempotency(sess, request.RequestId, func() (*proto.SetAttrReply, error) {
		if st := arbitrateContention(r.arbiter, request.SessionId, request.Path); st != proto.FsError_FS_OK {
			return &proto.SetAttrReply{Status: st}, nil
		}
		res := r.applySetAttrOp(ctx, fs, &id, request.Volume, request)
		if res.Status != fuse.OK {
			return &proto.SetAttrReply{Status: applyOpProtoStatus(res.Status)}, nil
		}
		return &proto.SetAttrReply{
			Status:     proto.FsError_FS_OK,
			Attributes: res.Attr,
			Grant:      grantFor(r.arbiter, request.SessionId, request.Delegation),
		}, nil
	})
}

// applySetAttr applies the attribute changes selected by request.Valid in the
// wire-contract order size→mode→owner→times, stopping at the first non-OK
// status. mutated reports whether any step succeeded before returning, so a
// partial failure can still invalidate client caches.
func applySetAttr(fs pathfs.FileSystem, request *proto.SetAttrRequest, fctx *fuse.Context) (st fuse.Status, mutated bool) {
	if request.Valid&fuse.FATTR_SIZE != 0 {
		if s := fs.Truncate(request.Path, request.Size, fctx); s != fuse.OK {
			return s, mutated
		}
		mutated = true
	}
	if request.Valid&fuse.FATTR_MODE != 0 {
		if s := fs.Chmod(request.Path, request.Mode, fctx); s != fuse.OK {
			return s, mutated
		}
		mutated = true
	}
	if request.Valid&(fuse.FATTR_UID|fuse.FATTR_GID) != 0 {
		uid, gid := request.Uid, request.Gid
		// pathfs.Chown has no "leave unchanged" sentinel, so when only one
		// half is requested the other half is read back via GetAttr instead
		// of clobbering it with the request's zero value.
		if request.Valid&fuse.FATTR_UID == 0 || request.Valid&fuse.FATTR_GID == 0 {
			attr, s := fs.GetAttr(request.Path, fctx)
			if !s.Ok() {
				return s, mutated
			}
			if attr == nil {
				return fuse.EIO, mutated
			}
			if request.Valid&fuse.FATTR_UID == 0 {
				uid = attr.Uid
			}
			if request.Valid&fuse.FATTR_GID == 0 {
				gid = attr.Gid
			}
		}
		if s := fs.Chown(request.Path, uid, gid, fctx); s != fuse.OK {
			return s, mutated
		}
		mutated = true
	}
	if request.Valid&(fuse.FATTR_ATIME|fuse.FATTR_MTIME) != 0 {
		// Only timestamps whose valid bit is set are touched; the rest pass
		// as nil (UTIME_OMIT), matching fileTimeToTime's nil contract.
		var atime, mtime *time.Time
		if request.Valid&fuse.FATTR_ATIME != 0 {
			atime = fileTimeToTime(request.Atime)
		}
		if request.Valid&fuse.FATTR_MTIME != 0 {
			mtime = fileTimeToTime(request.Mtime)
		}
		if s := fs.Utimens(request.Path, atime, mtime, fctx); s != fuse.OK {
			return s, mutated
		}
		mutated = true
	}
	return fuse.OK, mutated
}

// ----- Extended attributes -----

// xattrWriteAllowed reports whether attr may be WRITTEN through the wire
// protocol. The server process may run privileged and setfsuid does not
// drop capabilities, so a client must not be able to plant trusted.* or
// security.* (e.g. security.capability — file capabilities) xattrs on
// server-side files. Only namespaces a regular user could safely own are
// writable: user.* and the POSIX ACL pair. Reads stay unfiltered.
func xattrWriteAllowed(attr string) bool {
	return strings.HasPrefix(attr, "user.") ||
		attr == "system.posix_acl_access" ||
		attr == "system.posix_acl_default"
}

func (r *RpcServerImpl) GetXAttr(ctx context.Context, request *proto.GetXAttrRequest) (*proto.GetXAttrReply, error) {
	fs, _, err := r.fsService.BindIdentity(ctx, request.Volume, CallerFromProto(request.Caller))
	if err != nil {
		return nil, err
	}
	data, st := fs.GetXAttr(request.Path, request.Attribute, createContext(ctx, request.Caller))
	return &proto.GetXAttrReply{Data: data, Status: fserr.FromErrno(syscall.Errno(st))}, nil
}

func (r *RpcServerImpl) SetXAttr(ctx context.Context, request *proto.SetXAttrRequest) (*proto.SetXAttrReply, error) {
	sess, err := resolveSession(ctx, r.sessions, request.SessionId)
	if err != nil {
		return nil, err
	}
	if !xattrWriteAllowed(request.Attribute) {
		return &proto.SetXAttrReply{Status: proto.FsError_FS_EPERM}, nil
	}
	fs, _, err := r.fsService.BindIdentity(ctx, request.Volume, CallerFromProto(request.Caller))
	if err != nil {
		return nil, err
	}
	return withIdempotency(sess, request.RequestId, func() (*proto.SetXAttrReply, error) {
		if st := arbitrateContention(r.arbiter, request.SessionId, request.Path); st != proto.FsError_FS_OK {
			return &proto.SetXAttrReply{Status: st}, nil
		}
		res := r.applyPathOp(ctx, fs, nil, request.Volume, PathOp{
			Kind:   opSetXAttr,
			Path:   request.Path,
			XAttr:  request.Attribute,
			XData:  request.Data,
			XFlags: int(request.Flags),
			Caller: request.Caller,
		})
		reply := &proto.SetXAttrReply{Status: applyOpProtoStatus(res.Status)}
		if res.Status == fuse.OK {
			reply.Grant = grantFor(r.arbiter, request.SessionId, request.Delegation)
		}
		return reply, nil
	})
}

func (r *RpcServerImpl) RemoveXAttr(ctx context.Context, request *proto.RemoveXAttrRequest) (*proto.RemoveXAttrReply, error) {
	sess, err := resolveSession(ctx, r.sessions, request.SessionId)
	if err != nil {
		return nil, err
	}
	if !xattrWriteAllowed(request.Attribute) {
		return &proto.RemoveXAttrReply{Status: proto.FsError_FS_EPERM}, nil
	}
	fs, _, err := r.fsService.BindIdentity(ctx, request.Volume, CallerFromProto(request.Caller))
	if err != nil {
		return nil, err
	}
	return withIdempotency(sess, request.RequestId, func() (*proto.RemoveXAttrReply, error) {
		if st := arbitrateContention(r.arbiter, request.SessionId, request.Path); st != proto.FsError_FS_OK {
			return &proto.RemoveXAttrReply{Status: st}, nil
		}
		res := r.applyPathOp(ctx, fs, nil, request.Volume, PathOp{
			Kind:   opRemoveXAttr,
			Path:   request.Path,
			XAttr:  request.Attribute,
			Caller: request.Caller,
		})
		reply := &proto.RemoveXAttrReply{Status: applyOpProtoStatus(res.Status)}
		if res.Status == fuse.OK {
			reply.Grant = grantFor(r.arbiter, request.SessionId, request.Delegation)
		}
		return reply, nil
	})
}

// ListXAttr is read-only and identity-bound like GetXAttr — no session or
// idempotency machinery.
func (r *RpcServerImpl) ListXAttr(ctx context.Context, request *proto.ListXAttrRequest) (*proto.ListXAttrReply, error) {
	fs, _, err := r.fsService.BindIdentity(ctx, request.Volume, CallerFromProto(request.Caller))
	if err != nil {
		return nil, err
	}
	attrs, st := fs.ListXAttr(request.Path, createContext(ctx, request.Caller))
	return &proto.ListXAttrReply{Attributes: attrs, Status: fserr.FromErrno(syscall.Errno(st))}, nil
}

func (r *RpcServerImpl) GetAttrIfChanged(ctx context.Context, request *proto.GetAttrIfChangedRequest) (*proto.GetAttrIfChangedReply, error) {
	// Identity-bound like GetAttr: this serves file attrs to the client, so it
	// must enforce the principal's permissions — otherwise a client could skip
	// the identity-checked GetAttr and read metadata by polling here. The wire
	// Caller is honored in passthrough mode (so the cache revalidation runs as
	// the real uid/gid instead of anon); mapped modes use the ctx principal
	// and ignore Caller.
	fs, id, err := r.fsService.BindIdentity(ctx, request.Volume, CallerFromProto(request.Caller))
	if err != nil {
		return nil, err
	}
	attr, st := fs.GetAttr(request.Path, createContext(ctx, request.Caller))
	if !st.Ok() || attr == nil {
		if st == fuse.ENOENT {
			return nil, status.Error(codes.NotFound, "path not found")
		}
		return nil, status.Errorf(codes.Internal, "stat failed: %d", st)
	}
	v := serverio.VersionFromAttr(attr)
	if v == request.KnownVersion {
		return &proto.GetAttrIfChangedReply{NotModified: true}, nil
	}
	return &proto.GetAttrIfChangedReply{
		NotModified: false,
		Attrs:       toProtoAttr(attr, &id),
	}, nil
}
