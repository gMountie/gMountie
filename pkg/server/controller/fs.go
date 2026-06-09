package controller

import (
	"context"
	"strings"
	"time"

	"go.gmountie.dev/gmountie/pkg/proto"
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
	compound  *service.CompoundDispatcher
	bus       serverio.EventBus
	metrics   *metrics.Metrics
	proto.UnimplementedRpcFsServer
}

// Verify that RpcServerImpl implements proto.RpcFsServer
var _ proto.RpcFsServer = (*RpcServerImpl)(nil)

// NewGrpcServer creates a new gRPC server. The CompoundDispatcher is built
// here so it can reference the RpcServerImpl as its FsHandlers — avoids
// exposing a post-construction setter just for the back-reference.
// m may be nil; subscribe metrics are no-ops when unset.
func NewGrpcServer(fsService service.VolumeService, sessions service.SessionManager, compoundMaxParallel int, bus serverio.EventBus, m *metrics.Metrics) *RpcServerImpl {
	srv := &RpcServerImpl{
		fsService: fsService,
		sessions:  sessions,
		bus:       bus,
		metrics:   m,
	}
	srv.compound = service.NewCompoundDispatcher(srv, compoundMaxParallel)
	return srv
}

// Register registers the gRPC server
func (r *RpcServerImpl) Register(server *grpc.Server) {
	proto.RegisterRpcFsServer(server, r)
}

func (r *RpcServerImpl) GetAttr(ctx context.Context, request *proto.GetAttrRequest) (*proto.GetAttrReply, error) {
	fs, id, err := r.fsService.BindIdentity(ctx, request.Volume, request.Caller)
	if err != nil {
		return nil, err
	}
	attr, status := fs.GetAttr(request.Path, createContext(ctx, request.Caller))
	if attr == nil {
		return &proto.GetAttrReply{
			Status: int32(status),
		}, nil
	}
	reply := &proto.GetAttrReply{
		Attributes: toProtoAttr(attr, &id),
		Status:     int32(status),
	}
	return reply, nil
}

func (r *RpcServerImpl) Mkdir(ctx context.Context, request *proto.MkdirRequest) (*proto.MkdirReply, error) {
	sess, err := resolveSession(ctx, r.sessions, request.SessionId)
	if err != nil {
		return nil, err
	}
	fs, _, err := r.fsService.BindIdentity(ctx, request.Volume, request.Caller)
	if err != nil {
		return nil, err
	}
	return withIdempotency(sess, request.RequestId, func() (*proto.MkdirReply, error) {
		s := r.mutateEmit(ctx, fs, request.Volume, request.Path, request.Caller, func() fuse.Status {
			return fs.Mkdir(request.Path, request.Mode, createContext(ctx, request.Caller))
		})
		return &proto.MkdirReply{Status: int32(s)}, nil
	})
}

func (r *RpcServerImpl) Rmdir(ctx context.Context, request *proto.RmdirRequest) (*proto.RmdirReply, error) {
	sess, err := resolveSession(ctx, r.sessions, request.SessionId)
	if err != nil {
		return nil, err
	}
	fs, _, err := r.fsService.BindIdentity(ctx, request.Volume, request.Caller)
	if err != nil {
		return nil, err
	}
	return withIdempotency(sess, request.RequestId, func() (*proto.RmdirReply, error) {
		s := r.deleteEmit(request.Volume, request.Path, func() fuse.Status {
			return fs.Rmdir(request.Path, createContext(ctx, request.Caller))
		})
		return &proto.RmdirReply{Status: int32(s)}, nil
	})
}

func (r *RpcServerImpl) Rename(ctx context.Context, request *proto.RenameRequest) (*proto.RenameReply, error) {
	sess, err := resolveSession(ctx, r.sessions, request.SessionId)
	if err != nil {
		return nil, err
	}
	fs, _, err := r.fsService.BindIdentity(ctx, request.Volume, request.Caller)
	if err != nil {
		return nil, err
	}
	return withIdempotency(sess, request.RequestId, func() (*proto.RenameReply, error) {
		s := r.renameEmit(ctx, fs, request.Volume, request.OldName, request.NewName, request.Caller, func() fuse.Status {
			return fs.Rename(request.OldName, request.NewName, createContext(ctx, request.Caller))
		})
		return &proto.RenameReply{Status: int32(s)}, nil
	})
}

func (r *RpcServerImpl) Readlink(ctx context.Context, request *proto.ReadlinkRequest) (*proto.ReadlinkReply, error) {
	// Read-only path op, identity-bound like Access. Returns the link target
	// verbatim — confinement is enforced when something tries to FOLLOW it,
	// not when reading the link string itself.
	fs, _, err := r.fsService.BindIdentity(ctx, request.Volume, request.Caller)
	if err != nil {
		return nil, err
	}
	target, st := fs.Readlink(request.Path, createContext(ctx, request.Caller))
	return &proto.ReadlinkReply{Target: target, Status: int32(st)}, nil
}

func (r *RpcServerImpl) Symlink(ctx context.Context, request *proto.SymlinkRequest) (*proto.SymlinkReply, error) {
	sess, err := resolveSession(ctx, r.sessions, request.SessionId)
	if err != nil {
		return nil, err
	}
	fs, _, err := r.fsService.BindIdentity(ctx, request.Volume, request.Caller)
	if err != nil {
		return nil, err
	}
	return withIdempotency(sess, request.RequestId, func() (*proto.SymlinkReply, error) {
		s := r.mutateEmit(ctx, fs, request.Volume, request.LinkPath, request.Caller, func() fuse.Status {
			return fs.Symlink(request.Target, request.LinkPath, createContext(ctx, request.Caller))
		})
		return &proto.SymlinkReply{Status: int32(s)}, nil
	})
}

func (r *RpcServerImpl) OpenDir(ctx context.Context, request *proto.OpenDirRequest) (*proto.OpenDirReply, error) {
	fs, _, err := r.fsService.BindIdentity(ctx, request.Volume, request.Caller)
	if err != nil {
		return nil, err
	}
	dirs, s := fs.OpenDir(request.Path, createContext(ctx, request.Caller))
	// convert to proto.DirEntry
	entries := make([]*proto.DirEntry, len(dirs))
	for i, dir := range dirs {
		entries[i] = &proto.DirEntry{
			Mode: dir.Mode,
			Name: dir.Name,
			Ino:  dir.Ino,
			Off:  dir.Off,
		}
	}
	reply := &proto.OpenDirReply{
		Entries: entries,
		Status:  int32(s),
	}
	return reply, nil
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
	fs, _, err := r.fsService.BindIdentity(ctx, request.Volume, request.Caller)
	if err != nil {
		return nil, err
	}
	return withIdempotency(sess, request.RequestId, func() (*proto.UnlinkReply, error) {
		s := r.deleteEmit(request.Volume, request.Path, func() fuse.Status {
			return fs.Unlink(request.Path, createContext(ctx, request.Caller))
		})
		return &proto.UnlinkReply{Status: int32(s)}, nil
	})
}

func (r *RpcServerImpl) Access(ctx context.Context, request *proto.AccessRequest) (*proto.AccessReply, error) {
	fs, _, err := r.fsService.BindIdentity(ctx, request.Volume, request.Caller)
	if err != nil {
		return nil, err
	}
	status := fs.Access(request.Path, request.Mode, createContext(ctx, request.Caller))
	return &proto.AccessReply{Status: int32(status)}, nil
}

func (r *RpcServerImpl) Truncate(ctx context.Context, request *proto.TruncateRequest) (*proto.TruncateReply, error) {
	sess, err := resolveSession(ctx, r.sessions, request.SessionId)
	if err != nil {
		return nil, err
	}
	fs, _, err := r.fsService.BindIdentity(ctx, request.Volume, request.Caller)
	if err != nil {
		return nil, err
	}
	return withIdempotency(sess, request.RequestId, func() (*proto.TruncateReply, error) {
		s := r.mutateEmit(ctx, fs, request.Volume, request.Path, request.Caller, func() fuse.Status {
			return fs.Truncate(request.Path, request.Size, createContext(ctx, request.Caller))
		})
		return &proto.TruncateReply{Status: int32(s)}, nil
	})
}

func (r *RpcServerImpl) Chmod(ctx context.Context, request *proto.ChmodRequest) (*proto.ChmodReply, error) {
	sess, err := resolveSession(ctx, r.sessions, request.SessionId)
	if err != nil {
		return nil, err
	}
	fs, _, err := r.fsService.BindIdentity(ctx, request.Volume, request.Caller)
	if err != nil {
		return nil, err
	}
	return withIdempotency(sess, request.RequestId, func() (*proto.ChmodReply, error) {
		s := r.mutateEmit(ctx, fs, request.Volume, request.Path, request.Caller, func() fuse.Status {
			return fs.Chmod(request.Path, request.Mode, createContext(ctx, request.Caller))
		})
		return &proto.ChmodReply{Status: int32(s)}, nil
	})
}

func (r *RpcServerImpl) Chown(ctx context.Context, request *proto.ChownRequest) (*proto.ChownReply, error) {
	sess, err := resolveSession(ctx, r.sessions, request.SessionId)
	if err != nil {
		return nil, err
	}
	fs, _, err := r.fsService.BindIdentity(ctx, request.Volume, request.Caller)
	if err != nil {
		return nil, err
	}
	return withIdempotency(sess, request.RequestId, func() (*proto.ChownReply, error) {
		s := r.mutateEmit(ctx, fs, request.Volume, request.Path, request.Caller, func() fuse.Status {
			return fs.Chown(request.Path, request.Uid, request.Gid, createContext(ctx, request.Caller))
		})
		return &proto.ChownReply{Status: int32(s)}, nil
	})
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

func (r *RpcServerImpl) Utimens(ctx context.Context, request *proto.UtimensRequest) (*proto.UtimensReply, error) {
	sess, err := resolveSession(ctx, r.sessions, request.SessionId)
	if err != nil {
		return nil, err
	}
	fs, _, err := r.fsService.BindIdentity(ctx, request.Volume, request.Caller)
	if err != nil {
		return nil, err
	}
	return withIdempotency(sess, request.RequestId, func() (*proto.UtimensReply, error) {
		atime := fileTimeToTime(request.Atime)
		mtime := fileTimeToTime(request.Mtime)
		s := r.mutateEmit(ctx, fs, request.Volume, request.Path, request.Caller, func() fuse.Status {
			return fs.Utimens(request.Path, atime, mtime, createContext(ctx, request.Caller))
		})
		return &proto.UtimensReply{Status: int32(s)}, nil
	})
}

// SetAttr applies any combination of settable attributes in ONE RPC,
// replacing the per-field Truncate/Chmod/Chown/Utimens round-trips (those
// handlers stay until the client migrates). request.Valid mirrors FUSE
// FATTR_* — the wire contract pins the exact values go-fuse exports
// (1=MODE 2=UID 4=GID 8=SIZE 16=ATIME 32=MTIME), so the fuse constants are
// used directly. Fields apply size→mode→owner→times; the first non-OK status
// aborts the rest and travels in-band in Status like the per-field handlers.
// On success the reply carries the final attrs, so the client needs no
// trailing GetAttr — and that same stat seeds the mutation event.
func (r *RpcServerImpl) SetAttr(ctx context.Context, request *proto.SetAttrRequest) (*proto.SetAttrReply, error) {
	sess, err := resolveSession(ctx, r.sessions, request.SessionId)
	if err != nil {
		return nil, err
	}
	fs, id, err := r.fsService.BindIdentity(ctx, request.Volume, request.Caller)
	if err != nil {
		return nil, err
	}
	return withIdempotency(sess, request.RequestId, func() (*proto.SetAttrReply, error) {
		fctx := createContext(ctx, request.Caller)
		s, mutated := applySetAttr(fs, request, fctx)
		if s != fuse.OK {
			// A failure mid-sequence may leave earlier fields applied; client
			// caches must still be invalidated. Version 0 (no fresh attr in
			// hand) makes them revalidate via GetAttrIfChanged.
			if mutated {
				r.emitMutatedAttr(request.Volume, request.Path, nil)
			}
			return &proto.SetAttrReply{Status: int32(s)}, nil
		}
		reply := &proto.SetAttrReply{Status: int32(fuse.OK)}
		// ONE trailing stat serves both the reply attrs and the event seed —
		// emitMutatedAttr deliberately replaces versionAfter, which re-stats.
		// mutated is false only for a degenerate request with no settable
		// bits: nothing changed, so no invalidation event is broadcast.
		if attr, gst := fs.GetAttr(request.Path, fctx); gst.Ok() {
			reply.Attributes = toProtoAttr(attr, &id)
			if mutated {
				r.emitMutatedAttr(request.Volume, request.Path, attr)
			}
		} else if mutated {
			r.emitMutatedAttr(request.Volume, request.Path, nil)
		}
		return reply, nil
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
		// as nil (UTIME_OMIT), matching the Utimens handler's nil contract.
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
	fs, _, err := r.fsService.BindIdentity(ctx, request.Volume, request.Caller)
	if err != nil {
		return nil, err
	}
	data, status := fs.GetXAttr(request.Path, request.Attribute, createContext(ctx, request.Caller))
	return &proto.GetXAttrReply{Data: data, Status: int32(status)}, nil
}

func (r *RpcServerImpl) SetXAttr(ctx context.Context, request *proto.SetXAttrRequest) (*proto.SetXAttrReply, error) {
	sess, err := resolveSession(ctx, r.sessions, request.SessionId)
	if err != nil {
		return nil, err
	}
	if !xattrWriteAllowed(request.Attribute) {
		return &proto.SetXAttrReply{Status: int32(fuse.EPERM)}, nil
	}
	fs, _, err := r.fsService.BindIdentity(ctx, request.Volume, request.Caller)
	if err != nil {
		return nil, err
	}
	return withIdempotency(sess, request.RequestId, func() (*proto.SetXAttrReply, error) {
		s := r.mutateEmit(ctx, fs, request.Volume, request.Path, request.Caller, func() fuse.Status {
			return fs.SetXAttr(request.Path, request.Attribute, request.Data, int(request.Flags), createContext(ctx, request.Caller))
		})
		return &proto.SetXAttrReply{Status: int32(s)}, nil
	})
}

func (r *RpcServerImpl) RemoveXAttr(ctx context.Context, request *proto.RemoveXAttrRequest) (*proto.RemoveXAttrReply, error) {
	sess, err := resolveSession(ctx, r.sessions, request.SessionId)
	if err != nil {
		return nil, err
	}
	if !xattrWriteAllowed(request.Attribute) {
		return &proto.RemoveXAttrReply{Status: int32(fuse.EPERM)}, nil
	}
	fs, _, err := r.fsService.BindIdentity(ctx, request.Volume, request.Caller)
	if err != nil {
		return nil, err
	}
	return withIdempotency(sess, request.RequestId, func() (*proto.RemoveXAttrReply, error) {
		s := r.mutateEmit(ctx, fs, request.Volume, request.Path, request.Caller, func() fuse.Status {
			return fs.RemoveXAttr(request.Path, request.Attribute, createContext(ctx, request.Caller))
		})
		return &proto.RemoveXAttrReply{Status: int32(s)}, nil
	})
}

// ListXAttr is read-only and identity-bound like GetXAttr — no session or
// idempotency machinery.
func (r *RpcServerImpl) ListXAttr(ctx context.Context, request *proto.ListXAttrRequest) (*proto.ListXAttrReply, error) {
	fs, _, err := r.fsService.BindIdentity(ctx, request.Volume, request.Caller)
	if err != nil {
		return nil, err
	}
	attrs, st := fs.ListXAttr(request.Path, createContext(ctx, request.Caller))
	return &proto.ListXAttrReply{Attributes: attrs, Status: int32(st)}, nil
}

// Compound runs a batched read-only metadata request via the
// CompoundDispatcher. Per-op errors are surfaced inside the reply slot — the
// outer RPC only returns a Go error if the dispatcher itself panics, which
// it doesn't. The handler is intentionally thin: all logic lives in the
// dispatcher.
func (r *RpcServerImpl) Compound(ctx context.Context, request *proto.CompoundRequest) (*proto.CompoundBatch, error) {
	replies := r.compound.Dispatch(ctx, request.GetOps())
	return &proto.CompoundBatch{Replies: replies}, nil
}

func (r *RpcServerImpl) GetAttrIfChanged(ctx context.Context, request *proto.GetAttrIfChangedRequest) (*proto.GetAttrIfChangedReply, error) {
	// Identity-bound like GetAttr: this serves file attrs to the client, so it
	// must enforce the principal's permissions — otherwise a client could skip
	// the identity-checked GetAttr and read metadata by polling here. The wire
	// Caller is honored in passthrough mode (so the cache revalidation runs as
	// the real uid/gid instead of anon); mapped modes use the ctx principal
	// and ignore Caller.
	fs, id, err := r.fsService.BindIdentity(ctx, request.Volume, request.Caller)
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
