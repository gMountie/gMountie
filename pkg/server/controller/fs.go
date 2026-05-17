package controller

import (
	"context"
	"gmountie/pkg/proto"
	serverio "gmountie/pkg/server/io"
	"gmountie/pkg/server/metrics"
	"gmountie/pkg/server/service"

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

// versionAfter returns the freshness token for path after a successful
// mutation, suitable for Emit. Returns 0 if Stat fails — the event still
// fires; the client falls back to GetAttrIfChanged.
func (r *RpcServerImpl) versionAfter(ctx context.Context, fs pathfs.FileSystem, path string, caller *proto.Caller) uint64 {
	attr, st := fs.GetAttr(path, createContext(ctx, caller))
	if !st.Ok() || attr == nil {
		return 0
	}
	return serverio.VersionFromAttr(attr)
}

// Register registers the gRPC server
func (r *RpcServerImpl) Register(server *grpc.Server) {
	proto.RegisterRpcFsServer(server, r)
}

func (r *RpcServerImpl) GetAttr(ctx context.Context, request *proto.GetAttrRequest) (*proto.GetAttrReply, error) {
	fs, err := r.fsService.GetVolumeFileSystem(request.Volume)
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
		Attributes: &proto.Attr{
			Ino:       attr.Ino,
			Size:      attr.Size,
			Blocks:    attr.Blocks,
			Atime:     attr.Atime,
			Mtime:     attr.Mtime,
			Ctime:     attr.Ctime,
			Atimensec: attr.Atimensec,
			Mtimensec: attr.Mtimensec,
			Ctimensec: attr.Ctimensec,
			Mode:      attr.Mode,
			Nlink:     attr.Nlink,
			Owner:     &proto.Owner{Uid: attr.Owner.Uid, Gid: attr.Owner.Gid},
			Rdev:      attr.Rdev,
			Blksize:   attr.Blksize,
			Padding:   attr.Padding,
			Version:   serverio.VersionFromAttr(attr),
		},
		Status: int32(status),
	}
	return reply, nil
}

func (r *RpcServerImpl) Mkdir(ctx context.Context, request *proto.MkdirRequest) (*proto.MkdirReply, error) {
	sess, err := resolveSession(r.sessions, request.SessionId)
	if err != nil {
		return nil, err
	}
	fs, err := r.fsService.GetVolumeFileSystem(request.Volume)
	if err != nil {
		return nil, err
	}
	return withIdempotency(sess, request.RequestId, func() (*proto.MkdirReply, error) {
		s := fs.Mkdir(request.Path, request.Mode, createContext(ctx, request.Caller))
		if s == fuse.OK {
			r.bus.Emit(request.Volume, request.Path, r.versionAfter(ctx, fs, request.Path, request.Caller), serverio.KindMutated)
		}
		return &proto.MkdirReply{Status: int32(s)}, nil
	})
}

func (r *RpcServerImpl) Rmdir(ctx context.Context, request *proto.RmdirRequest) (*proto.RmdirReply, error) {
	sess, err := resolveSession(r.sessions, request.SessionId)
	if err != nil {
		return nil, err
	}
	fs, err := r.fsService.GetVolumeFileSystem(request.Volume)
	if err != nil {
		return nil, err
	}
	return withIdempotency(sess, request.RequestId, func() (*proto.RmdirReply, error) {
		s := fs.Rmdir(request.Path, createContext(ctx, request.Caller))
		if s == fuse.OK {
			r.bus.Emit(request.Volume, request.Path, 0, serverio.KindDeleted)
		}
		return &proto.RmdirReply{Status: int32(s)}, nil
	})
}

func (r *RpcServerImpl) Rename(ctx context.Context, request *proto.RenameRequest) (*proto.RenameReply, error) {
	sess, err := resolveSession(r.sessions, request.SessionId)
	if err != nil {
		return nil, err
	}
	fs, err := r.fsService.GetVolumeFileSystem(request.Volume)
	if err != nil {
		return nil, err
	}
	return withIdempotency(sess, request.RequestId, func() (*proto.RenameReply, error) {
		s := fs.Rename(request.OldName, request.NewName, createContext(ctx, request.Caller))
		if s == fuse.OK {
			r.bus.EmitRename(request.Volume, request.OldName, request.NewName,
				r.versionAfter(ctx, fs, request.NewName, request.Caller))
		}
		return &proto.RenameReply{Status: int32(s)}, nil
	})
}

func (r *RpcServerImpl) OpenDir(ctx context.Context, request *proto.OpenDirRequest) (*proto.OpenDirReply, error) {
	fs, err := r.fsService.GetVolumeFileSystem(request.Volume)
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
	fs, err := r.fsService.GetVolumeFileSystem(request.Volume)
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
	sess, err := resolveSession(r.sessions, request.SessionId)
	if err != nil {
		return nil, err
	}
	fs, err := r.fsService.GetVolumeFileSystem(request.Volume)
	if err != nil {
		return nil, err
	}
	return withIdempotency(sess, request.RequestId, func() (*proto.UnlinkReply, error) {
		s := fs.Unlink(request.Path, createContext(ctx, request.Caller))
		if s == fuse.OK {
			r.bus.Emit(request.Volume, request.Path, 0, serverio.KindDeleted)
		}
		return &proto.UnlinkReply{Status: int32(s)}, nil
	})
}

func (r *RpcServerImpl) Access(ctx context.Context, request *proto.AccessRequest) (*proto.AccessReply, error) {
	fs, err := r.fsService.GetVolumeFileSystem(request.Volume)
	if err != nil {
		return nil, err
	}
	status := fs.Access(request.Path, request.Mode, createContext(ctx, request.Caller))
	return &proto.AccessReply{Status: int32(status)}, nil
}

func (r *RpcServerImpl) Truncate(ctx context.Context, request *proto.TruncateRequest) (*proto.TruncateReply, error) {
	sess, err := resolveSession(r.sessions, request.SessionId)
	if err != nil {
		return nil, err
	}
	fs, err := r.fsService.GetVolumeFileSystem(request.Volume)
	if err != nil {
		return nil, err
	}
	return withIdempotency(sess, request.RequestId, func() (*proto.TruncateReply, error) {
		s := fs.Truncate(request.Path, request.Size, createContext(ctx, request.Caller))
		if s == fuse.OK {
			r.bus.Emit(request.Volume, request.Path, r.versionAfter(ctx, fs, request.Path, request.Caller), serverio.KindMutated)
		}
		return &proto.TruncateReply{Status: int32(s)}, nil
	})
}

func (r *RpcServerImpl) Chmod(ctx context.Context, request *proto.ChmodRequest) (*proto.ChmodReply, error) {
	sess, err := resolveSession(r.sessions, request.SessionId)
	if err != nil {
		return nil, err
	}
	fs, err := r.fsService.GetVolumeFileSystem(request.Volume)
	if err != nil {
		return nil, err
	}
	return withIdempotency(sess, request.RequestId, func() (*proto.ChmodReply, error) {
		s := fs.Chmod(request.Path, request.Mode, createContext(ctx, request.Caller))
		if s == fuse.OK {
			r.bus.Emit(request.Volume, request.Path, r.versionAfter(ctx, fs, request.Path, request.Caller), serverio.KindMutated)
		}
		return &proto.ChmodReply{Status: int32(s)}, nil
	})
}

func (r *RpcServerImpl) Chown(ctx context.Context, request *proto.ChownRequest) (*proto.ChownReply, error) {
	sess, err := resolveSession(r.sessions, request.SessionId)
	if err != nil {
		return nil, err
	}
	fs, err := r.fsService.GetVolumeFileSystem(request.Volume)
	if err != nil {
		return nil, err
	}
	return withIdempotency(sess, request.RequestId, func() (*proto.ChownReply, error) {
		s := fs.Chown(request.Path, request.Uid, request.Gid, createContext(ctx, request.Caller))
		if s == fuse.OK {
			r.bus.Emit(request.Volume, request.Path, r.versionAfter(ctx, fs, request.Path, request.Caller), serverio.KindMutated)
		}
		return &proto.ChownReply{Status: int32(s)}, nil
	})
}

// ----- Extended attributes -----

func (r *RpcServerImpl) GetXAttr(ctx context.Context, request *proto.GetXAttrRequest) (*proto.GetXAttrReply, error) {
	fs, err := r.fsService.GetVolumeFileSystem(request.Volume)
	if err != nil {
		return nil, err
	}
	data, status := fs.GetXAttr(request.Path, request.Attribute, createContext(ctx, request.Caller))
	return &proto.GetXAttrReply{Data: data, Status: int32(status)}, nil
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
	fs, err := r.fsService.GetVolumeFileSystem(request.Volume)
	if err != nil {
		return nil, err
	}
	attr, st := fs.GetAttr(request.Path, createContext(ctx, nil))
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
		Attrs: &proto.Attr{
			Ino: attr.Ino, Size: attr.Size, Blocks: attr.Blocks,
			Atime: attr.Atime, Mtime: attr.Mtime, Ctime: attr.Ctime,
			Atimensec: attr.Atimensec, Mtimensec: attr.Mtimensec, Ctimensec: attr.Ctimensec,
			Mode: attr.Mode, Nlink: attr.Nlink,
			Owner:   &proto.Owner{Uid: attr.Owner.Uid, Gid: attr.Owner.Gid},
			Rdev:    attr.Rdev, Blksize: attr.Blksize, Padding: attr.Padding,
			Version: v,
		},
	}, nil
}

