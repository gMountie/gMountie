package controller

import (
	"context"
	"gmountie/pkg/proto"
	"gmountie/pkg/server/service"

	"github.com/hanwen/go-fuse/v2/fuse"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type RpcFileServerImpl struct {
	fsService service.VolumeService
	sessions  service.SessionManager
	proto.UnimplementedRpcFileServer
}

var _ proto.RpcFileServer = (*RpcFileServerImpl)(nil)

func NewRpcFileServer(fsService service.VolumeService, sessions service.SessionManager) *RpcFileServerImpl {
	return &RpcFileServerImpl{fsService: fsService, sessions: sessions}
}

func (r *RpcFileServerImpl) Register(server *grpc.Server) {
	proto.RegisterRpcFileServer(server, r)
}

func (r *RpcFileServerImpl) resolveSession(sessionID string) (service.Session, error) {
	if sessionID == "" {
		return nil, status.Error(codes.InvalidArgument, "session_id is required")
	}
	sess, err := r.sessions.Get(sessionID)
	if err != nil {
		return nil, status.Errorf(codes.NotFound, "session not found: %s", sessionID)
	}
	return sess, nil
}

func (r *RpcFileServerImpl) Open(ctx context.Context, request *proto.OpenRequest) (*proto.OpenReply, error) {
	sess, err := r.resolveSession(request.SessionId)
	if err != nil {
		return nil, err
	}
	fs, err := r.fsService.GetVolumeFileSystem(request.Volume)
	if err != nil {
		return nil, err
	}
	file, s := fs.Open(request.Path, request.Flags, createContext(ctx, request.Caller))
	reply := &proto.OpenReply{Status: int32(s)}
	if s == fuse.OK {
		reply.Fd = sess.RegisterFile(request.Path, file)
	}
	return reply, nil
}

func (r *RpcFileServerImpl) Create(ctx context.Context, request *proto.CreateRequest) (*proto.CreateReply, error) {
	sess, err := r.resolveSession(request.SessionId)
	if err != nil {
		return nil, err
	}
	fs, err := r.fsService.GetVolumeFileSystem(request.Volume)
	if err != nil {
		return nil, err
	}
	file, s := fs.Create(request.Path, request.Flags, request.Mode, createContext(ctx, request.Caller))
	reply := &proto.CreateReply{Status: int32(s)}
	if s == fuse.OK {
		reply.Fd = sess.RegisterFile(request.Path, file)
	}
	return reply, nil
}

func (r *RpcFileServerImpl) Read(_ context.Context, request *proto.ReadRequest) (*proto.ReadReply, error) {
	sess, err := r.resolveSession(request.SessionId)
	if err != nil {
		return nil, err
	}
	entry, ok := sess.GetFile(request.Fd)
	if !ok {
		return nil, status.Errorf(codes.NotFound, "fd %d not found in session", request.Fd)
	}
	buf := make([]byte, request.Size)
	n, s := entry.File.Read(buf, request.Offset)
	if s != fuse.OK {
		return &proto.ReadReply{Status: int32(s)}, nil
	}
	buf, s = n.Bytes(buf)
	return &proto.ReadReply{Size: int64(n.Size()), Bytes: buf, Status: int32(s)}, nil
}

func (r *RpcFileServerImpl) Write(_ context.Context, request *proto.WriteRequest) (*proto.WriteReply, error) {
	sess, err := r.resolveSession(request.SessionId)
	if err != nil {
		return nil, err
	}
	entry, ok := sess.GetFile(request.Fd)
	if !ok {
		return nil, status.Errorf(codes.NotFound, "fd %d not found in session", request.Fd)
	}
	written, s := entry.File.Write(request.Bytes, request.Offset)
	return &proto.WriteReply{Written: written, Status: int32(s)}, nil
}

func (r *RpcFileServerImpl) Fsync(_ context.Context, request *proto.FsyncRequest) (*proto.FsyncReply, error) {
	sess, err := r.resolveSession(request.SessionId)
	if err != nil {
		return nil, err
	}
	entry, ok := sess.GetFile(request.Fd)
	if !ok {
		return nil, status.Errorf(codes.NotFound, "fd %d not found in session", request.Fd)
	}
	return &proto.FsyncReply{Status: int32(entry.File.Fsync(int(request.Flags)))}, nil
}

func (r *RpcFileServerImpl) Release(_ context.Context, request *proto.ReleaseRequest) (*proto.ReleaseReply, error) {
	sess, err := r.resolveSession(request.SessionId)
	if err != nil {
		return nil, err
	}
	sess.ReleaseFile(request.Fd)
	return &proto.ReleaseReply{}, nil
}

func (r *RpcFileServerImpl) Flush(_ context.Context, request *proto.FlushRequest) (*proto.FlushReply, error) {
	sess, err := r.resolveSession(request.SessionId)
	if err != nil {
		return nil, err
	}
	entry, ok := sess.GetFile(request.Fd)
	if !ok {
		return nil, status.Errorf(codes.NotFound, "fd %d not found in session", request.Fd)
	}
	return &proto.FlushReply{Status: int32(entry.File.Flush())}, nil
}

func (r *RpcFileServerImpl) GetLk(_ context.Context, request *proto.GetLkRequest) (*proto.GetLkReply, error) {
	sess, err := r.resolveSession(request.SessionId)
	if err != nil {
		return nil, err
	}
	entry, ok := sess.GetFile(request.Fd)
	if !ok {
		return nil, status.Errorf(codes.NotFound, "fd %d not found in session", request.Fd)
	}
	lock := &fuse.FileLock{Start: request.Lk.Start, End: request.Lk.End, Typ: request.Lk.Typ, Pid: request.Lk.Pid}
	out := &fuse.FileLock{}
	s := entry.File.GetLk(request.Owner, lock, request.Flags, out)
	return &proto.GetLkReply{
		Lk:     &proto.FileLock{Start: out.Start, End: out.End, Typ: out.Typ, Pid: out.Pid},
		Status: int32(s),
	}, nil
}

func (r *RpcFileServerImpl) SetLk(_ context.Context, request *proto.SetLkRequest) (*proto.SetLkReply, error) {
	sess, err := r.resolveSession(request.SessionId)
	if err != nil {
		return nil, err
	}
	entry, ok := sess.GetFile(request.Fd)
	if !ok {
		return nil, status.Errorf(codes.NotFound, "fd %d not found in session", request.Fd)
	}
	lock := &fuse.FileLock{Start: request.Lk.Start, End: request.Lk.End, Typ: request.Lk.Typ, Pid: request.Lk.Pid}
	return &proto.SetLkReply{Status: int32(entry.File.SetLk(request.Owner, lock, request.Flags))}, nil
}

func (r *RpcFileServerImpl) SetLkw(_ context.Context, request *proto.SetLkwRequest) (*proto.SetLkwReply, error) {
	sess, err := r.resolveSession(request.SessionId)
	if err != nil {
		return nil, err
	}
	entry, ok := sess.GetFile(request.Fd)
	if !ok {
		return nil, status.Errorf(codes.NotFound, "fd %d not found in session", request.Fd)
	}
	lock := &fuse.FileLock{Start: request.Lk.Start, End: request.Lk.End, Typ: request.Lk.Typ, Pid: request.Lk.Pid}
	return &proto.SetLkwReply{Status: int32(entry.File.SetLkw(request.Owner, lock, request.Flags))}, nil
}

func (r *RpcFileServerImpl) Allocate(_ context.Context, request *proto.AllocateRequest) (*proto.AllocateReply, error) {
	sess, err := r.resolveSession(request.SessionId)
	if err != nil {
		return nil, err
	}
	entry, ok := sess.GetFile(request.Fd)
	if !ok {
		return nil, status.Errorf(codes.NotFound, "fd %d not found in session", request.Fd)
	}
	return &proto.AllocateReply{Status: int32(entry.File.Allocate(request.Off, request.Size, request.Mode))}, nil
}
