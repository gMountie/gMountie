package controller

import (
	"context"
	"gmountie/pkg/proto"
	"gmountie/pkg/server/metrics"
	"gmountie/pkg/server/service"

	"github.com/hanwen/go-fuse/v2/fuse"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type RpcFileServerImpl struct {
	fsService service.VolumeService
	sessions  service.SessionManager
	metrics   *metrics.Metrics
	streamer  *service.ReadStreamer
	proto.UnimplementedRpcFileServer
}

var _ proto.RpcFileServer = (*RpcFileServerImpl)(nil)

// NewRpcFileServer wires the file controller. When m is nil a fresh,
// unregistered *Metrics is substituted so callers (e.g. unit tests)
// don't need to plumb one through. frameSize bounds each ReadFrame
// emitted by the streaming Read handler.
func NewRpcFileServer(fsService service.VolumeService, sessions service.SessionManager, m *metrics.Metrics, frameSize int) *RpcFileServerImpl {
	if m == nil {
		m = metrics.NewMetrics()
	}
	return &RpcFileServerImpl{
		fsService: fsService,
		sessions:  sessions,
		metrics:   m,
		streamer:  service.NewReadStreamer(frameSize),
	}
}

func (r *RpcFileServerImpl) Register(server *grpc.Server) {
	proto.RegisterRpcFileServer(server, r)
}

func (r *RpcFileServerImpl) Open(ctx context.Context, request *proto.OpenRequest) (*proto.OpenReply, error) {
	sess, err := resolveSession(r.sessions, request.SessionId)
	if err != nil {
		return nil, err
	}
	fs, err := r.fsService.GetVolumeFileSystem(request.Volume)
	if err != nil {
		return nil, err
	}
	return withIdempotency(sess, request.RequestId, func() (*proto.OpenReply, error) {
		file, s := fs.Open(request.Path, request.Flags, createContext(ctx, request.Caller))
		reply := &proto.OpenReply{Status: int32(s)}
		if s == fuse.OK {
			reply.Fd = sess.RegisterFile(request.Path, file)
			r.metrics.OpenFilesInc(request.Volume, request.SessionId)
		}
		return reply, nil
	})
}

func (r *RpcFileServerImpl) Create(ctx context.Context, request *proto.CreateRequest) (*proto.CreateReply, error) {
	sess, err := resolveSession(r.sessions, request.SessionId)
	if err != nil {
		return nil, err
	}
	fs, err := r.fsService.GetVolumeFileSystem(request.Volume)
	if err != nil {
		return nil, err
	}
	return withIdempotency(sess, request.RequestId, func() (*proto.CreateReply, error) {
		file, s := fs.Create(request.Path, request.Flags, request.Mode, createContext(ctx, request.Caller))
		reply := &proto.CreateReply{Status: int32(s)}
		if s == fuse.OK {
			reply.Fd = sess.RegisterFile(request.Path, file)
			r.metrics.OpenFilesInc(request.Volume, request.SessionId)
		}
		return reply, nil
	})
}

// Read is server-streaming: the server emits one ReadFrame per chunk of at
// most ServerConfig.FrameSizeBytes, followed by a terminal frame whose
// Status carries the final FUSE result (typically OK or an errno). The
// streaming loop lives in service.ReadStreamer; this handler is a thin
// resolve+delegate.
func (r *RpcFileServerImpl) Read(request *proto.ReadRequest, stream proto.RpcFile_ReadServer) error {
	sess, err := resolveSession(r.sessions, request.SessionId)
	if err != nil {
		return err
	}
	entry, ok := sess.GetFile(request.Fd)
	if !ok {
		// Surface bad-fd as a terminal status frame rather than a transport
		// error so the client gets a clean errno through the FUSE layer.
		return stream.Send(&proto.ReadFrame{Status: int32(fuse.EBADF)})
	}

	fileRead := func(buf []byte, off int64) (int, fuse.Status) {
		res, st := entry.File.Read(buf, off)
		if !st.Ok() {
			return 0, st
		}
		out, st := res.Bytes(buf)
		if !st.Ok() {
			return 0, st
		}
		// ReadResult.Bytes may return a slice that does not alias buf; copy
		// into buf so the streamer's `buf[:n]` slicing remains correct.
		n := copy(buf, out)
		return n, fuse.OK
	}
	emit := func(data []byte, st fuse.Status) error {
		if len(data) > 0 {
			r.metrics.BytesAdd(request.Volume, "out", float64(len(data)))
		}
		return stream.Send(&proto.ReadFrame{Data: data, Status: int32(st)})
	}
	return r.streamer.Stream(stream.Context(), int(request.Size), request.Offset, fileRead, emit)
}

func (r *RpcFileServerImpl) Write(_ context.Context, request *proto.WriteRequest) (*proto.WriteReply, error) {
	sess, err := resolveSession(r.sessions, request.SessionId)
	if err != nil {
		return nil, err
	}
	return withIdempotency(sess, request.RequestId, func() (*proto.WriteReply, error) {
		entry, ok := sess.GetFile(request.Fd)
		if !ok {
			return nil, status.Errorf(codes.NotFound, "fd %d not found in session", request.Fd)
		}
		written, s := entry.File.Write(request.Bytes, request.Offset)
		if s == fuse.OK {
			r.metrics.BytesAdd(request.Volume, "in", float64(written))
		}
		return &proto.WriteReply{Written: written, Status: int32(s)}, nil
	})
}

func (r *RpcFileServerImpl) Fsync(_ context.Context, request *proto.FsyncRequest) (*proto.FsyncReply, error) {
	sess, err := resolveSession(r.sessions, request.SessionId)
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
	sess, err := resolveSession(r.sessions, request.SessionId)
	if err != nil {
		return nil, err
	}
	sess.ReleaseFile(request.Fd)
	r.metrics.OpenFilesDec(request.Volume, request.SessionId)
	return &proto.ReleaseReply{}, nil
}

func (r *RpcFileServerImpl) Flush(_ context.Context, request *proto.FlushRequest) (*proto.FlushReply, error) {
	sess, err := resolveSession(r.sessions, request.SessionId)
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
	sess, err := resolveSession(r.sessions, request.SessionId)
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
	sess, err := resolveSession(r.sessions, request.SessionId)
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
	sess, err := resolveSession(r.sessions, request.SessionId)
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
	sess, err := resolveSession(r.sessions, request.SessionId)
	if err != nil {
		return nil, err
	}
	entry, ok := sess.GetFile(request.Fd)
	if !ok {
		return nil, status.Errorf(codes.NotFound, "fd %d not found in session", request.Fd)
	}
	return &proto.AllocateReply{Status: int32(entry.File.Allocate(request.Off, request.Size, request.Mode))}, nil
}
