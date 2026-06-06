package controller

import (
	"context"
	stdio "io"

	"go.gmountie.dev/gmountie/pkg/proto"
	serverio "go.gmountie.dev/gmountie/pkg/server/io"
	"go.gmountie.dev/gmountie/pkg/server/metrics"
	"go.gmountie.dev/gmountie/pkg/server/service"

	"github.com/hanwen/go-fuse/v2/fuse"
	"github.com/pkg/errors"
	"golang.org/x/sys/unix"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type RpcFileServerImpl struct {
	fsService service.VolumeService
	sessions  service.SessionManager
	metrics   *metrics.Metrics
	streamer  *service.ReadStreamer
	bus       serverio.EventBus
	proto.UnimplementedRpcFileServer
}

var _ proto.RpcFileServer = (*RpcFileServerImpl)(nil)

// NewRpcFileServer wires the file controller. When m is nil a fresh,
// unregistered *Metrics is substituted so callers (e.g. unit tests)
// don't need to plumb one through. frameSize bounds each ReadFrame
// emitted by the streaming Read handler.
func NewRpcFileServer(fsService service.VolumeService, sessions service.SessionManager, m *metrics.Metrics, frameSize int, bus serverio.EventBus) *RpcFileServerImpl {
	if m == nil {
		m = metrics.NewMetrics()
	}
	return &RpcFileServerImpl{
		fsService: fsService,
		sessions:  sessions,
		metrics:   m,
		streamer:  service.NewReadStreamer(frameSize),
		bus:       bus,
	}
}

func (r *RpcFileServerImpl) Register(server *grpc.Server) {
	proto.RegisterRpcFileServer(server, r)
}

// versionAfterPath re-Stats path on the named volume to seed an event with the
// fresh version token. Returns 0 if the volume or Stat fails — the event still
// fires; the client falls back to GetAttrIfChanged.
func (r *RpcFileServerImpl) versionAfterPath(ctx context.Context, volume, path string, caller *proto.Caller) uint64 {
	fs, err := r.fsService.GetVolumeFileSystem(volume)
	if err != nil {
		return 0
	}
	return versionAfter(ctx, fs, path, caller)
}

func (r *RpcFileServerImpl) Open(ctx context.Context, request *proto.OpenRequest) (*proto.OpenReply, error) {
	sess, err := resolveSession(ctx, r.sessions, request.SessionId)
	if err != nil {
		return nil, err
	}
	fs, _, err := r.fsService.BindIdentity(ctx, request.Volume, request.Caller)
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
	sess, err := resolveSession(ctx, r.sessions, request.SessionId)
	if err != nil {
		return nil, err
	}
	fs, id, err := r.fsService.BindIdentity(ctx, request.Volume, request.Caller)
	if err != nil {
		return nil, err
	}
	return withIdempotency(sess, request.RequestId, func() (*proto.CreateReply, error) {
		file, s := fs.Create(request.Path, request.Flags, request.Mode, createContext(ctx, request.Caller))
		reply := &proto.CreateReply{Status: int32(s)}
		if s == fuse.OK {
			reply.Fd = sess.RegisterFile(request.Path, file)
			r.metrics.OpenFilesInc(request.Volume, request.SessionId)
			if attr, gst := fs.GetAttr(request.Path, createContext(ctx, request.Caller)); gst.Ok() {
				reply.Attributes = toProtoAttr(attr, &id)
				r.bus.Emit(request.Volume, request.Path, serverio.VersionFromAttr(attr), serverio.KindMutated)
			} else {
				r.bus.Emit(request.Volume, request.Path, 0, serverio.KindMutated)
			}
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
	sess, err := resolveSession(stream.Context(), r.sessions, request.SessionId)
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
		// ReadResult.Bytes may return a slice that does not alias buf (e.g. an
		// in-memory FS handing back its own backing array); in that case copy
		// into buf so the streamer's `buf[:n]` slicing reads the right bytes.
		// The loopback FS we actually serve Preads straight into buf and
		// returns buf[:n], so out already aliases buf — copying there is a
		// self-overlapping memmove of the whole frame for no gain. Skip it.
		n := len(out)
		if n > 0 && len(buf) > 0 && &out[0] != &buf[0] {
			n = copy(buf, out)
		}
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

// Write is client-streaming: the client sends one or more WriteFrame messages
// and the server replies once after the stream closes. The FIRST frame carries
// the header (volume/fd/session_id/request_id/offset); subsequent frames carry
// only `data`. Idempotency is keyed on (session_id, request_id) from frame 1,
// so a retry of the entire stream short-circuits to the cached reply.
//
// The frame application loop lives in service.WriteFrameSink; this handler
// validates frame 1, consults the idempotency cache, and either drains or
// applies the stream accordingly.
func (r *RpcFileServerImpl) Write(stream proto.RpcFile_WriteServer) error {
	first, err := stream.Recv()
	if err != nil {
		if errors.Is(err, stdio.EOF) {
			return status.Error(codes.InvalidArgument, "Write: empty stream, no header frame")
		}
		return errors.Wrap(err, "Write: receive first frame")
	}
	if first.Volume == "" || first.Fd == 0 || first.SessionId == "" || first.RequestId == "" {
		return status.Error(codes.InvalidArgument, "Write: first frame must carry volume, fd, session_id and request_id")
	}

	sess, err := resolveSession(stream.Context(), r.sessions, first.SessionId)
	if err != nil {
		return err
	}

	// applied flips true inside DoOnce only on a cache miss. On a hit we must
	// drain the remainder of the stream before replying so gRPC sees a clean
	// half-close.
	applied := false
	var entryPath string
	raw, err := sess.DoOnce(first.RequestId, func() (any, error) {
		applied = true
		entry, ok := sess.GetFile(first.Fd)
		if !ok {
			return nil, status.Errorf(codes.NotFound, "fd %d not found in session", first.Fd)
		}
		entryPath = entry.Path
		return r.applyWriteStream(stream, first, entry.File)
	})
	if err != nil {
		return err
	}

	if !applied {
		if drainErr := drainWriteStream(stream); drainErr != nil {
			return drainErr
		}
	}

	reply, ok := raw.(*proto.WriteReply)
	if !ok {
		return status.Error(codes.Internal, "Write: idempotency cache: unexpected reply type")
	}

	if applied && fuse.Status(reply.Status) == fuse.OK && entryPath != "" {
		ver := r.versionAfterPath(stream.Context(), first.Volume, entryPath, nil)
		r.bus.Emit(first.Volume, entryPath, ver, serverio.KindMutated)
	}

	return stream.SendAndClose(reply)
}

// applyWriteStream applies frame 1's data, then iterates remaining frames from
// the stream until EOF or a non-OK FUSE status. Header fields on subsequent
// frames must be zero-valued or exactly match frame 1 (proto3 zero values are
// treated as "inherit"). The returned reply is what the idempotency cache
// stores under (session_id, request_id).
func (r *RpcFileServerImpl) applyWriteStream(stream proto.RpcFile_WriteServer, first *proto.WriteFrame, file service.FuseFileWriter) (*proto.WriteReply, error) {
	sink := service.NewWriteFrameSink(file, first.Offset)
	finalStatus := fuse.OK

	if firstN, st := sink.WriteFrame(first.Data); !st.Ok() {
		finalStatus = st
	} else {
		if firstN > 0 {
			r.metrics.BytesAdd(first.Volume, "in", float64(firstN))
		}
		for finalStatus.Ok() {
			frame, recvErr := stream.Recv()
			if errors.Is(recvErr, stdio.EOF) {
				break
			}
			if recvErr != nil {
				return nil, errors.Wrap(recvErr, "Write: receive frame")
			}
			if err := validateContinuationFrame(frame, first); err != nil {
				return nil, err
			}
			n, st := sink.WriteFrame(frame.Data)
			if !st.Ok() {
				finalStatus = st
				break
			}
			if n > 0 {
				r.metrics.BytesAdd(first.Volume, "in", float64(n))
			}
		}
	}

	// On a non-OK terminal status we still want to drain the remainder of the
	// stream so the client sees a clean half-close rather than RST_STREAM.
	if !finalStatus.Ok() {
		if drainErr := drainWriteStream(stream); drainErr != nil {
			return nil, drainErr
		}
	}

	return &proto.WriteReply{Written: sink.Total(), Status: int32(finalStatus)}, nil
}

// validateContinuationFrame enforces the "either zero or matches frame 1"
// invariant on header fields of subsequent frames. proto3 zero values mean
// "inherit from frame 1"; non-zero values must equal frame 1's value.
func validateContinuationFrame(frame, first *proto.WriteFrame) error {
	if frame.Volume != "" && frame.Volume != first.Volume {
		return status.Error(codes.InvalidArgument, "Write: continuation frame volume mismatch")
	}
	if frame.Fd != 0 && frame.Fd != first.Fd {
		return status.Error(codes.InvalidArgument, "Write: continuation frame fd mismatch")
	}
	if frame.SessionId != "" && frame.SessionId != first.SessionId {
		return status.Error(codes.InvalidArgument, "Write: continuation frame session_id mismatch")
	}
	if frame.RequestId != "" && frame.RequestId != first.RequestId {
		return status.Error(codes.InvalidArgument, "Write: continuation frame request_id mismatch")
	}
	if frame.Offset != 0 && frame.Offset != first.Offset {
		return status.Error(codes.InvalidArgument, "Write: continuation frame offset mismatch")
	}
	return nil
}

// drainWriteStream consumes remaining frames from stream until EOF. Used on
// idempotency-cache hits (no apply) and on early termination (FUSE error) so
// the transport sees a clean half-close.
func drainWriteStream(stream proto.RpcFile_WriteServer) error {
	for {
		_, err := stream.Recv()
		if errors.Is(err, stdio.EOF) {
			return nil
		}
		if err != nil {
			return errors.Wrap(err, "Write: drain stream")
		}
	}
}

func (r *RpcFileServerImpl) Fsync(ctx context.Context, request *proto.FsyncRequest) (*proto.FsyncReply, error) {
	sess, err := resolveSession(ctx, r.sessions, request.SessionId)
	if err != nil {
		return nil, err
	}
	entry, ok := sess.GetFile(request.Fd)
	if !ok {
		return nil, status.Errorf(codes.NotFound, "fd %d not found in session", request.Fd)
	}
	return &proto.FsyncReply{Status: int32(entry.File.Fsync(int(request.Flags)))}, nil
}

func (r *RpcFileServerImpl) Release(ctx context.Context, request *proto.ReleaseRequest) (*proto.ReleaseReply, error) {
	sess, err := resolveSession(ctx, r.sessions, request.SessionId)
	if err != nil {
		return nil, err
	}
	sess.ReleaseFile(request.Fd)
	r.metrics.OpenFilesDec(request.Volume, request.SessionId)
	return &proto.ReleaseReply{}, nil
}

func (r *RpcFileServerImpl) Flush(ctx context.Context, request *proto.FlushRequest) (*proto.FlushReply, error) {
	sess, err := resolveSession(ctx, r.sessions, request.SessionId)
	if err != nil {
		return nil, err
	}
	entry, ok := sess.GetFile(request.Fd)
	if !ok {
		return nil, status.Errorf(codes.NotFound, "fd %d not found in session", request.Fd)
	}
	return &proto.FlushReply{Status: int32(entry.File.Flush())}, nil
}

func (r *RpcFileServerImpl) GetLk(ctx context.Context, request *proto.GetLkRequest) (*proto.GetLkReply, error) {
	sess, err := resolveSession(ctx, r.sessions, request.SessionId)
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

func (r *RpcFileServerImpl) SetLk(ctx context.Context, request *proto.SetLkRequest) (*proto.SetLkReply, error) {
	sess, err := resolveSession(ctx, r.sessions, request.SessionId)
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

func (r *RpcFileServerImpl) SetLkw(ctx context.Context, request *proto.SetLkwRequest) (*proto.SetLkwReply, error) {
	sess, err := resolveSession(ctx, r.sessions, request.SessionId)
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

// WriteAndFlush writes data at offset (if any) then flushes the fd, in one RPC.
// A write error short-circuits the flush and is what close() will see. The
// reply carries the post-op Attr so the client needn't re-stat.
func (r *RpcFileServerImpl) WriteAndFlush(ctx context.Context, req *proto.WriteAndFlushRequest) (*proto.WriteAndFlushReply, error) {
	sess, err := resolveSession(ctx, r.sessions, req.SessionId)
	if err != nil {
		return nil, err
	}
	entry, ok := sess.GetFile(req.Fd)
	if !ok {
		// EBADF in-reply (not a transport error) keeps the errno visible at the FUSE layer.
		return &proto.WriteAndFlushReply{Status: int32(fuse.EBADF)}, nil
	}
	fs, err := r.fsService.GetVolumeFileSystem(req.Volume)
	if err != nil {
		return nil, err
	}
	reply := &proto.WriteAndFlushReply{}
	if len(req.Data) > 0 {
		n, st := entry.File.Write(req.Data, req.Offset)
		if st != fuse.OK {
			reply.Status = int32(st)
			return reply, nil // write error: skip flush, surface at close()
		}
		reply.Written = n
	}
	st := entry.File.Flush()
	reply.Status = int32(st)
	// final_attr is advisory and currently unused by the client (FUSE FLUSH
	// returns no attrs to the kernel). The stat uses a nil caller — fine while
	// it only feeds the bus version. A future client that consumes final_attr
	// for a caller-scoped view would need a Caller field on WriteAndFlushRequest.
	if attr, gst := fs.GetAttr(entry.Path, createContext(ctx, nil)); gst.Ok() {
		// WriteAndFlushRequest carries no Caller — pass nil here, same as the
		// stat above. final_attr is currently advisory; a future client that
		// consumes the names will need a Caller field on the request.
		reply.FinalAttr = toProtoAttr(attr, nil)
		if st == fuse.OK {
			r.bus.Emit(req.Volume, entry.Path, serverio.VersionFromAttr(attr), serverio.KindMutated)
		}
	} else if st == fuse.OK {
		r.bus.Emit(req.Volume, entry.Path, 0, serverio.KindMutated)
	}
	return reply, nil
}

func (r *RpcFileServerImpl) Allocate(ctx context.Context, request *proto.AllocateRequest) (*proto.AllocateReply, error) {
	sess, err := resolveSession(ctx, r.sessions, request.SessionId)
	if err != nil {
		return nil, err
	}
	entry, ok := sess.GetFile(request.Fd)
	if !ok {
		return nil, status.Errorf(codes.NotFound, "fd %d not found in session", request.Fd)
	}
	s := entry.File.Allocate(request.Off, request.Size, request.Mode)
	if s == fuse.OK {
		path := entry.Path
		if path == "" {
			path = request.Path
		}
		ver := r.versionAfterPath(ctx, request.Volume, path, request.Caller)
		r.bus.Emit(request.Volume, path, ver, serverio.KindMutated)
	}
	return &proto.AllocateReply{Status: int32(s)}, nil
}

// CopyFileRange copies length bytes between two open handles entirely on
// the server — the whole point of the RPC is that no file data crosses
// the wire. Errnos travel in-reply (WriteAndFlush convention): EBADF for a
// handle the session doesn't own, EINVAL for nonzero flags (per
// copy_file_range(2), flags must be 0). No identity re-bind: permission
// was checked at Open and the fds carry their access rights, same trust
// model as Read/Write/Allocate.
// Like every GetFile-then-use handler, this holds no reference on the
// entry: a concurrent session reap can Release the files mid-copy. The
// window here is wider than Read/Write (one RPC can loop over many
// chunks), accepted for now alongside the rest of the session layer.
func (r *RpcFileServerImpl) CopyFileRange(ctx context.Context, request *proto.CopyFileRangeRequest) (*proto.CopyFileRangeReply, error) {
	sess, err := resolveSession(ctx, r.sessions, request.SessionId)
	if err != nil {
		return nil, err
	}
	if request.Flags != 0 {
		return &proto.CopyFileRangeReply{Status: int32(fuse.EINVAL)}, nil
	}
	srcEntry, ok := sess.GetFile(request.FdIn)
	if !ok {
		return &proto.CopyFileRangeReply{Status: int32(fuse.EBADF)}, nil
	}
	dstEntry, ok := sess.GetFile(request.FdOut)
	if !ok {
		return &proto.CopyFileRangeReply{Status: int32(fuse.EBADF)}, nil
	}
	copied, st := serverio.CopyFileRange(srcEntry.File, dstEntry.File, request.OffIn, request.OffOut, request.Length)
	if st == fuse.OK && copied > 0 {
		path := dstEntry.Path
		if path == "" {
			path = request.PathOut
		}
		ver := r.versionAfterPath(ctx, request.Volume, path, request.Caller)
		r.bus.Emit(request.Volume, path, ver, serverio.KindMutated)
	}
	return &proto.CopyFileRangeReply{BytesCopied: copied, Status: int32(st)}, nil
}

// Lseek probes hole geometry on an open handle. Only SEEK_DATA/SEEK_HOLE
// are valid on the wire — the kernel resolves SET/CUR/END itself and
// never sends them through FUSE. Safe on the shared server fd: Read and
// Write are pread/pwrite-based (offset-less), and each lseek(2) call
// atomically returns its result.
func (r *RpcFileServerImpl) Lseek(ctx context.Context, request *proto.LseekRequest) (*proto.LseekReply, error) {
	sess, err := resolveSession(ctx, r.sessions, request.SessionId)
	if err != nil {
		return nil, err
	}
	if request.Whence != uint32(unix.SEEK_DATA) && request.Whence != uint32(unix.SEEK_HOLE) {
		return &proto.LseekReply{Status: int32(fuse.EINVAL)}, nil
	}
	entry, ok := sess.GetFile(request.Fd)
	if !ok {
		return &proto.LseekReply{Status: int32(fuse.EBADF)}, nil
	}
	off, st := serverio.Lseek(entry.File, request.Offset, request.Whence)
	return &proto.LseekReply{Offset: off, Status: int32(st)}, nil
}
