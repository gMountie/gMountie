package controller

import (
	"context"
	stdio "io"
	"syscall"

	fserr "go.gmountie.dev/gmountie/pkg/common/fserr"
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

// RpcFileServerImpl serves the fd-based RPC surface. Its handlers use three
// volume/file lookup seams with DIFFERENT trust levels — pick deliberately:
//
//   - VolumeService.BindIdentity (Open/Create): full trust path. Checks the
//     principal's ACL AND returns an identity-bound filesystem, so the op runs
//     with the caller's resolved credentials. REQUIRED whenever a path is
//     opened/created or attrs flow back to the client from a path lookup.
//
//   - sessionFile (all fd ops): authorization-by-possession. The fd was
//     authorized at Open/Create time under BindIdentity; sessionFile verifies
//     the caller owns the session AND that the fd belongs to request.Volume.
//     No re-bind: the fd carries its access rights (POSIX semantics).
//
//   - VolumeService.GetVolumeFileSystem (versionAfterPath only): RAW seam —
//     no ACL, no identity binding, runs as the server's own user. Safe ONLY
//     for the event-bus version probe, never for data returned to a client.
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
//
// Trust level: this uses the RAW GetVolumeFileSystem seam (no ACL check, no
// identity binding — the stat runs with the server's own credentials). That is
// only safe because the result feeds the event bus version token, never the
// caller's reply, and because every caller has already proven it owns an fd on
// the volume via sessionFile. Do NOT reuse this seam for data that flows back
// to the client; use VolumeService.BindIdentity for that (see MAINT-4 note on
// the handler seams below).
func (r *RpcFileServerImpl) versionAfterPath(ctx context.Context, volume, path string, caller *proto.Caller) uint64 {
	fs, err := r.fsService.GetVolumeFileSystem(volume)
	if err != nil {
		return 0
	}
	return versionAfter(ctx, fs, path, caller)
}

// emitMutatedFd emits the cache-invalidation event for a successful fd-based
// mutation (Write/Allocate/CopyFileRange), seeding the version with a fresh
// stat via versionAfterPath. Part of the controller-wide policy documented in
// emit.go: mutating handlers never call bus.Emit by hand. No subscribers →
// skip the stat and the emit entirely (see the PERF note in emit.go for the
// race semantics — this is the hottest gated site: every Write RPC lands
// here).
func (r *RpcFileServerImpl) emitMutatedFd(ctx context.Context, volume, path string, caller *proto.Caller) {
	if !r.bus.HasSubscribers(volume) {
		return
	}
	r.bus.Emit(volume, path, r.versionAfterPath(ctx, volume, path, caller), serverio.KindMutated)
}

// emitMutatedAttr is emitMutatedFd for handlers that already hold a fresh
// post-op attr (Create/WriteAndFlush stat the path for their replies anyway —
// re-statting would waste a syscall). A nil attr yields version 0; clients
// fall back to GetAttrIfChanged.
func (r *RpcFileServerImpl) emitMutatedAttr(volume, path string, attr *fuse.Attr) {
	if !r.bus.HasSubscribers(volume) {
		return
	}
	r.bus.Emit(volume, path, serverio.VersionFromAttr(attr), serverio.KindMutated)
}

// sessionFile looks up fd in sess and enforces the fd↔volume binding recorded
// at Open/Create: an fd is only usable with the volume it was opened on. A
// mismatched volume is treated exactly like an unknown fd, so a caller cannot
// probe whether an fd exists by guessing volumes — and, transitively, every
// fd-based handler's versionAfterPath/bus.Emit runs against the fd's true
// volume only (SEC-1).
//
// A successful return holds a ref on the entry (Acquire), pinning the
// underlying File open for the op's duration even if a session reap or
// Release RPC removes the fd concurrently. EVERY caller must pair it with
// entry.ReleaseRef() on all paths — a leaked ref keeps the file open forever.
func sessionFile(sess service.Session, fd uint64, volume string) (*service.FileEntry, bool) {
	entry, ok := sess.GetFile(fd)
	if !ok || entry.Volume != volume || !entry.Acquire() {
		return nil, false
	}
	return entry, true
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
		reply := &proto.OpenReply{Status: fserr.FromErrno(syscall.Errno(s))}
		if s == fuse.OK {
			reply.Fd = sess.RegisterFile(request.Volume, request.Path, file)
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
		reply := &proto.CreateReply{Status: fserr.FromErrno(syscall.Errno(s))}
		if s == fuse.OK {
			reply.Fd = sess.RegisterFile(request.Volume, request.Path, file)
			if attr, gst := fs.GetAttr(request.Path, createContext(ctx, request.Caller)); gst.Ok() {
				reply.Attributes = toProtoAttr(attr, &id)
				r.emitMutatedAttr(request.Volume, request.Path, attr)
			} else {
				r.emitMutatedAttr(request.Volume, request.Path, nil)
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
	entry, ok := sessionFile(sess, request.Fd, request.Volume)
	if !ok {
		// Surface bad-fd (or an fd↔volume mismatch) as a terminal status frame
		// rather than a transport error so the client gets a clean errno
		// through the FUSE layer.
		return stream.Send(&proto.ReadFrame{Status: proto.FsError_FS_EBADF})
	}
	// Held for the whole stream: fileRead closures below run until the
	// streamer finishes.
	defer entry.ReleaseRef()

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
		return stream.Send(&proto.ReadFrame{Data: data, Status: fserr.FromErrno(syscall.Errno(st))})
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
		entry, ok := sessionFile(sess, first.Fd, first.Volume)
		if !ok {
			return nil, status.Errorf(codes.NotFound, "fd %d not found in session", first.Fd)
		}
		// Held for the whole apply loop — released when the closure returns.
		defer entry.ReleaseRef()
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

	if applied && reply.Status == proto.FsError_FS_OK && entryPath != "" {
		r.emitMutatedFd(stream.Context(), first.Volume, entryPath, nil)
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

	return &proto.WriteReply{Written: sink.Total(), Status: fserr.FromErrno(syscall.Errno(finalStatus))}, nil
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
	entry, ok := sessionFile(sess, request.Fd, request.Volume)
	if !ok {
		return nil, status.Errorf(codes.NotFound, "fd %d not found in session", request.Fd)
	}
	defer entry.ReleaseRef()
	return &proto.FsyncReply{Status: fserr.FromErrno(syscall.Errno(entry.File.Fsync(int(request.Flags))))}, nil
}

func (r *RpcFileServerImpl) Release(ctx context.Context, request *proto.ReleaseRequest) (*proto.ReleaseReply, error) {
	sess, err := resolveSession(ctx, r.sessions, request.SessionId)
	if err != nil {
		return nil, err
	}
	// ReleaseFile no-ops (and does not move the open-files gauge) when the fd
	// is already gone, so a retried Release is harmless. The sessionFile guard
	// keeps a cross-volume Release from touching another volume's fd.
	//
	// Ref math: sessionFile acquired one ref on top of the table's (refs=2);
	// ReleaseFile drops the table ref (refs=1); our ReleaseRef drops the last
	// one and performs the actual File.Release — unless another op is still
	// in flight on the fd, in which case that op's ReleaseRef closes it.
	if entry, ok := sessionFile(sess, request.Fd, request.Volume); ok {
		sess.ReleaseFile(request.Fd)
		entry.ReleaseRef()
	}
	return &proto.ReleaseReply{}, nil
}

func (r *RpcFileServerImpl) Flush(ctx context.Context, request *proto.FlushRequest) (*proto.FlushReply, error) {
	sess, err := resolveSession(ctx, r.sessions, request.SessionId)
	if err != nil {
		return nil, err
	}
	entry, ok := sessionFile(sess, request.Fd, request.Volume)
	if !ok {
		return nil, status.Errorf(codes.NotFound, "fd %d not found in session", request.Fd)
	}
	defer entry.ReleaseRef()
	return &proto.FlushReply{Status: fserr.FromErrno(syscall.Errno(entry.File.Flush()))}, nil
}

func (r *RpcFileServerImpl) GetLk(ctx context.Context, request *proto.GetLkRequest) (*proto.GetLkReply, error) {
	sess, err := resolveSession(ctx, r.sessions, request.SessionId)
	if err != nil {
		return nil, err
	}
	entry, ok := sessionFile(sess, request.Fd, request.Volume)
	if !ok {
		return nil, status.Errorf(codes.NotFound, "fd %d not found in session", request.Fd)
	}
	defer entry.ReleaseRef()
	lock := &fuse.FileLock{Start: request.Lk.Start, End: request.Lk.End, Typ: request.Lk.Typ, Pid: request.Lk.Pid}
	out := &fuse.FileLock{}
	s := entry.File.GetLk(request.Owner, lock, request.Flags, out)
	return &proto.GetLkReply{
		Lk:     &proto.FileLock{Start: out.Start, End: out.End, Typ: out.Typ, Pid: out.Pid},
		Status: fserr.FromErrno(syscall.Errno(s)),
	}, nil
}

func (r *RpcFileServerImpl) SetLk(ctx context.Context, request *proto.SetLkRequest) (*proto.SetLkReply, error) {
	sess, err := resolveSession(ctx, r.sessions, request.SessionId)
	if err != nil {
		return nil, err
	}
	entry, ok := sessionFile(sess, request.Fd, request.Volume)
	if !ok {
		return nil, status.Errorf(codes.NotFound, "fd %d not found in session", request.Fd)
	}
	defer entry.ReleaseRef()
	lock := &fuse.FileLock{Start: request.Lk.Start, End: request.Lk.End, Typ: request.Lk.Typ, Pid: request.Lk.Pid}
	return &proto.SetLkReply{Status: fserr.FromErrno(syscall.Errno(entry.File.SetLk(request.Owner, lock, request.Flags)))}, nil
}

func (r *RpcFileServerImpl) SetLkw(ctx context.Context, request *proto.SetLkwRequest) (*proto.SetLkwReply, error) {
	sess, err := resolveSession(ctx, r.sessions, request.SessionId)
	if err != nil {
		return nil, err
	}
	entry, ok := sessionFile(sess, request.Fd, request.Volume)
	if !ok {
		return nil, status.Errorf(codes.NotFound, "fd %d not found in session", request.Fd)
	}
	defer entry.ReleaseRef()
	lock := &fuse.FileLock{Start: request.Lk.Start, End: request.Lk.End, Typ: request.Lk.Typ, Pid: request.Lk.Pid}
	return &proto.SetLkwReply{Status: fserr.FromErrno(syscall.Errno(entry.File.SetLkw(request.Owner, lock, request.Flags)))}, nil
}

// WriteAndFlush writes data at offset (if any) then flushes the fd, in one RPC.
// A write error short-circuits the flush and is what close() will see. The
// reply carries the post-op Attr so the client needn't re-stat.
//
// Idempotency: a non-empty request_id dedups via the session cache so a
// retried RPC replays the cached reply (non-OK in-band statuses included —
// Write's convention: only transport-level errors skip the cache). request_id
// is OPTIONAL here, unlike the path-mutation RPCs: pure-flush calls carry
// none, and withOptionalIdempotency runs those directly without touching the
// cache. The fd ref is acquired INSIDE the closure (same shape as Write):
// a cache-hit replay must not touch the fd table at all — after a
// reconnect retry the fd may be long released.
func (r *RpcFileServerImpl) WriteAndFlush(ctx context.Context, req *proto.WriteAndFlushRequest) (*proto.WriteAndFlushReply, error) {
	sess, err := resolveSession(ctx, r.sessions, req.SessionId)
	if err != nil {
		return nil, err
	}
	return withOptionalIdempotency(sess, req.RequestId, func() (*proto.WriteAndFlushReply, error) {
		entry, ok := sessionFile(sess, req.Fd, req.Volume)
		if !ok {
			// EBADF in-reply (not a transport error) keeps the errno visible at the
			// FUSE layer. Covers both an unknown fd and an fd↔volume mismatch.
			return &proto.WriteAndFlushReply{Status: proto.FsError_FS_EBADF}, nil
		}
		// Held for the whole write+flush+stat — released when the closure returns.
		defer entry.ReleaseRef()
		// Resolved after the fd↔volume check so a foreign volume can't be probed
		// with someone else's fd (SEC-1), and only on actual execution — never on
		// a cache-hit replay.
		fs, err := r.fsService.GetVolumeFileSystem(req.Volume)
		if err != nil {
			return nil, err
		}
		reply := &proto.WriteAndFlushReply{}
		if len(req.Data) > 0 {
			n, st := entry.File.Write(req.Data, req.Offset)
			if st != fuse.OK {
				reply.Status = fserr.FromErrno(syscall.Errno(st))
				return reply, nil // write error: skip flush, surface at close()
			}
			reply.Written = n
		}
		st := entry.File.Flush()
		reply.Status = fserr.FromErrno(syscall.Errno(st))
		// final_attr is advisory and currently unused by the client (FUSE FLUSH
		// returns no attrs to the kernel). The stat uses a nil caller — fine while
		// it only feeds the bus version. A future client that consumes final_attr
		// for a caller-scoped view would need a Caller field on WriteAndFlushRequest.
		if attr, gst := fs.GetAttr(entry.Path, createContext(ctx, nil)); gst.Ok() {
			reply.FinalAttr = toProtoAttr(attr, nil)
			if st == fuse.OK {
				r.emitMutatedAttr(req.Volume, entry.Path, attr)
			}
		} else if st == fuse.OK {
			r.emitMutatedAttr(req.Volume, entry.Path, nil)
		}
		return reply, nil
	})
}

func (r *RpcFileServerImpl) Allocate(ctx context.Context, request *proto.AllocateRequest) (*proto.AllocateReply, error) {
	sess, err := resolveSession(ctx, r.sessions, request.SessionId)
	if err != nil {
		return nil, err
	}
	entry, ok := sessionFile(sess, request.Fd, request.Volume)
	if !ok {
		return nil, status.Errorf(codes.NotFound, "fd %d not found in session", request.Fd)
	}
	defer entry.ReleaseRef()
	s := entry.File.Allocate(request.Off, request.Size, request.Mode)
	if s == fuse.OK {
		path := entry.Path
		if path == "" {
			path = request.Path
		}
		r.emitMutatedFd(ctx, request.Volume, path, request.Caller)
	}
	return &proto.AllocateReply{Status: fserr.FromErrno(syscall.Errno(s))}, nil
}

// CopyFileRange copies length bytes between two open handles entirely on
// the server — the whole point of the RPC is that no file data crosses
// the wire. Errnos travel in-reply (WriteAndFlush convention): EBADF for a
// handle the session doesn't own, EINVAL for nonzero flags (per
// copy_file_range(2), flags must be 0). No identity re-bind: permission
// was checked at Open and the fds carry their access rights, same trust
// model as Read/Write/Allocate. sessionFile's ref pins BOTH entries open
// for the whole chunk loop — a concurrent reap defers the close until the
// copy finishes.
//
// Idempotency: a non-empty request_id replays the cached reply (bytes_copied
// included) instead of redoing the copy. Both fd refs are acquired INSIDE the
// closure, so a cache-hit replay never touches the fd table — the fds may be
// long released by the time a reconnect retry lands. An empty request_id
// bypasses the cache entirely (withOptionalIdempotency). The flags gate stays
// outside the wrap: pure deterministic validation, nothing worth caching.
func (r *RpcFileServerImpl) CopyFileRange(ctx context.Context, request *proto.CopyFileRangeRequest) (*proto.CopyFileRangeReply, error) {
	sess, err := resolveSession(ctx, r.sessions, request.SessionId)
	if err != nil {
		return nil, err
	}
	if request.Flags != 0 {
		return &proto.CopyFileRangeReply{Status: proto.FsError_FS_EINVAL}, nil
	}
	return withOptionalIdempotency(sess, request.RequestId, func() (*proto.CopyFileRangeReply, error) {
		srcEntry, ok := sessionFile(sess, request.FdIn, request.Volume)
		if !ok {
			return &proto.CopyFileRangeReply{Status: proto.FsError_FS_EBADF}, nil
		}
		defer srcEntry.ReleaseRef()
		dstEntry, ok := sessionFile(sess, request.FdOut, request.Volume)
		if !ok {
			return &proto.CopyFileRangeReply{Status: proto.FsError_FS_EBADF}, nil
		}
		defer dstEntry.ReleaseRef()
		copied, st := serverio.CopyFileRange(srcEntry.File, dstEntry.File, request.OffIn, request.OffOut, request.Length)
		if st == fuse.OK && copied > 0 {
			path := dstEntry.Path
			if path == "" {
				path = request.PathOut
			}
			r.emitMutatedFd(ctx, request.Volume, path, request.Caller)
		}
		return &proto.CopyFileRangeReply{BytesCopied: copied, Status: fserr.FromErrno(syscall.Errno(st))}, nil
	})
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
		return &proto.LseekReply{Status: proto.FsError_FS_EINVAL}, nil
	}
	entry, ok := sessionFile(sess, request.Fd, request.Volume)
	if !ok {
		return &proto.LseekReply{Status: proto.FsError_FS_EBADF}, nil
	}
	defer entry.ReleaseRef()
	off, st := serverio.Lseek(entry.File, request.Offset, request.Whence)
	return &proto.LseekReply{Offset: off, Status: fserr.FromErrno(syscall.Errno(st))}, nil
}
