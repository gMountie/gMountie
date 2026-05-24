// backend_grpc.go implements FileSystemBackend against the gRPC layer.
// It owns the wiring previously spread across fs.go (path-level ops) and
// file.go (per-fd ops + streaming Read/Write). Behaviour mirrors the
// legacy implementation: same retry shape, same per-call Snappy on
// Read/Write, same session/fd/request_id discipline from Phase 1.
package io

import (
	"context"
	stdio "io"
	"sync/atomic"
	"time"

	grpcclient "gmountie/pkg/client/grpc"
	"gmountie/pkg/proto"
	"gmountie/pkg/utils/log"

	"github.com/google/uuid"
	"github.com/hanwen/go-fuse/v2/fuse"
	"github.com/pkg/errors"
	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	grpcstatus "google.golang.org/grpc/status"
)

// readResult is the accumulated outcome of a single streaming Read attempt:
// how many bytes landed in dest and the terminal FUSE status reported by the
// server. The two are tracked together so retryableCall can replace them
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
// load-bearing: the server's AssumeUserMiddleware runs setfsuid /
// setfsgid from these fields on every op, so a zero Caller would have
// the server side run as root.
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

// metaCtx bounds an idempotent metadata RPC with the meta timeout AND detaches
// it from the caller's cancellation. The caller is the go-fuse request context,
// which the kernel cancels on FUSE_INTERRUPT — fired constantly by Go's
// async-preemption SIGURG landing on a long-running op. Propagating that
// cancellation aborts the in-flight RPC (gRPC Canceled) and surfaces a spurious
// EIO on an otherwise-healthy readdir/stat (confirmed: the failures vanish under
// GODEBUG=asyncpreemptoff=1). context.WithoutCancel keeps the caller's values
// (the kernel uid/gid/pid that AssumeUserMiddleware reads) while ignoring its
// cancellation, so the RPC runs to completion or its own timeout. Scoped to
// idempotent reads; mutating, fd-returning, and streaming ops keep the
// cancellable context (distinct interrupt-correctness considerations).
func (b *BackendClient) metaCtx(ctx context.Context) (context.Context, context.CancelFunc) {
	return withMetaTimeout(context.WithoutCancel(ctx), b.client.MetaTimeout())
}

// Stat returns the attributes of path. Idempotent; no request_id stamping.
func (b *BackendClient) Stat(ctx context.Context, path string) (*Attr, fuse.Status) {
	ctx2, cancel := b.metaCtx(ctx)
	defer cancel()
	res, err := retryableCall(ctx2, "GetAttr", func(ctx context.Context) (*proto.GetAttrReply, error) {
		return b.client.Fs().GetAttr(ctx, &proto.GetAttrRequest{
			Volume: b.volume,
			Caller: callerFromCtx(ctx),
			Path:   path,
		})
	})
	if err != nil || res == nil {
		log.Log.Error("error in call: GetAttr", zap.String("path", path), zap.Error(err))
		return nil, fuse.EIO
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
	ctx2, cancel := b.metaCtx(ctx)
	defer cancel()
	reply, err := b.client.Fs().GetAttrIfChanged(ctx2, &proto.GetAttrIfChangedRequest{
		Volume:       b.volume,
		Path:         path,
		KnownVersion: knownVersion,
	})
	if err != nil {
		if st, ok := grpcstatus.FromError(err); ok && st.Code() == codes.NotFound {
			return nil, false, fuse.ENOENT
		}
		log.Log.Error("error in call: GetAttrIfChanged", zap.String("path", path), zap.Error(err))
		return nil, false, fuse.EIO
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
	ctx2, cancel := b.metaCtx(ctx)
	defer cancel()
	res, err := retryableCall(ctx2, "OpenDir", func(ctx context.Context) (*proto.OpenDirReply, error) {
		return b.client.Fs().OpenDir(ctx, &proto.OpenDirRequest{
			Volume: b.volume,
			Caller: callerFromCtx(ctx),
			Path:   path,
		})
	})
	if err != nil || res == nil {
		log.Log.Error("error in call: OpenDir", zap.String("path", path), zap.Error(err))
		return nil, fuse.EIO
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
	ctx2, cancel := b.metaCtx(ctx)
	defer cancel()
	res, err := retryableCall(ctx2, "Access", func(ctx context.Context) (*proto.AccessReply, error) {
		return b.client.Fs().Access(ctx, &proto.AccessRequest{
			Volume: b.volume,
			Caller: callerFromCtx(ctx),
			Path:   path,
			Mode:   mode,
		})
	})
	if err != nil || res == nil {
		log.Log.Error("error in call: Access", zap.String("path", path), zap.Error(err))
		return fuse.EIO
	}
	return fuse.Status(res.Status)
}

// StatFs returns filesystem statistics for the volume containing path.
func (b *BackendClient) StatFs(ctx context.Context, path string) (*StatFs, fuse.Status) {
	ctx2, cancel := b.metaCtx(ctx)
	defer cancel()
	res, err := retryableCall(ctx2, "StatFs", func(ctx context.Context) (*proto.StatFsReply, error) {
		return b.client.Fs().StatFs(ctx, &proto.StatFsRequest{Volume: b.volume, Path: path})
	})
	if err != nil || res == nil {
		log.Log.Error("error in call: StatFs", zap.String("path", path), zap.Error(err))
		return nil, fuse.EIO
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
	ctx2, cancel := b.metaCtx(ctx)
	defer cancel()
	res, err := retryableCall(ctx2, "GetXAttr", func(ctx context.Context) (*proto.GetXAttrReply, error) {
		return b.client.Fs().GetXAttr(ctx, &proto.GetXAttrRequest{
			Volume: b.volume, Caller: callerFromCtx(ctx), Path: path, Attribute: attr,
		})
	})
	if err != nil || res == nil {
		log.Log.Error("error in call: GetXAttr", zap.String("path", path), zap.Error(err))
		return nil, fuse.EIO
	}
	return res.Data, fuse.Status(res.Status)
}

// Mkdir creates a directory. Mutating — request_id stamped outside retry.
func (b *BackendClient) Mkdir(ctx context.Context, path string, mode uint32) fuse.Status {
	ctx2, cancel := withMetaTimeout(ctx, b.client.MetaTimeout())
	defer cancel()
	requestID := uuid.NewString()
	res, err := retryableCall(ctx2, "Mkdir", func(ctx context.Context) (*proto.MkdirReply, error) {
		return b.client.Fs().Mkdir(ctx, &proto.MkdirRequest{
			Volume:    b.volume,
			Caller:    callerFromCtx(ctx),
			Path:      path,
			Mode:      mode,
			SessionId: b.client.SessionID(),
			RequestId: requestID,
		})
	})
	if err != nil || res == nil {
		log.Log.Error("error in call: MkDir", zap.String("path", path), zap.Error(err))
		return fuse.EIO
	}
	return fuse.Status(res.Status)
}

// Rmdir removes an empty directory.
func (b *BackendClient) Rmdir(ctx context.Context, path string) fuse.Status {
	ctx2, cancel := withMetaTimeout(ctx, b.client.MetaTimeout())
	defer cancel()
	requestID := uuid.NewString()
	res, err := retryableCall(ctx2, "Rmdir", func(ctx context.Context) (*proto.RmdirReply, error) {
		return b.client.Fs().Rmdir(ctx, &proto.RmdirRequest{
			Volume:    b.volume,
			Caller:    callerFromCtx(ctx),
			Path:      path,
			SessionId: b.client.SessionID(),
			RequestId: requestID,
		})
	})
	if err != nil || res == nil {
		log.Log.Error("error in call: RmDir", zap.String("path", path), zap.Error(err))
		return fuse.EIO
	}
	return fuse.Status(res.Status)
}

// Unlink removes a non-directory.
func (b *BackendClient) Unlink(ctx context.Context, path string) fuse.Status {
	ctx2, cancel := withMetaTimeout(ctx, b.client.MetaTimeout())
	defer cancel()
	requestID := uuid.NewString()
	res, err := retryableCall(ctx2, "Unlink", func(ctx context.Context) (*proto.UnlinkReply, error) {
		return b.client.Fs().Unlink(ctx, &proto.UnlinkRequest{
			Volume:    b.volume,
			Caller:    callerFromCtx(ctx),
			Path:      path,
			SessionId: b.client.SessionID(),
			RequestId: requestID,
		})
	})
	if err != nil || res == nil {
		log.Log.Error("error in call: Unlink", zap.String("path", path), zap.Error(err))
		return fuse.EIO
	}
	return fuse.Status(res.Status)
}

// Rename moves a file/directory.
func (b *BackendClient) Rename(ctx context.Context, oldPath, newPath string) fuse.Status {
	ctx2, cancel := withMetaTimeout(ctx, b.client.MetaTimeout())
	defer cancel()
	requestID := uuid.NewString()
	res, err := retryableCall(ctx2, "Rename", func(ctx context.Context) (*proto.RenameReply, error) {
		return b.client.Fs().Rename(ctx, &proto.RenameRequest{
			Volume:    b.volume,
			Caller:    callerFromCtx(ctx),
			OldName:   oldPath,
			NewName:   newPath,
			SessionId: b.client.SessionID(),
			RequestId: requestID,
		})
	})
	if err != nil || res == nil {
		log.Log.Error("error in call: Rename", zap.String("oldName", oldPath), zap.String("newName", newPath), zap.Error(err))
		return fuse.EIO
	}
	return fuse.Status(res.Status)
}

// Truncate changes a file's length.
func (b *BackendClient) Truncate(ctx context.Context, path string, size uint64) fuse.Status {
	ctx2, cancel := withMetaTimeout(ctx, b.client.MetaTimeout())
	defer cancel()
	requestID := uuid.NewString()
	res, err := retryableCall(ctx2, "Truncate", func(ctx context.Context) (*proto.TruncateReply, error) {
		return b.client.Fs().Truncate(ctx, &proto.TruncateRequest{
			Volume:    b.volume,
			Caller:    callerFromCtx(ctx),
			Path:      path,
			Size:      size,
			SessionId: b.client.SessionID(),
			RequestId: requestID,
		})
	})
	if err != nil || res == nil {
		log.Log.Error("error in call: Truncate", zap.String("path", path), zap.Error(err))
		return fuse.EIO
	}
	return fuse.Status(res.Status)
}

// Chmod changes file permissions.
func (b *BackendClient) Chmod(ctx context.Context, path string, mode uint32) fuse.Status {
	ctx2, cancel := withMetaTimeout(ctx, b.client.MetaTimeout())
	defer cancel()
	requestID := uuid.NewString()
	res, err := retryableCall(ctx2, "Chmod", func(ctx context.Context) (*proto.ChmodReply, error) {
		return b.client.Fs().Chmod(ctx, &proto.ChmodRequest{
			Volume:    b.volume,
			Caller:    callerFromCtx(ctx),
			Path:      path,
			Mode:      mode,
			SessionId: b.client.SessionID(),
			RequestId: requestID,
		})
	})
	if err != nil || res == nil {
		log.Log.Error("error in call: Chmod", zap.String("path", path), zap.Error(err))
		return fuse.EIO
	}
	return fuse.Status(res.Status)
}

// Chown changes ownership.
func (b *BackendClient) Chown(ctx context.Context, path string, uid, gid uint32) fuse.Status {
	ctx2, cancel := withMetaTimeout(ctx, b.client.MetaTimeout())
	defer cancel()
	requestID := uuid.NewString()
	res, err := retryableCall(ctx2, "Chown", func(ctx context.Context) (*proto.ChownReply, error) {
		return b.client.Fs().Chown(ctx, &proto.ChownRequest{
			Volume:    b.volume,
			Caller:    callerFromCtx(ctx),
			Path:      path,
			Uid:       uid,
			Gid:       gid,
			SessionId: b.client.SessionID(),
			RequestId: requestID,
		})
	})
	if err != nil || res == nil {
		log.Log.Error("error in call: Chown", zap.String("path", path), zap.Error(err))
		return fuse.EIO
	}
	return fuse.Status(res.Status)
}

// Open opens an existing file. The returned FileHandle is a
// *grpcFileHandle holding fd + session + per-file knobs.
func (b *BackendClient) Open(ctx context.Context, path string, flags uint32) (FileHandle, fuse.Status) {
	ctx2, cancel := withMetaTimeout(ctx, b.client.MetaTimeout())
	defer cancel()
	requestID := uuid.NewString()
	res, err := retryableCall(ctx2, "Open", func(ctx context.Context) (*proto.OpenReply, error) {
		return b.client.File().Open(ctx, &proto.OpenRequest{
			Volume:    b.volume,
			Caller:    callerFromCtx(ctx),
			Path:      path,
			Flags:     flags,
			SessionId: b.client.SessionID(),
			RequestId: requestID,
		})
	})
	if err != nil || res == nil {
		log.Log.Error("error in call: Open", zap.String("path", path), zap.Error(err))
		return nil, fuse.EIO
	}
	if fuse.Status(res.Status) != fuse.OK {
		return nil, fuse.Status(res.Status)
	}
	return newGrpcFileHandle(
		b.client.File(), b.volume, path, res.Fd,
		b.client.IOTimeout(), b.client.SessionID(),
		b.client.PerFileConfig(),
	), fuse.OK
}

// Create creates a new file. The Attr return is always nil — the current
// proto.CreateReply does not carry attributes; Task 3's node adapter will
// issue a Stat right after Create to fill the kernel's EntryOut.
func (b *BackendClient) Create(ctx context.Context, parent, name string, flags, mode uint32) (FileHandle, *Attr, fuse.Status) {
	path := joinPath(parent, name)
	ctx2, cancel := withMetaTimeout(ctx, b.client.MetaTimeout())
	defer cancel()
	requestID := uuid.NewString()
	res, err := retryableCall(ctx2, "Create", func(ctx context.Context) (*proto.CreateReply, error) {
		return b.client.File().Create(ctx, &proto.CreateRequest{
			Volume:    b.volume,
			Caller:    callerFromCtx(ctx),
			Path:      path,
			Flags:     flags,
			Mode:      mode,
			SessionId: b.client.SessionID(),
			RequestId: requestID,
		})
	})
	if err != nil || res == nil {
		log.Log.Error("error in call: Create", zap.String("path", path), zap.Error(err))
		return nil, nil, fuse.EIO
	}
	if fuse.Status(res.Status) != fuse.OK {
		return nil, nil, fuse.Status(res.Status)
	}
	h := newGrpcFileHandle(
		b.client.File(), b.volume, path, res.Fd,
		b.client.IOTimeout(), b.client.SessionID(),
		b.client.PerFileConfig(),
	)
	return h, nil, fuse.OK
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
			if prefetchOff, ok := h.readahead.Observe(off, n); ok {
				go b.doPrefetch(h, prefetchOff)
			}
			return n, fuse.OK
		}
	}
	ctx2, cancel := withIOTimeout(ctx, h.ioTimeout)
	defer cancel()
	res, err := retryableCall(ctx2, "Read", func(ctx context.Context) (readResult, error) {
		stream, err := h.fileClient.Read(ctx, &proto.ReadRequest{
			Volume:    h.volume,
			Fd:        h.fd,
			Offset:    off,
			Size:      uint32(len(dest)),
			SessionId: h.sessionID,
		})
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
		return 0, fuse.EIO
	}
	if !res.status.Ok() {
		return 0, res.status
	}
	if h.readahead != nil {
		if prefetchOff, ok := h.readahead.Observe(off, res.written); ok {
			go b.doPrefetch(h, prefetchOff)
		}
	}
	return res.written, fuse.OK
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
	ctx, cancel := withIOTimeout(h.lifeCtx, h.ioTimeout)
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
	ctx, cancel := withIOTimeout(context.Background(), h.ioTimeout)
	defer cancel()
	res, err := retryableCall(ctx, "Write", func(ctx context.Context) (*proto.WriteReply, error) {
		stream, err := h.fileClient.Write(ctx)
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
		return 0, fuse.EIO
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
	// Mark dirty before any network I/O so that a failed write still
	// leaves the handle marked dirty; the next Flush will retry.
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
// prefetch goroutine bails out, then drains the coalescer best-effort,
// then issues the server-side Release RPC. Always proceeds to the
// server-side Release even if drain fails.
func (b *BackendClient) Release(ctx context.Context, fh FileHandle) fuse.Status {
	h := resolveHandle(fh)
	if h == nil {
		return fuse.EBADF
	}
	if h.lifeCancel != nil {
		h.lifeCancel()
	}
	if st := b.drainCoalescer(h); !st.Ok() {
		log.Log.Error("error draining coalescer on Release",
			zap.String("path", h.path), zap.Stringer("status", st))
	}
	ctx2, cancel := withIOTimeout(ctx, h.ioTimeout)
	defer cancel()
	_, err := h.fileClient.Release(ctx2, &proto.ReleaseRequest{
		Volume:    h.volume,
		Fd:        h.fd,
		SessionId: h.sessionID,
	})
	if err != nil {
		log.Log.Error("error in call: Release", zap.String("path", h.path), zap.Error(err))
		return fuse.EIO
	}
	return fuse.OK
}

// Flush drains the coalescer in memory and fuses the pending write with
// the flush into a single WriteAndFlush RPC (one RTT instead of the
// previous two: streaming Write + Flush). If the handle is clean (nothing
// written since the last successful Flush) the RPC is skipped entirely.
//
// Idempotent — wrapped in retryableCall so transient gRPC failures
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
	ctx2, cancel := withIOTimeout(ctx, h.ioTimeout)
	defer cancel()
	res, err := retryableCall(ctx2, "WriteAndFlush", func(ctx context.Context) (*proto.WriteAndFlushReply, error) {
		return h.fileClient.WriteAndFlush(ctx, &proto.WriteAndFlushRequest{
			Volume:    h.volume,
			Fd:        h.fd,
			SessionId: h.sessionID,
			Offset:    offset,
			Data:      data,
		})
	})
	if err != nil {
		log.Log.Error("error in call: WriteAndFlush", zap.String("path", h.path), zap.Error(err))
		return fuse.EIO
	}
	st := fuse.Status(res.Status)
	if st.Ok() {
		h.dirty.Store(false)
	}
	return st
}

// Fsync drains coalesced writes then issues the server-side Fsync RPC.
// Idempotent — wrapped in retryableCall for the same reason as Flush
// (a second Fsync against an already-synced fd is a no-op).
func (b *BackendClient) Fsync(ctx context.Context, fh FileHandle, flags int64) fuse.Status {
	h := resolveHandle(fh)
	if h == nil {
		return fuse.EBADF
	}
	if st := b.drainCoalescer(h); !st.Ok() {
		return st
	}
	ctx2, cancel := withIOTimeout(ctx, h.ioTimeout)
	defer cancel()
	res, err := retryableCall(ctx2, "Fsync", func(ctx context.Context) (*proto.FsyncReply, error) {
		return h.fileClient.Fsync(ctx, &proto.FsyncRequest{
			Volume:    h.volume,
			Fd:        h.fd,
			Flags:     flags,
			SessionId: h.sessionID,
		})
	})
	if err != nil {
		log.Log.Error("error in call: Fsync", zap.String("path", h.path), zap.Error(err))
		return fuse.EIO
	}
	return fuse.Status(res.Status)
}

// Allocate proxies fallocate(2) to the server. Mutating but not retried
// — fallocate's idempotency is fuzzy across keep-size vs. extend modes,
// and the legacy code never retried it either. No request_id stamp for
// the same reason.
func (b *BackendClient) Allocate(ctx context.Context, fh FileHandle, off, size uint64, mode uint32) fuse.Status {
	h := resolveHandle(fh)
	if h == nil {
		return fuse.EBADF
	}
	ctx2, cancel := withIOTimeout(ctx, h.ioTimeout)
	defer cancel()
	res, err := h.fileClient.Allocate(ctx2, &proto.AllocateRequest{
		Volume:    h.volume,
		Caller:    callerFromCtx(ctx),
		Fd:        h.fd,
		Path:      h.path,
		Off:       off,
		Size:      size,
		Mode:      mode,
		SessionId: h.sessionID,
	})
	if err != nil {
		log.Log.Error("error in call: Allocate", zap.String("path", h.path), zap.Error(err))
		return fuse.EIO
	}
	return fuse.Status(res.Status)
}

// GetLk queries the lock state for a region of the file. No retry —
// lock semantics get weird under replay (a server-side success the
// client missed would leave us holding a phantom lock).
func (b *BackendClient) GetLk(ctx context.Context, fh FileHandle, owner uint64, lk *fuse.FileLock, flags uint32, out *fuse.FileLock) fuse.Status {
	h := resolveHandle(fh)
	if h == nil {
		return fuse.EBADF
	}
	ctx2, cancel := withIOTimeout(ctx, h.ioTimeout)
	defer cancel()
	res, err := h.fileClient.GetLk(ctx2, &proto.GetLkRequest{
		Volume:    h.volume,
		Fd:        h.fd,
		Owner:     owner,
		Flags:     flags,
		Lk:        &proto.FileLock{Start: lk.Start, End: lk.End, Typ: lk.Typ, Pid: lk.Pid},
		SessionId: h.sessionID,
	})
	if err != nil {
		log.Log.Error("error in call: GetLk", zap.String("path", h.path), zap.Error(err))
		return fuse.EIO
	}
	if res.Lk != nil {
		*out = fuse.FileLock{Start: res.Lk.Start, End: res.Lk.End, Typ: res.Lk.Typ, Pid: res.Lk.Pid}
	}
	return fuse.Status(res.Status)
}

// SetLk attempts a non-blocking lock acquisition (fcntl(F_SETLK)). No
// retry — see GetLk.
func (b *BackendClient) SetLk(ctx context.Context, fh FileHandle, owner uint64, lk *fuse.FileLock, flags uint32) fuse.Status {
	h := resolveHandle(fh)
	if h == nil {
		return fuse.EBADF
	}
	ctx2, cancel := withIOTimeout(ctx, h.ioTimeout)
	defer cancel()
	res, err := h.fileClient.SetLk(ctx2, &proto.SetLkRequest{
		Volume:    h.volume,
		Fd:        h.fd,
		Owner:     owner,
		Flags:     flags,
		Lk:        &proto.FileLock{Start: lk.Start, End: lk.End, Typ: lk.Typ, Pid: lk.Pid},
		SessionId: h.sessionID,
	})
	if err != nil {
		log.Log.Error("error in call: SetLk", zap.String("path", h.path), zap.Error(err))
		return fuse.EIO
	}
	return fuse.Status(res.Status)
}

// SetLkw attempts a blocking lock acquisition (fcntl(F_SETLKW)). No
// retry — see GetLk.
func (b *BackendClient) SetLkw(ctx context.Context, fh FileHandle, owner uint64, lk *fuse.FileLock, flags uint32) fuse.Status {
	h := resolveHandle(fh)
	if h == nil {
		return fuse.EBADF
	}
	ctx2, cancel := withIOTimeout(ctx, h.ioTimeout)
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
		return fuse.EIO
	}
	return fuse.Status(res.Status)
}

// grpcFileHandle is the per-fd state returned by Open/Create. It mirrors
// the legacy GrpcFile minus the nodefs.File embedding — the FUSE adapter
// for the new go-fuse v2 fs.* node interface lives in node.go (Task 3).
type grpcFileHandle struct {
	fileClient proto.RpcFileClient
	volume     string
	path       string
	fd         uint64
	ioTimeout  time.Duration
	sessionID  string
	// dirty tracks whether a Write has been accepted since the last
	// successful Flush. Flush skips the RPC entirely when dirty is false
	// (clean-handle fast path). Set atomically by Write; cleared atomically
	// by a successful Flush. Fsync does not touch dirty.
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
// volume. cfg bundles the per-file knobs: ReadaheadChunkBytes of 0
// disables the readahead path entirely; otherwise prefetches arm after
// ReadaheadThreshold strictly-sequential reads. WriteCoalesceBytes of 0
// disables per-fd write coalescing; otherwise small contiguous writes
// accumulate up to that threshold before flushing.
func newGrpcFileHandle(
	fileClient proto.RpcFileClient,
	volume, path string,
	fd uint64,
	ioTimeout time.Duration,
	sessionID string,
	cfg grpcclient.PerFileConfig,
) *grpcFileHandle {
	ctx, cancel := context.WithCancel(context.Background())
	h := &grpcFileHandle{
		fileClient:        fileClient,
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
		h.readahead = NewReadahead(cfg.ReadaheadChunkBytes, cfg.ReadaheadThreshold)
	}
	if cfg.WriteCoalesceBytes > 0 {
		h.coalescer = NewWriteCoalescer(cfg.WriteCoalesceBytes)
	}
	return h
}
