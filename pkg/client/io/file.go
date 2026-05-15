package io

import (
	"context"
	stdio "io"
	"time"

	"gmountie/pkg/proto"
	"gmountie/pkg/server/grpc/snappy"
	"gmountie/pkg/utils/log"

	"github.com/google/uuid"
	"github.com/hanwen/go-fuse/v2/fuse"
	"github.com/hanwen/go-fuse/v2/fuse/nodefs"
	"github.com/pkg/errors"
	"go.uber.org/zap"
	"google.golang.org/grpc"
)

type GrpcFile struct {
	fileClient proto.RpcFileClient
	path       string
	volume     string
	fd         uint64
	ioTimeout  time.Duration
	sessionID  string
	// readahead is non-nil when readahead is enabled for this fd. The
	// synchronous Read path consults Serve before issuing a network Read
	// and feeds Observe afterwards; the prefetch goroutine writes via
	// Store.
	readahead *Readahead
	// lifeCtx is cancelled by Release so any in-flight prefetch goroutine
	// returns promptly instead of holding the file open on the server
	// past the FUSE close.
	lifeCtx    context.Context
	lifeCancel context.CancelFunc
	nodefs.File
}

// NewGrpcFile constructs a GrpcFile bound to fd on the named volume.
// readaheadChunk of 0 disables the readahead path entirely; otherwise
// readaheadChunk-sized prefetches arm after readaheadThreshold
// strictly-sequential reads.
func NewGrpcFile(fileClient proto.RpcFileClient, volume, path string, fd uint64, ioTimeout time.Duration, sessionID string, readaheadChunk, readaheadThreshold int) *GrpcFile {
	ctx, cancel := context.WithCancel(context.Background())
	f := &GrpcFile{
		fileClient: fileClient,
		path:       path,
		volume:     volume,
		fd:         fd,
		ioTimeout:  ioTimeout,
		sessionID:  sessionID,
		lifeCtx:    ctx,
		lifeCancel: cancel,
		File:       nodefs.NewDefaultFile(),
	}
	if readaheadChunk > 0 && readaheadThreshold > 0 {
		f.readahead = NewReadahead(readaheadChunk, readaheadThreshold)
	}
	return f
}

// readResult is the accumulated outcome of a single streaming Read attempt:
// how many bytes landed in dest and the terminal FUSE status reported by the
// server. The two are tracked together so retryableCall can replace them
// wholesale on each attempt without partial-state bleed-through.
type readResult struct {
	written int
	status  fuse.Status
}

// Read consumes the server-streaming Read RPC, accumulating frames into dest
// in order. Each retry attempt opens a fresh stream — Read is naturally
// idempotent (no side effects) so we do not stamp a request_id.
//
// If readahead is enabled and the previous prefetch fully covers the
// requested range, Serve returns the bytes directly without touching the
// network. Otherwise we issue the streaming Read, then feed Observe with
// the result; if Observe arms a prefetch, we kick off a background
// doPrefetch goroutine bounded by f.lifeCtx so Release cancels it.
func (f *GrpcFile) Read(dest []byte, off int64) (fuse.ReadResult, fuse.Status) {
	if f.readahead != nil {
		if n, hit := f.readahead.Serve(dest, off); hit {
			if prefetchOff, ok := f.readahead.Observe(off, n); ok {
				go f.doPrefetch(prefetchOff)
			}
			return fuse.ReadResultData(dest[:n]), fuse.OK
		}
	}
	ctx, cancel := withIOTimeout(context.Background(), f.ioTimeout)
	defer cancel()
	res, err := retryableCall(ctx, "Read", func(ctx context.Context) (readResult, error) {
		stream, err := f.fileClient.Read(ctx, &proto.ReadRequest{
			Volume:    f.volume,
			Fd:        f.fd,
			Offset:    off,
			Size:      uint32(len(dest)),
			SessionId: f.sessionID,
		}, grpc.UseCompressor(snappy.Name))
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
		log.Log.Error("error in call: Read", zap.String("path", f.path), zap.Error(err))
		return nil, fuse.EIO
	}
	if !res.status.Ok() {
		return nil, res.status
	}
	if f.readahead != nil {
		if prefetchOff, ok := f.readahead.Observe(off, res.written); ok {
			go f.doPrefetch(prefetchOff)
		}
	}
	return fuse.ReadResultData(dest[:res.written]), fuse.OK
}

// doPrefetch issues a streaming Read for the next chunk and parks the
// result in the readahead ring on success. It runs under f.lifeCtx so
// Release cancels in-flight prefetches. Errors are intentionally
// swallowed — the synchronous Read path will re-fetch on the next Read
// if the ring is empty, and the prefetch is a hint, not a contract.
func (f *GrpcFile) doPrefetch(off int64) {
	if f.readahead == nil {
		return
	}
	chunk := f.readahead.chunkSize
	ctx, cancel := withIOTimeout(f.lifeCtx, f.ioTimeout)
	defer cancel()
	stream, err := f.fileClient.Read(ctx, &proto.ReadRequest{
		Volume:    f.volume,
		Fd:        f.fd,
		Offset:    off,
		Size:      uint32(chunk),
		SessionId: f.sessionID,
	}, grpc.UseCompressor(snappy.Name))
	if err != nil {
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
			return
		}
		if st := fuse.Status(frame.GetStatus()); !st.Ok() {
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
	f.readahead.Store(off, buf[:written])
}

// writeFrameSizeBytes bounds a single WriteFrame's data slice. Hardcoded
// here for now; Task 7 of the Phase 3 plan negotiates this value with the
// server. 1 MiB matches the server's default FrameSizeBytes.
const writeFrameSizeBytes = 1 << 20

// Write proxies a FUSE Write to the server-streaming Write RPC. Frame 1
// carries the header; subsequent frames carry only `data`. requestID is
// generated once outside the retry closure so any retry attempt re-sends
// the same id and the server's per-session idempotency cache
// short-circuits the apply on the second attempt.
func (f *GrpcFile) Write(data []byte, off int64) (written uint32, code fuse.Status) {
	ctx, cancel := withIOTimeout(context.Background(), f.ioTimeout)
	defer cancel()
	requestID := uuid.NewString()
	res, err := retryableCall(ctx, "Write", func(ctx context.Context) (*proto.WriteReply, error) {
		stream, err := f.fileClient.Write(ctx, grpc.UseCompressor(snappy.Name))
		if err != nil {
			return nil, err
		}
		// Frame 1 always carries the header. If data fits in a single frame
		// we send it whole; otherwise we send the first writeFrameSizeBytes
		// here and loop on the remainder below.
		first := writeFrameSizeBytes
		if first > len(data) {
			first = len(data)
		}
		header := &proto.WriteFrame{
			Volume:    f.volume,
			Fd:        f.fd,
			SessionId: f.sessionID,
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
		log.Log.Error("error in call: Write", zap.String("path", f.path), zap.Error(err))
		return 0, fuse.EIO
	}
	return res.Written, fuse.Status(res.Status)
}

func (f *GrpcFile) Release() {
	// Cancel any in-flight prefetch goroutine first so it bails out
	// before we issue the server-side Release.
	if f.lifeCancel != nil {
		f.lifeCancel()
	}
	ctx, cancel := withIOTimeout(context.Background(), f.ioTimeout)
	defer cancel()
	_, err := f.fileClient.Release(ctx, &proto.ReleaseRequest{
		Volume:    f.volume,
		Fd:        f.fd,
		SessionId: f.sessionID,
	})
	if err != nil {
		log.Log.Error("error in call: Release", zap.String("path", f.path), zap.Error(err))
	}
}

func (f *GrpcFile) Flush() fuse.Status {
	ctx, cancel := withIOTimeout(context.Background(), f.ioTimeout)
	defer cancel()
	res, err := f.fileClient.Flush(ctx, &proto.FlushRequest{
		Volume:    f.volume,
		Fd:        f.fd,
		SessionId: f.sessionID,
	})
	if err != nil {
		log.Log.Error("error in call: Flush", zap.String("path", f.path), zap.Error(err))
		return fuse.EIO
	}
	return fuse.Status(res.Status)
}

func (f *GrpcFile) Fsync(flags int) fuse.Status {
	ctx, cancel := withIOTimeout(context.Background(), f.ioTimeout)
	defer cancel()
	res, err := f.fileClient.Fsync(ctx, &proto.FsyncRequest{
		Volume:    f.volume,
		Fd:        f.fd,
		Flags:     int64(flags),
		SessionId: f.sessionID,
	})
	if err != nil {
		log.Log.Error("error in call: Fsync", zap.String("path", f.path), zap.Error(err))
		return fuse.EIO
	}
	return fuse.Status(res.Status)
}

func (f *GrpcFile) GetLk(owner uint64, lk *fuse.FileLock, flags uint32, out *fuse.FileLock) fuse.Status {
	ctx, cancel := withIOTimeout(context.Background(), f.ioTimeout)
	defer cancel()
	res, err := f.fileClient.GetLk(ctx, &proto.GetLkRequest{
		Volume:    f.volume,
		Fd:        f.fd,
		Owner:     owner,
		Flags:     flags,
		Lk:        &proto.FileLock{Start: lk.Start, End: lk.End, Typ: lk.Typ, Pid: lk.Pid},
		SessionId: f.sessionID,
	})
	if err != nil {
		log.Log.Error("error in call: GetLk", zap.String("path", f.path), zap.Error(err))
		return fuse.EIO
	}

	*out = fuse.FileLock{Start: res.Lk.Start, End: res.Lk.End, Typ: res.Lk.Typ, Pid: res.Lk.Pid}
	return fuse.Status(res.Status)
}

func (f *GrpcFile) SetLk(owner uint64, lk *fuse.FileLock, flags uint32) fuse.Status {
	ctx, cancel := withIOTimeout(context.Background(), f.ioTimeout)
	defer cancel()
	res, err := f.fileClient.SetLk(ctx, &proto.SetLkRequest{
		Volume:    f.volume,
		Fd:        f.fd,
		Owner:     owner,
		Flags:     flags,
		Lk:        &proto.FileLock{Start: lk.Start, End: lk.End, Typ: lk.Typ, Pid: lk.Pid},
		SessionId: f.sessionID,
	})
	if err != nil {
		log.Log.Error("error in call: SetLk", zap.String("path", f.path), zap.Error(err))
		return fuse.EIO
	}
	return fuse.Status(res.Status)
}

func (f *GrpcFile) SetLkw(owner uint64, lk *fuse.FileLock, flags uint32) fuse.Status {
	ctx, cancel := withIOTimeout(context.Background(), f.ioTimeout)
	defer cancel()
	res, err := f.fileClient.SetLkw(ctx, &proto.SetLkwRequest{
		Volume:    f.volume,
		Fd:        f.fd,
		Owner:     owner,
		Flags:     flags,
		Lk:        &proto.FileLock{Start: lk.Start, End: lk.End, Typ: lk.Typ, Pid: lk.Pid},
		SessionId: f.sessionID,
	})
	if err != nil {
		log.Log.Error("error in call: SetLkw", zap.String("path", f.path), zap.Error(err))
		return fuse.EIO
	}
	return fuse.Status(res.Status)
}

// Allocate allocates space for a file
func (f *GrpcFile) Allocate(off uint64, size uint64, mode uint32) fuse.Status {
	ctx, cancel := withIOTimeout(context.Background(), f.ioTimeout)
	defer cancel()
	res, err := f.fileClient.Allocate(ctx, &proto.AllocateRequest{
		Volume:    f.volume,
		Fd:        f.fd,
		Off:       off,
		Size:      size,
		Mode:      mode,
		SessionId: f.sessionID,
	})
	if err != nil {
		log.Log.Error("error in call: Allocate", zap.String("path", f.path), zap.Error(err))
		return fuse.EIO
	}
	return fuse.Status(res.Status)
}
