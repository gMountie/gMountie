package io

import (
	"context"
	"gmountie/pkg/proto"
	"gmountie/pkg/server/grpc/snappy"
	"gmountie/pkg/utils/log"
	"time"

	"github.com/hanwen/go-fuse/v2/fuse"
	"github.com/hanwen/go-fuse/v2/fuse/nodefs"
	"go.uber.org/zap"
	"google.golang.org/grpc"
)

type GrpcFile struct {
	fileClient proto.RpcFileClient
	path       string
	volume     string
	fd         uint64
	ioTimeout  time.Duration
	nodefs.File
}

func NewGrpcFile(fileClient proto.RpcFileClient, volume, path string, fd uint64, ioTimeout time.Duration) *GrpcFile {
	return &GrpcFile{
		fileClient: fileClient,
		path:       path,
		volume:     volume,
		fd:         fd,
		ioTimeout:  ioTimeout,
		File:       nodefs.NewDefaultFile(),
	}
}

func (f *GrpcFile) Read(dest []byte, off int64) (fuse.ReadResult, fuse.Status) {
	ctx, cancel := withIOTimeout(context.Background(), f.ioTimeout)
	defer cancel()
	res, err := retryableCall(ctx, "Read", func(ctx context.Context) (*proto.ReadReply, error) {
		return f.fileClient.Read(ctx, &proto.ReadRequest{
			Volume: f.volume,
			Fd:     f.fd,
			Offset: off,
			Size:   uint32(len(dest)),
		},
			grpc.UseCompressor(snappy.Name),
		)
	})
	if err != nil || res == nil {
		log.Log.Error("error in call: Read", zap.String("path", f.path), zap.Error(err))
		return nil, fuse.EIO
	}
	return fuse.ReadResultData(res.Bytes), fuse.Status(res.Status)
}

func (f *GrpcFile) Write(data []byte, off int64) (written uint32, code fuse.Status) {
	ctx, cancel := withIOTimeout(context.Background(), f.ioTimeout)
	defer cancel()
	res, err := f.fileClient.Write(ctx, &proto.WriteRequest{
		Volume: f.volume,
		Fd:     f.fd,
		Offset: off,
		Bytes:  data,
	},
		grpc.UseCompressor(snappy.Name),
	)
	if err != nil || res == nil {
		log.Log.Error("error in call: Write", zap.String("path", f.path), zap.Error(err))
		return 0, fuse.EIO
	}
	return res.Written, fuse.Status(res.Status)
}

func (f *GrpcFile) Release() {
	ctx, cancel := withIOTimeout(context.Background(), f.ioTimeout)
	defer cancel()
	_, err := f.fileClient.Release(ctx, &proto.ReleaseRequest{
		Volume: f.volume,
		Fd:     f.fd,
	})
	if err != nil {
		log.Log.Error("error in call: Release", zap.String("path", f.path), zap.Error(err))
	}
}

func (f *GrpcFile) Flush() fuse.Status {
	ctx, cancel := withIOTimeout(context.Background(), f.ioTimeout)
	defer cancel()
	res, err := f.fileClient.Flush(ctx, &proto.FlushRequest{
		Volume: f.volume,
		Fd:     f.fd,
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
		Volume: f.volume,
		Fd:     f.fd,
		Flags:  int64(flags),
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
		Volume: f.volume,
		Fd:     f.fd,
		Owner:  owner,
		Flags:  flags,
		Lk:     &proto.FileLock{Start: lk.Start, End: lk.End, Typ: lk.Typ, Pid: lk.Pid},
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
		Volume: f.volume,
		Fd:     f.fd,
		Owner:  owner,
		Flags:  flags,
		Lk:     &proto.FileLock{Start: lk.Start, End: lk.End, Typ: lk.Typ, Pid: lk.Pid},
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
		Volume: f.volume,
		Fd:     f.fd,
		Owner:  owner,
		Flags:  flags,
		Lk:     &proto.FileLock{Start: lk.Start, End: lk.End, Typ: lk.Typ, Pid: lk.Pid},
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
		Volume: f.volume,
		Fd:     f.fd,
		Off:    off,
		Size:   size,
		Mode:   mode,
	})
	if err != nil {
		log.Log.Error("error in call: Allocate", zap.String("path", f.path), zap.Error(err))
		return fuse.EIO
	}
	return fuse.Status(res.Status)
}
