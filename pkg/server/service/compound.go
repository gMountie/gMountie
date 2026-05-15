// Package service contains the server-side business logic that controllers
// delegate to. compound.go holds CompoundDispatcher — the fan-out driver for
// the RpcFs.Compound batched-metadata RPC.
package service

import (
	"context"
	"sync"

	"gmountie/pkg/proto"
	"gmountie/pkg/server/config"

	"github.com/hanwen/go-fuse/v2/fuse"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// FsHandlers is the subset of the RpcFs server surface that the dispatcher
// invokes. Declared here (in service) so the dispatcher can be unit tested
// without spinning up gRPC; the existing RpcServerImpl satisfies it
// structurally — no explicit conformance needed.
type FsHandlers interface {
	GetAttr(context.Context, *proto.GetAttrRequest) (*proto.GetAttrReply, error)
	StatFs(context.Context, *proto.StatFsRequest) (*proto.StatFsReply, error)
	OpenDir(context.Context, *proto.OpenDirRequest) (*proto.OpenDirReply, error)
	Access(context.Context, *proto.AccessRequest) (*proto.AccessReply, error)
	GetXAttr(context.Context, *proto.GetXAttrRequest) (*proto.GetXAttrReply, error)
}

// CompoundDispatcher fans a CompoundRequest's op list out across a bounded
// pool of goroutines, returning N replies in input order. Per-op errors are
// surfaced as a CompoundReply_Status variant carrying a fuse status — the
// batch shape is always N-in / N-out so the client doesn't have to
// reconstruct positions on partial failures.
type CompoundDispatcher struct {
	fs          FsHandlers
	maxParallel int
}

// NewCompoundDispatcher constructs a dispatcher that will run at most
// maxParallel sub-ops concurrently. Non-positive values are clamped to
// config.DefaultCompoundMaxParallel.
func NewCompoundDispatcher(fs FsHandlers, maxParallel int) *CompoundDispatcher {
	if maxParallel <= 0 {
		maxParallel = config.DefaultCompoundMaxParallel
	}
	return &CompoundDispatcher{fs: fs, maxParallel: maxParallel}
}

// Dispatch runs each op concurrently (bounded by maxParallel) and returns
// the per-op replies in input order. An empty input yields an empty slice.
func (d *CompoundDispatcher) Dispatch(ctx context.Context, ops []*proto.CompoundOp) []*proto.CompoundReply {
	replies := make([]*proto.CompoundReply, len(ops))
	if len(ops) == 0 {
		return replies
	}
	sem := make(chan struct{}, d.maxParallel)
	var wg sync.WaitGroup
	for i, op := range ops {
		wg.Add(1)
		sem <- struct{}{}
		go func(i int, op *proto.CompoundOp) {
			defer wg.Done()
			defer func() { <-sem }()
			replies[i] = d.dispatchOne(ctx, op)
		}(i, op)
	}
	wg.Wait()
	return replies
}

// dispatchOne handles one CompoundOp. A Go-error return from the underlying
// handler (typically a volume-lookup failure surfaced as a gRPC status code)
// is mapped to a fuse status and surfaced via the Status oneof variant; the
// successful path returns the matching typed reply variant. A nil op or
// unknown op type returns fuse.EINVAL — defensive, not load-bearing.
func (d *CompoundDispatcher) dispatchOne(ctx context.Context, op *proto.CompoundOp) *proto.CompoundReply {
	if op == nil {
		return statusReply(fuse.EINVAL)
	}
	switch v := op.GetOp().(type) {
	case *proto.CompoundOp_GetAttr:
		reply, err := d.fs.GetAttr(ctx, v.GetAttr)
		if err != nil {
			return statusReply(grpcErrToFuseStatus(err))
		}
		return &proto.CompoundReply{Reply: &proto.CompoundReply_GetAttr{GetAttr: reply}}
	case *proto.CompoundOp_StatFs:
		reply, err := d.fs.StatFs(ctx, v.StatFs)
		if err != nil {
			return statusReply(grpcErrToFuseStatus(err))
		}
		return &proto.CompoundReply{Reply: &proto.CompoundReply_StatFs{StatFs: reply}}
	case *proto.CompoundOp_OpenDir:
		reply, err := d.fs.OpenDir(ctx, v.OpenDir)
		if err != nil {
			return statusReply(grpcErrToFuseStatus(err))
		}
		return &proto.CompoundReply{Reply: &proto.CompoundReply_OpenDir{OpenDir: reply}}
	case *proto.CompoundOp_Access:
		reply, err := d.fs.Access(ctx, v.Access)
		if err != nil {
			return statusReply(grpcErrToFuseStatus(err))
		}
		return &proto.CompoundReply{Reply: &proto.CompoundReply_Access{Access: reply}}
	case *proto.CompoundOp_GetXattr:
		reply, err := d.fs.GetXAttr(ctx, v.GetXattr)
		if err != nil {
			return statusReply(grpcErrToFuseStatus(err))
		}
		return &proto.CompoundReply{Reply: &proto.CompoundReply_GetXattr{GetXattr: reply}}
	default:
		return statusReply(fuse.EINVAL)
	}
}

func statusReply(s fuse.Status) *proto.CompoundReply {
	return &proto.CompoundReply{Reply: &proto.CompoundReply_Status{Status: int32(s)}}
}

// grpcErrToFuseStatus maps a gRPC error to a fuse.Status (errno-equivalent).
// Used to flatten transport-class failures (unknown volume, deadline) into
// the per-op status slot so the batch shape stays consistent. Codes not
// listed default to fuse.EIO.
func grpcErrToFuseStatus(err error) fuse.Status {
	if err == nil {
		return fuse.OK
	}
	switch status.Code(err) {
	case codes.OK:
		return fuse.OK
	case codes.NotFound:
		return fuse.ENOENT
	case codes.PermissionDenied:
		return fuse.EACCES
	case codes.Unauthenticated:
		return fuse.EACCES
	case codes.Unimplemented:
		return fuse.ENOSYS
	case codes.InvalidArgument:
		return fuse.EINVAL
	case codes.Unavailable:
		return fuse.EIO
	case codes.Canceled, codes.DeadlineExceeded:
		return fuse.EINTR
	default:
		return fuse.EIO
	}
}
