package service

import (
	"context"
	"sync/atomic"
	"testing"

	"gmountie/pkg/proto"

	"github.com/hanwen/go-fuse/v2/fuse"
	"github.com/stretchr/testify/suite"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// stubFsHandlers is a hand-rolled FsHandlers stub. Hand-rolled (rather than
// mockery-generated) because the parallelism test needs custom blocking
// behaviour and the interface is narrow enough that a stub is cheaper than
// teaching mockery to pick it up.
type stubFsHandlers struct {
	getAttr  func(context.Context, *proto.GetAttrRequest) (*proto.GetAttrReply, error)
	statFs   func(context.Context, *proto.StatFsRequest) (*proto.StatFsReply, error)
	openDir  func(context.Context, *proto.OpenDirRequest) (*proto.OpenDirReply, error)
	access   func(context.Context, *proto.AccessRequest) (*proto.AccessReply, error)
	getXAttr func(context.Context, *proto.GetXAttrRequest) (*proto.GetXAttrReply, error)
}

func (s *stubFsHandlers) GetAttr(ctx context.Context, r *proto.GetAttrRequest) (*proto.GetAttrReply, error) {
	return s.getAttr(ctx, r)
}
func (s *stubFsHandlers) StatFs(ctx context.Context, r *proto.StatFsRequest) (*proto.StatFsReply, error) {
	return s.statFs(ctx, r)
}
func (s *stubFsHandlers) OpenDir(ctx context.Context, r *proto.OpenDirRequest) (*proto.OpenDirReply, error) {
	return s.openDir(ctx, r)
}
func (s *stubFsHandlers) Access(ctx context.Context, r *proto.AccessRequest) (*proto.AccessReply, error) {
	return s.access(ctx, r)
}
func (s *stubFsHandlers) GetXAttr(ctx context.Context, r *proto.GetXAttrRequest) (*proto.GetXAttrReply, error) {
	return s.getXAttr(ctx, r)
}

type CompoundSuite struct {
	suite.Suite
}

func TestCompoundSuite(t *testing.T) {
	suite.Run(t, new(CompoundSuite))
}

// TestDispatch_GetAttr_OpenDir_GetAttr_ReturnsThreeReplies feeds a mixed op
// list and asserts the dispatcher returns N typed replies in order.
func (s *CompoundSuite) TestDispatch_GetAttr_OpenDir_GetAttr_ReturnsThreeReplies() {
	stub := &stubFsHandlers{
		getAttr: func(_ context.Context, r *proto.GetAttrRequest) (*proto.GetAttrReply, error) {
			// Encode the path into Ino so we can verify per-op routing.
			return &proto.GetAttrReply{
				Attributes: &proto.Attr{Ino: uint64(len(r.Path))},
				Status:     int32(fuse.OK),
			}, nil
		},
		openDir: func(_ context.Context, r *proto.OpenDirRequest) (*proto.OpenDirReply, error) {
			return &proto.OpenDirReply{
				Entries: []*proto.DirEntry{{Name: "x", Ino: 42}},
				Status:  int32(fuse.OK),
			}, nil
		},
	}
	d := NewCompoundDispatcher(stub, 4)

	ops := []*proto.CompoundOp{
		{Op: &proto.CompoundOp_GetAttr{GetAttr: &proto.GetAttrRequest{Volume: "v", Path: "/a.txt"}}},
		{Op: &proto.CompoundOp_OpenDir{OpenDir: &proto.OpenDirRequest{Volume: "v", Path: "/"}}},
		{Op: &proto.CompoundOp_GetAttr{GetAttr: &proto.GetAttrRequest{Volume: "v", Path: "/bb.txt"}}},
	}

	replies := d.Dispatch(context.Background(), ops)

	s.Require().Len(replies, 3)
	// reply[0]: GetAttr for /a.txt (len 6)
	r0 := replies[0].GetGetAttr()
	s.Require().NotNil(r0)
	s.Equal(uint64(6), r0.GetAttributes().GetIno())
	// reply[1]: OpenDir for /
	r1 := replies[1].GetOpenDir()
	s.Require().NotNil(r1)
	s.Require().Len(r1.Entries, 1)
	s.Equal("x", r1.Entries[0].Name)
	// reply[2]: GetAttr for /bb.txt (len 7)
	r2 := replies[2].GetGetAttr()
	s.Require().NotNil(r2)
	s.Equal(uint64(7), r2.GetAttributes().GetIno())
}

// TestDispatch_PerOpErrorDoesNotAbortBatch confirms a Go-error return from a
// sub-op is flattened into a status-variant reply at the matching index and
// the rest of the batch still produces typed replies.
func (s *CompoundSuite) TestDispatch_PerOpErrorDoesNotAbortBatch() {
	stub := &stubFsHandlers{
		getAttr: func(_ context.Context, r *proto.GetAttrRequest) (*proto.GetAttrReply, error) {
			if r.Path == "/missing" {
				return nil, status.Error(codes.NotFound, "no such volume")
			}
			return &proto.GetAttrReply{Attributes: &proto.Attr{Ino: 7}, Status: int32(fuse.OK)}, nil
		},
	}
	d := NewCompoundDispatcher(stub, 4)

	ops := []*proto.CompoundOp{
		{Op: &proto.CompoundOp_GetAttr{GetAttr: &proto.GetAttrRequest{Volume: "v", Path: "/missing"}}},
		{Op: &proto.CompoundOp_GetAttr{GetAttr: &proto.GetAttrRequest{Volume: "v", Path: "/exists"}}},
	}

	replies := d.Dispatch(context.Background(), ops)

	s.Require().Len(replies, 2)
	s.Equal(int32(fuse.ENOENT), replies[0].GetStatus(), "errored op should carry fuse status")
	s.Nil(replies[0].GetGetAttr(), "errored slot should not carry a typed reply")
	s.Require().NotNil(replies[1].GetGetAttr(), "second op should still succeed")
	s.Equal(uint64(7), replies[1].GetGetAttr().GetAttributes().GetIno())
}

// TestDispatch_RespectsParallelismCap drives 100 ops with maxParallel=4 and
// asserts the peak in-flight count never exceeded 4. Deterministic: each
// goroutine signals "entered" on readyCh; the test drains 4 entries, asserts
// the cap is observed, then unblocks via gate so the rest can run.
func (s *CompoundSuite) TestDispatch_RespectsParallelismCap() {
	const total = 100
	const cap = 4

	var inFlight atomic.Int32
	var peak atomic.Int32
	gate := make(chan struct{})
	readyCh := make(chan struct{}, total)

	stub := &stubFsHandlers{
		getAttr: func(_ context.Context, _ *proto.GetAttrRequest) (*proto.GetAttrReply, error) {
			cur := inFlight.Add(1)
			defer inFlight.Add(-1)
			for {
				prev := peak.Load()
				if cur <= prev || peak.CompareAndSwap(prev, cur) {
					break
				}
			}
			readyCh <- struct{}{}
			<-gate
			return &proto.GetAttrReply{Status: int32(fuse.OK)}, nil
		},
	}
	d := NewCompoundDispatcher(stub, cap)

	ops := make([]*proto.CompoundOp, total)
	for i := range ops {
		ops[i] = &proto.CompoundOp{Op: &proto.CompoundOp_GetAttr{GetAttr: &proto.GetAttrRequest{Volume: "v", Path: "/p"}}}
	}

	done := make(chan []*proto.CompoundReply, 1)
	go func() { done <- d.Dispatch(context.Background(), ops) }()

	// Wait until exactly cap goroutines have entered. They're blocked on
	// gate; the dispatcher's semaphore must prevent a 5th from entering.
	for i := 0; i < cap; i++ {
		<-readyCh
	}
	s.Equal(int32(cap), inFlight.Load(), "should have exactly cap goroutines in flight")
	s.Equal(int32(cap), peak.Load(), "peak should equal cap, not exceed it")

	// Release everyone. The rest of the batch will drain in waves of `cap`.
	close(gate)

	replies := <-done
	s.Require().Len(replies, total)
	s.LessOrEqual(int(peak.Load()), cap, "peak concurrency must never exceed cap")
	for i, r := range replies {
		s.Require().NotNil(r.GetGetAttr(), "reply %d missing GetAttr", i)
	}
}

// TestGrpcErrToFuseStatus covers the mapper directly — cheap, lets us pin
// the contract without spinning up the dispatcher for each code.
func (s *CompoundSuite) TestGrpcErrToFuseStatus() {
	cases := []struct {
		in   error
		want fuse.Status
	}{
		{nil, fuse.OK},
		{status.Error(codes.OK, ""), fuse.OK},
		{status.Error(codes.NotFound, "x"), fuse.ENOENT},
		{status.Error(codes.PermissionDenied, "x"), fuse.EACCES},
		{status.Error(codes.Unauthenticated, "x"), fuse.EACCES},
		{status.Error(codes.Unimplemented, "x"), fuse.ENOSYS},
		{status.Error(codes.InvalidArgument, "x"), fuse.EINVAL},
		{status.Error(codes.Unavailable, "x"), fuse.EIO},
		{status.Error(codes.Canceled, "x"), fuse.EINTR},
		{status.Error(codes.DeadlineExceeded, "x"), fuse.EINTR},
		{status.Error(codes.Internal, "x"), fuse.EIO},
	}
	for _, tc := range cases {
		s.Equal(tc.want, grpcErrToFuseStatus(tc.in), "for %v", tc.in)
	}
}
