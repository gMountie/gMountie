package io

import (
	"context"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	grpcmocks "go.gmountie.dev/gmountie/internal/mocks/pkg/client/grpc"
	mockProto "go.gmountie.dev/gmountie/internal/mocks/pkg/proto"
	grpcclient "go.gmountie.dev/gmountie/pkg/client/grpc"
	"go.gmountie.dev/gmountie/pkg/proto"

	"github.com/hanwen/go-fuse/v2/fuse"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"
	"google.golang.org/grpc"
)

type ReclaimFlagsSuite struct {
	suite.Suite
}

func (s *ReclaimFlagsSuite) TestStripsCreateExclTrunc() {
	in := uint32(syscall.O_RDWR | syscall.O_CREAT | syscall.O_EXCL | syscall.O_TRUNC)
	s.Equal(uint32(syscall.O_RDWR), sanitizeReopenFlags(in))
}

func (s *ReclaimFlagsSuite) TestPreservesAppend() {
	in := uint32(syscall.O_WRONLY | syscall.O_APPEND)
	s.Equal(uint32(syscall.O_WRONLY|syscall.O_APPEND), sanitizeReopenFlags(in))
}

func (s *ReclaimFlagsSuite) TestReadOnlyUnchanged() {
	s.Equal(uint32(syscall.O_RDONLY), sanitizeReopenFlags(uint32(syscall.O_RDONLY)))
}

func (s *ReclaimFlagsSuite) TestStripsCreateKeepsAppend() {
	in := uint32(syscall.O_WRONLY | syscall.O_CREAT | syscall.O_APPEND)
	s.Equal(uint32(syscall.O_WRONLY|syscall.O_APPEND), sanitizeReopenFlags(in))
}

func TestReclaimFlagsSuite(t *testing.T) {
	suite.Run(t, new(ReclaimFlagsSuite))
}

// ReclaimStaleSuite tests the reclaimIfStale method on grpcFileHandle.
type ReclaimStaleSuite struct {
	suite.Suite
}

// newStaleHandle builds a grpcFileHandle whose sessionID snapshot ("A") differs
// from the live session the mock client returns. The handle's fileClient is
// replaced with the supplied fileClient mock so Open() calls can be intercepted.
func (s *ReclaimStaleSuite) newStaleHandle(
	client *grpcmocks.MockClient,
	fileClient *mockProto.MockRpcFileClient,
	path string,
	fd uint64,
	flags uint32,
	snapshotSession string,
) *grpcFileHandle {
	h := newGrpcFileHandle(
		client, "vol", path, fd,
		flags,
		nil, /*caller — not the focus of these tests*/
		30*time.Second,
		snapshotSession,
		grpcclient.PerFileConfig{},
	)
	// Override the fileClient with the test mock so Open calls are interceptable.
	h.fileClient = fileClient
	return h
}

// TestReopensWhenStale: a handle whose snapshotted sessionID ("A") differs from
// the live client session ("B") must call File().Open() exactly once, update
// h.fd to the reply fd, update h.sessionID to the live session, and — critically
// — stamp the OPENER's identity (reopenCaller) on the reopen request rather than
// deriving a caller from the triggering op's context (which may be a detached
// context.Background() that carries no FUSE caller at all).
func (s *ReclaimStaleSuite) TestReopensWhenStale() {
	client := grpcmocks.NewMockClient(s.T())
	fileClient := mockProto.NewMockRpcFileClient(s.T())

	client.EXPECT().SessionID().Return("B").Maybe()
	client.EXPECT().File().Return(fileClient).Maybe()

	const wantFd = uint64(99)
	const flags = uint32(syscall.O_RDWR)
	const path = "/data/file.txt"

	// openerCaller is the identity captured at Open/Create time and stored on the
	// handle. The reopen must use this, not callerFromCtx(ctx) — the bug being
	// fixed: a detached context (e.g. streamingWrite) carries no FUSE caller.
	openerCaller := &proto.Caller{Owner: &proto.Owner{Uid: 1000, Gid: 2000}, Pid: 42}

	fileClient.EXPECT().Open(
		mock.Anything,
		mock.MatchedBy(func(req *proto.OpenRequest) bool {
			return req.Volume == "vol" &&
				req.Path == path &&
				req.Flags == sanitizeReopenFlags(flags) &&
				req.SessionId == "B" &&
				req.RequestId != "" &&
				req.Caller == openerCaller // must be the stored opener identity
		}),
		mock.Anything,
	).Return(&proto.OpenReply{Fd: wantFd, Status: int32(fuse.OK)}, nil).Once()

	h := s.newStaleHandle(client, fileClient, path, 7, flags, "A")
	h.reopenCaller = openerCaller // inject the opener's identity

	st := h.reclaimIfStale(context.Background())

	s.Require().Equal(fuse.OK, st)
	s.Assert().Equal(wantFd, h.state.Load().fd, "fd must be updated to the reply fd")
	s.Assert().Equal("B", h.state.Load().sessionID, "sessionID must be updated to the live session")
	fileClient.AssertNumberOfCalls(s.T(), "Open", 1)
}

// TestNoopWhenFresh: when the handle's snapshotted sessionID matches the live
// client session, reclaimIfStale must return OK without calling Open.
func (s *ReclaimStaleSuite) TestNoopWhenFresh() {
	client := grpcmocks.NewMockClient(s.T())
	fileClient := mockProto.NewMockRpcFileClient(s.T())

	client.EXPECT().SessionID().Return("A").Maybe()
	// newGrpcFileHandle calls client.File() once at construction to populate
	// h.fileClient; newStaleHandle then overwrites h.fileClient with our mock.
	// Allow that one construction-time call; Open must never be called.
	client.EXPECT().File().Return(fileClient).Once()

	h := s.newStaleHandle(client, fileClient, "/f", 7, uint32(syscall.O_RDONLY), "A")
	origFd := h.state.Load().fd

	st := h.reclaimIfStale(context.Background())

	s.Require().Equal(fuse.OK, st)
	s.Assert().Equal(origFd, h.state.Load().fd, "fd must be unchanged")
	s.Assert().Equal("A", h.state.Load().sessionID, "sessionID must be unchanged")
	fileClient.AssertNumberOfCalls(s.T(), "Open", 0)
}

// TestConcurrentReopenOnce: 8 goroutines all call reclaimIfStale on the same
// stale handle concurrently. Open must be called EXACTLY once (the mutex
// serializes re-checkers so only the first caller actually reopens).
func (s *ReclaimStaleSuite) TestConcurrentReopenOnce() {
	client := grpcmocks.NewMockClient(s.T())
	fileClient := mockProto.NewMockRpcFileClient(s.T())

	client.EXPECT().SessionID().Return("B").Maybe()
	client.EXPECT().File().Return(fileClient).Maybe()

	var openCount atomic.Int32
	fileClient.EXPECT().Open(mock.Anything, mock.Anything, mock.Anything).
		RunAndReturn(func(_ context.Context, _ *proto.OpenRequest, _ ...grpc.CallOption) (*proto.OpenReply, error) {
			openCount.Add(1)
			return &proto.OpenReply{Fd: 42, Status: int32(fuse.OK)}, nil
		}).Maybe()

	h := s.newStaleHandle(client, fileClient, "/f", 7, uint32(syscall.O_RDWR), "A")

	const n = 8
	var wg sync.WaitGroup
	wg.Add(n)
	for range n {
		go func() {
			defer wg.Done()
			st := h.reclaimIfStale(context.Background())
			s.Assert().Equal(fuse.OK, st)
		}()
	}
	wg.Wait()

	s.Assert().Equal(int32(1), openCount.Load(), "Open must be called exactly once despite concurrent callers")
	s.Assert().Equal(uint64(42), h.state.Load().fd, "fd must be updated")
	s.Assert().Equal("B", h.state.Load().sessionID, "sessionID must be updated")
}

func TestReclaimStaleSuite(t *testing.T) {
	suite.Run(t, new(ReclaimStaleSuite))
}
