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
// from the live session the mock client returns. The handle's epoch is baked as
// "epoch-1"; tests that want the RESTART path set BootEpoch() to return a
// different value (e.g. "epoch-2"). Tests that want the REAP path set
// BootEpoch() to return "epoch-1" (matching the stored epoch). The handle's
// fileClient is replaced with the supplied fileClient mock so Open() calls can
// be intercepted.
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
		"epoch-1", // baked stored epoch; tests vary live BootEpoch() around this
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
	client.EXPECT().BootEpoch().Return("epoch-2").Maybe() // epoch changed → restart case
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
	s.Assert().Equal("epoch-2", h.state.Load().epoch, "epoch must be updated to the live boot epoch")
	fileClient.AssertNumberOfCalls(s.T(), "Open", 1)
}

// TestNoopWhenFresh: when the handle's snapshotted sessionID matches the live
// client session, reclaimIfStale must return OK without calling Open.
func (s *ReclaimStaleSuite) TestNoopWhenFresh() {
	client := grpcmocks.NewMockClient(s.T())
	fileClient := mockProto.NewMockRpcFileClient(s.T())

	client.EXPECT().SessionID().Return("A").Maybe()
	client.EXPECT().BootEpoch().Return("epoch-1").Maybe() // fast path returns before epoch read; maybe is safe
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
	client.EXPECT().BootEpoch().Return("epoch-2").Maybe() // epoch changed → restart case
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
	s.Assert().Equal("epoch-2", h.state.Load().epoch, "epoch must be updated to the live boot epoch")
}

// TestNoReclaimOnReap: when the session changes but the server boot epoch is
// UNCHANGED (same-process reap: the server reaped our idle session), reclaimIfStale
// must return OK WITHOUT calling File().Open. The dead fd will then surface a
// clean NotFound from the server — the "fail cleanly past grace" contract.
// This is the key invariant the epoch gate protects.
func (s *ReclaimStaleSuite) TestNoReclaimOnReap() {
	client := grpcmocks.NewMockClient(s.T())
	fileClient := mockProto.NewMockRpcFileClient(s.T())

	// Session changed (Resume failed → Create), but epoch is UNCHANGED → reap.
	client.EXPECT().SessionID().Return("B").Maybe()
	client.EXPECT().BootEpoch().Return("epoch-1").Maybe() // same as stored epoch → reap
	// newGrpcFileHandle calls client.File() once at construction; allow that.
	// Open must never be called.
	client.EXPECT().File().Return(fileClient).Once()

	h := s.newStaleHandle(client, fileClient, "/data/file.txt", 7, uint32(syscall.O_RDONLY), "A")
	origFd := h.state.Load().fd
	origSession := h.state.Load().sessionID
	origEpoch := h.state.Load().epoch

	st := h.reclaimIfStale(context.Background())

	s.Require().Equal(fuse.OK, st, "reap must return OK (let fd-op send the dead fd for a clean NotFound)")
	s.Assert().Equal(origFd, h.state.Load().fd, "fd must be unchanged on a reap")
	s.Assert().Equal(origSession, h.state.Load().sessionID, "sessionID must be unchanged on a reap")
	s.Assert().Equal(origEpoch, h.state.Load().epoch, "epoch must be unchanged on a reap")
	fileClient.AssertNumberOfCalls(s.T(), "Open", 0)
}

// TestReclaimOnRestart: when the session changes AND the server boot epoch
// changes (server process restarted), reclaimIfStale must call File().Open
// exactly once, update the stored fd, sessionID, and epoch to the new values.
func (s *ReclaimStaleSuite) TestReclaimOnRestart() {
	client := grpcmocks.NewMockClient(s.T())
	fileClient := mockProto.NewMockRpcFileClient(s.T())

	const wantFd = uint64(55)
	// Session changed AND epoch changed → server restarted.
	client.EXPECT().SessionID().Return("B").Maybe()
	client.EXPECT().BootEpoch().Return("epoch-2").Maybe() // epoch changed → restart
	client.EXPECT().File().Return(fileClient).Maybe()

	fileClient.EXPECT().Open(
		mock.Anything,
		mock.MatchedBy(func(req *proto.OpenRequest) bool {
			return req.Volume == "vol" &&
				req.Path == "/data/file.txt" &&
				req.SessionId == "B" &&
				req.RequestId != ""
		}),
		mock.Anything,
	).Return(&proto.OpenReply{Fd: wantFd, Status: int32(fuse.OK)}, nil).Once()

	h := s.newStaleHandle(client, fileClient, "/data/file.txt", 7, uint32(syscall.O_RDONLY), "A")

	st := h.reclaimIfStale(context.Background())

	s.Require().Equal(fuse.OK, st)
	s.Assert().Equal(wantFd, h.state.Load().fd, "fd must be updated to the reply fd")
	s.Assert().Equal("B", h.state.Load().sessionID, "sessionID must be updated to the live session")
	s.Assert().Equal("epoch-2", h.state.Load().epoch, "epoch must be updated to the live boot epoch")
	fileClient.AssertNumberOfCalls(s.T(), "Open", 1)
}

func TestReclaimStaleSuite(t *testing.T) {
	suite.Run(t, new(ReclaimStaleSuite))
}
