package controller

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	mockservice "go.gmountie.dev/gmountie/internal/mocks/pkg/server/service"
	"go.gmountie.dev/gmountie/pkg/proto"
	"go.gmountie.dev/gmountie/pkg/server/delegation"
	serverio "go.gmountie.dev/gmountie/pkg/server/io"
	"go.gmountie.dev/gmountie/pkg/server/service"

	pathfsmock "go.gmountie.dev/gmountie/internal/mocks/github.com/hanwen/go-fuse/v2/fuse/pathfs"

	"github.com/hanwen/go-fuse/v2/fuse"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// fakeRecallRecorder is a delegation.Recaller that records calls with
// a mutex so it is safe to read from the test goroutine while recalls
// fire on server goroutines.
type fakeRecallRecorder struct {
	mu    sync.Mutex
	calls []string // "owner:root"
	err   error    // returned by Recall when non-nil; simulates a failed/timed-out recall
}

func (f *fakeRecallRecorder) Recall(owner, root string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls = append(f.calls, owner+":"+root)
	return f.err
}

// Calls returns a snapshot of recorded recall invocations.
func (f *fakeRecallRecorder) Calls() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]string, len(f.calls))
	copy(out, f.calls)
	return out
}

// DelegationHandlerSuite tests that every mutating handler:
//
//  1. Calls arbitrateContention (fires a recall when a foreign delegation covers
//     the path).
//  2. Stamps a DelegationGrant on the success reply when a piggybacked
//     DelegationRequest is present.
type DelegationHandlerSuite struct {
	suite.Suite
	srv        *RpcServerImpl
	fileServer *RpcFileServerImpl
	fsService  *mockservice.MockVolumeService
	sessionMgr service.SessionManager
	recaller   *fakeRecallRecorder
	arbiter    *delegation.Arbiter

	// Two real sessions with known principals.
	sidA string // holds the delegation in TestForeignMkdirRecallsHolder
	sidB string // the foreign contender

	// principals maps a session id to the principal it was created with, so
	// grantTo/ctxForSession/ctxFor can resolve the right identity without
	// hardcoding it at each call site.
	principals map[string]string

	vol    string
	caller *proto.Caller
}

func TestDelegationHandlerSuite(t *testing.T) {
	suite.Run(t, new(DelegationHandlerSuite))
}

func (s *DelegationHandlerSuite) SetupTest() {
	s.vol = "testVol"
	s.caller = CreateCaller(0, 0, 0)

	s.fsService = new(mockservice.MockVolumeService)
	s.fsService.On("ResolveIdentity", mock.Anything, mock.Anything, mock.Anything).
		Return(service.Identity{}, nil).Maybe()

	s.sessionMgr = service.NewSessionManager(service.SessionManagerOptions{})

	var err error
	s.sidA, err = s.sessionMgr.Create("userA", "")
	s.Require().NoError(err)
	s.sidB, err = s.sessionMgr.Create("userB", "")
	s.Require().NoError(err)
	s.principals = map[string]string{s.sidA: "userA", s.sidB: "userB"}

	s.recaller = &fakeRecallRecorder{}
	s.arbiter = delegation.NewArbiter(s.recaller, delegation.Config{
		Cooldown: delegation.CooldownConfigDefault(),
	}, time.Now, newFakeWatermarkStore())

	bus := serverio.NewLocalEventBus(serverio.EventBusOptions{BufferSize: 16})
	s.srv = NewGrpcServer(s.fsService, s.sessionMgr, bus, nil, s.arbiter, nil, nil)
	s.fileServer = NewRpcFileServer(s.fsService, s.sessionMgr, nil, 1<<20, bus, s.arbiter)
}

// grantTo grants owner sessionID a delegation rooted at root, resolving the
// session's principal from s.principals so callers don't have to.
func (s *DelegationHandlerSuite) grantTo(sessionID, root string) {
	s.arbiter.Request(sessionID, root, s.principals[sessionID], s.vol, "")
}

// ctxForSession returns a context carrying sessionID in gRPC incoming
// metadata (what sessionIDFromContext reads) AND the session's principal —
// for read handlers (fs.go) that resolve the contender via ctx rather than a
// request.SessionId field.
func (s *DelegationHandlerSuite) ctxForSession(sessionID string) context.Context {
	return ctxWithSession(testAuthedCtx(s.principals[sessionID]), sessionID)
}

func (s *DelegationHandlerSuite) TearDownTest() {
	_ = s.sessionMgr.Stop(context.Background())
}

// ctxFor returns a context authenticated as the given session's principal.
func (s *DelegationHandlerSuite) ctxFor(sid string) context.Context {
	if sid == s.sidA {
		return testAuthedCtx("userA")
	}
	return testAuthedCtx("userB")
}

// TestForeignMkdirRecallsHolder: sessA holds a delegation on "d"; sessB's
// Mkdir under "d" must trigger a recall of sessA's delegation.
func (s *DelegationHandlerSuite) TestForeignMkdirRecallsHolder() {
	// sessA holds a delegation on "d" (granted via piggybacked request).
	s.arbiter.Request(s.sidA, "d", "userA", s.vol, "")

	// Wire the filesystem mock.
	mockFs := new(pathfsmock.MockFileSystem)
	s.fsService.On("BindIdentity", mock.Anything, s.vol, mock.Anything).
		Return(mockFs, service.Identity{}, nil)
	mockFs.EXPECT().Mkdir("d/sub", uint32(0), mock.Anything).Return(fuse.OK)
	mockFs.EXPECT().GetAttr("d/sub", mock.Anything).Return(&fuse.Attr{}, fuse.OK).Maybe()

	_, err := s.srv.Mkdir(s.ctxFor(s.sidB), &proto.MkdirRequest{
		Volume:    s.vol,
		Caller:    s.caller,
		Path:      "d/sub",
		SessionId: s.sidB,
		RequestId: "r-mkdir-recall",
	})
	s.Require().NoError(err)

	// The recall should have fired for sidA on root "d".
	s.Equal([]string{s.sidA + ":d"}, s.recaller.Calls())
}

// TestPiggybackedRequestReturnsGrant: a MkdirRequest with a piggybacked
// DelegationRequest{Root: "proj"} must return a grant in the reply.
func (s *DelegationHandlerSuite) TestPiggybackedRequestReturnsGrant() {
	mockFs := new(pathfsmock.MockFileSystem)
	s.fsService.On("BindIdentity", mock.Anything, s.vol, mock.Anything).
		Return(mockFs, service.Identity{}, nil)
	mockFs.EXPECT().Mkdir("proj/x", uint32(0), mock.Anything).Return(fuse.OK)
	mockFs.EXPECT().GetAttr("proj/x", mock.Anything).Return(&fuse.Attr{}, fuse.OK).Maybe()

	reply, err := s.srv.Mkdir(s.ctxFor(s.sidA), &proto.MkdirRequest{
		Volume:     s.vol,
		Caller:     s.caller,
		Path:       "proj/x",
		SessionId:  s.sidA,
		RequestId:  "r-mkdir-grant",
		Delegation: &proto.DelegationRequest{Root: "proj"},
	})
	s.Require().NoError(err)
	s.Require().NotNil(reply)
	s.Equal("proj", reply.Grant.GetGrantedRoot())
}

// ---- Phase-2 read arbitration: a WAL holder may have deferred state a
// reader would otherwise miss, so reads must arbitrate like writes. ----

// TestGetAttrRecallsForeignDelegation: sidA holds a delegation on "proj";
// sidB's GetAttr under "proj" must trigger a recall of sidA's delegation, and
// the read must still proceed (succeed) once the recall completes.
func (s *DelegationHandlerSuite) TestGetAttrRecallsForeignDelegation() {
	s.grantTo(s.sidA, "proj")

	mockFs := new(pathfsmock.MockFileSystem)
	s.fsService.On("BindIdentity", mock.Anything, s.vol, mock.Anything).
		Return(mockFs, service.Identity{}, nil)
	mockFs.EXPECT().GetAttr("proj/f.txt", mock.Anything).Return(&fuse.Attr{}, fuse.OK).Once()

	reply, err := s.srv.GetAttr(s.ctxForSession(s.sidB), &proto.GetAttrRequest{
		Volume: s.vol,
		Caller: s.caller,
		Path:   "proj/f.txt",
	})
	s.Require().NoError(err)
	s.Require().NotNil(reply)
	s.Equal(proto.FsError_FS_OK, reply.Status, "read proceeds once the foreign delegation is recalled")
	s.Equal([]string{s.sidA + ":proj"}, s.recaller.Calls(), "a foreign read must trigger a recall")
}

// TestGetAttrSelfAccessDoesNotRecall: sidA reading its own delegated subtree
// never recalls (OnMutation is a no-op when owner == contender).
func (s *DelegationHandlerSuite) TestGetAttrSelfAccessDoesNotRecall() {
	s.grantTo(s.sidA, "proj")

	mockFs := new(pathfsmock.MockFileSystem)
	s.fsService.On("BindIdentity", mock.Anything, s.vol, mock.Anything).
		Return(mockFs, service.Identity{}, nil)
	mockFs.EXPECT().GetAttr("proj/f.txt", mock.Anything).Return(&fuse.Attr{}, fuse.OK).Once()

	reply, err := s.srv.GetAttr(s.ctxForSession(s.sidA), &proto.GetAttrRequest{
		Volume: s.vol,
		Caller: s.caller,
		Path:   "proj/f.txt",
	})
	s.Require().NoError(err)
	s.Require().NotNil(reply)
	s.Equal(proto.FsError_FS_OK, reply.Status)
	s.Empty(s.recaller.Calls(), "the holder's own reads never recall its delegation")
}

// TestGetAttrRecallFailureReturnsEAGAIN: when the recall RTT fails, the read
// must fail closed with FS_EAGAIN carried in the reply (not a transport
// error) — the contender backs off and retries.
func (s *DelegationHandlerSuite) TestGetAttrRecallFailureReturnsEAGAIN() {
	s.grantTo(s.sidA, "proj")
	s.recaller.err = errors.New("recall timed out")

	mockFs := new(pathfsmock.MockFileSystem)
	s.fsService.On("BindIdentity", mock.Anything, s.vol, mock.Anything).
		Return(mockFs, service.Identity{}, nil)
	// GetAttr must never reach the filesystem: arbitration short-circuits first.

	reply, err := s.srv.GetAttr(s.ctxForSession(s.sidB), &proto.GetAttrRequest{
		Volume: s.vol,
		Caller: s.caller,
		Path:   "proj/f.txt",
	})
	s.Require().NoError(err, "recall failure surfaces in-band, not as a transport error")
	s.Require().NotNil(reply)
	s.Equal(proto.FsError_FS_EAGAIN, reply.Status)
	mockFs.AssertNotCalled(s.T(), "GetAttr", mock.Anything, mock.Anything)
}

// TestGetAttrIfChangedRecallFailureReturnsUnavailable: when a foreign
// delegation's recall fails, GetAttrIfChanged must surface codes.Unavailable
// (not an in-band FS_EAGAIN, since the proto reply has no Status field) so
// the client retries the entire RPC including arbitration within rpc.retry_window.
func (s *DelegationHandlerSuite) TestGetAttrIfChangedRecallFailureReturnsUnavailable() {
	s.grantTo(s.sidA, "proj")
	s.recaller.err = errors.New("recall timed out")

	mockFs := new(pathfsmock.MockFileSystem)
	s.fsService.On("BindIdentity", mock.Anything, s.vol, mock.Anything).
		Return(mockFs, service.Identity{}, nil)
	// GetAttr must never reach the filesystem: arbitration short-circuits first.

	_, err := s.srv.GetAttrIfChanged(s.ctxForSession(s.sidB), &proto.GetAttrIfChangedRequest{
		Volume:       s.vol,
		Caller:       s.caller,
		Path:         "proj/f.txt",
		KnownVersion: 0,
	})
	s.Require().Error(err, "recall failure surfaces as a transport error")

	// Verify it's a codes.Unavailable gRPC status error.
	st, ok := status.FromError(err)
	s.Require().True(ok, "error should be a gRPC status error")
	s.Equal(codes.Unavailable, st.Code(), "recall failure returns codes.Unavailable")

	mockFs.AssertNotCalled(s.T(), "GetAttr", mock.Anything, mock.Anything)
}

// TestLseekRecallsForeignDelegation: sidA holds a delegation on "proj"; sidB
// opens an fd under "proj" and calls Lseek — arbitration must key off
// entry.Path (the fd-bound path), not any path field on the request, and
// recall sidA's delegation before the seek proceeds.
func (s *DelegationHandlerSuite) TestLseekRecallsForeignDelegation() {
	s.grantTo(s.sidA, "proj")

	p := filepath.Join(s.T().TempDir(), "f.txt")
	s.Require().NoError(os.WriteFile(p, []byte("0123456789"), 0o644))
	f, err := os.OpenFile(p, os.O_RDWR, 0)
	s.Require().NoError(err)
	rf := serverio.NewRawFdFile(f)
	s.T().Cleanup(func() { rf.Release() })

	sess, err := s.sessionMgr.Get(s.sidB)
	s.Require().NoError(err)
	fd := sess.RegisterFile(s.vol, "proj/f.txt", rf)

	reply, err := s.fileServer.Lseek(s.ctxFor(s.sidB), &proto.LseekRequest{
		Volume:    s.vol,
		Fd:        fd,
		Offset:    0,
		Whence:    proto.SeekWhence_SEEK_WHENCE_DATA,
		SessionId: s.sidB,
	})
	s.Require().NoError(err)
	s.Require().NotNil(reply)
	s.Equal(proto.FsError_FS_OK, reply.Status)
	s.Equal([]string{s.sidA + ":proj"}, s.recaller.Calls(), "a foreign fd-based read must recall using entry.Path")
}
