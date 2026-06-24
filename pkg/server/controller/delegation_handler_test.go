package controller

import (
	"context"
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
)

// fakeRecallRecorder is a delegation.Recaller that records calls with
// a mutex so it is safe to read from the test goroutine while recalls
// fire on server goroutines.
type fakeRecallRecorder struct {
	mu    sync.Mutex
	calls []string // "owner:root"
}

func (f *fakeRecallRecorder) Recall(owner, root string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls = append(f.calls, owner+":"+root)
	return nil
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
	fsService  *mockservice.MockVolumeService
	sessionMgr service.SessionManager
	recaller   *fakeRecallRecorder
	arbiter    *delegation.Arbiter

	// Two real sessions with known principals.
	sidA string // holds the delegation in TestForeignMkdirRecallsHolder
	sidB string // the foreign contender

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

	s.recaller = &fakeRecallRecorder{}
	s.arbiter = delegation.NewArbiter(s.recaller, delegation.Config{
		Cooldown: delegation.CooldownConfigDefault(),
	}, time.Now, newFakeWatermarkStore())

	bus := serverio.NewLocalEventBus(serverio.EventBusOptions{BufferSize: 16})
	s.srv = NewGrpcServer(s.fsService, s.sessionMgr, bus, nil, s.arbiter, nil, nil)
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
