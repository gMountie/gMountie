package transport

import (
	"context"
	"sync"
	"testing"
	"time"

	grpcmocks "go.gmountie.dev/gmountie/internal/mocks/pkg/client/grpc"
	mockProto "go.gmountie.dev/gmountie/internal/mocks/pkg/proto"
	"go.gmountie.dev/gmountie/pkg/client/backend"
	grpcclient "go.gmountie.dev/gmountie/pkg/client/grpc"
	"go.gmountie.dev/gmountie/pkg/proto"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"
)

// fakeDelegationHook is a test double for DelegationHook. RequestedRoot
// returns a fixed root string; Apply records every grant delivered so tests
// can assert on them.
type fakeDelegationHook struct {
	mu     sync.Mutex
	grants []*proto.DelegationGrant
	root   string
	epoch  string
}

func newFakeHook(root string) *fakeDelegationHook { return &fakeDelegationHook{root: root} }

func (f *fakeDelegationHook) RequestedRoot() string { return f.root }
func (f *fakeDelegationHook) WalEpoch() string      { return f.epoch }

func (f *fakeDelegationHook) Apply(g *proto.DelegationGrant) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.grants = append(f.grants, g)
}

// receivedGrants returns the grants delivered to the hook so far. Thread-safe.
func (f *fakeDelegationHook) receivedGrants() []*proto.DelegationGrant {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]*proto.DelegationGrant, len(f.grants))
	copy(out, f.grants)
	return out
}

// DelegationHookSuite tests the transport.DelegationHook seam using the same
// stub harness as BackendClientTestSuite (MockRpcFsClient / MockRpcFileClient
// wired through a MockClient).
type DelegationHookSuite struct {
	suite.Suite
	client     *grpcmocks.MockClient
	fsClient   *mockProto.MockRpcFsClient
	fileClient *mockProto.MockRpcFileClient
}

func (s *DelegationHookSuite) SetupTest() {
	s.client = grpcmocks.NewMockClient(s.T())
	s.fsClient = mockProto.NewMockRpcFsClient(s.T())
	s.fileClient = mockProto.NewMockRpcFileClient(s.T())
	s.client.EXPECT().Fs().Return(s.fsClient).Maybe()
	s.client.EXPECT().File().Return(s.fileClient).Maybe()
	s.client.EXPECT().DataFileClient().Return(s.fileClient, func() {}).Maybe()
	s.client.EXPECT().MetaTimeout().Return(2 * time.Second).Maybe()
	s.client.EXPECT().IOTimeout().Return(30 * time.Second).Maybe()
	s.client.EXPECT().SessionID().Return("test-session").Maybe()
	s.client.EXPECT().BootEpoch().Return("epoch-1").Maybe()
	s.client.EXPECT().RetryWindow().Return(2 * time.Second).Maybe()
	s.client.EXPECT().Lifetime().Return(context.Background()).Maybe()
	s.client.EXPECT().Metrics().Return(nil).Maybe()
	s.client.EXPECT().PerFileConfig().Return(grpcclient.PerFileConfig{}).Maybe()
}

// newBackend constructs a BackendClient with the given options against the
// shared stub client.
func (s *DelegationHookSuite) newBackend(opts ...BackendOption) *BackendClient {
	return NewBackendClient(s.client, "testVol", opts...)
}

// --- Mkdir: request carries Delegation.Root, reply Grant delivered ---

// TestMkdir_HookStampsRequestAndDeliversGrant asserts:
// (a) the MkdirRequest sent to the stub carries Delegation.Root == hook root,
// (b) the DelegationGrant from the server reply is delivered to hook.Apply.
func (s *DelegationHookSuite) TestMkdir_HookStampsRequestAndDeliversGrant() {
	const fixedRoot = "/mnt/test-root"
	hook := newFakeHook(fixedRoot)
	b := s.newBackend(WithDelegationHook(hook))

	grant := &proto.DelegationGrant{GrantedRoot: "/mnt/test-root", ExcludedPaths: []string{"/mnt/test-root/tmp"}}

	var capturedDelegation *proto.DelegationRequest
	s.fsClient.EXPECT().Mkdir(mock.Anything,
		mock.MatchedBy(func(req *proto.MkdirRequest) bool {
			capturedDelegation = req.Delegation
			return true
		}),
		mock.Anything,
	).Return(&proto.MkdirReply{
		Status: proto.FsError_FS_OK,
		Grant:  grant,
	}, nil).Once()

	_, st := b.Mkdir(context.Background(), "/d", 0o755)
	s.Require().Equal(proto.FsError_FS_OK, st)

	// (a) request must carry the hook root.
	s.Require().NotNil(capturedDelegation, "request must carry Delegation when hook is set")
	s.Assert().Equal(fixedRoot, capturedDelegation.Root)

	// (b) grant delivered to hook.
	grants := hook.receivedGrants()
	s.Require().Len(grants, 1, "Apply must be called once")
	s.Assert().Equal("/mnt/test-root", grants[0].GrantedRoot)
	s.Assert().Equal([]string{"/mnt/test-root/tmp"}, grants[0].ExcludedPaths)
}

// TestMkdir_NoHook_NoDelegationSet: without a hook the request must NOT carry
// a Delegation field and no panic must occur (nil-default unchanged behaviour).
func (s *DelegationHookSuite) TestMkdir_NoHook_NoDelegationSet() {
	b := s.newBackend() // no WithDelegationHook

	var capturedDelegation *proto.DelegationRequest
	s.fsClient.EXPECT().Mkdir(mock.Anything,
		mock.MatchedBy(func(req *proto.MkdirRequest) bool {
			capturedDelegation = req.Delegation
			return true
		}),
		mock.Anything,
	).Return(&proto.MkdirReply{Status: proto.FsError_FS_OK}, nil).Once()

	s.Require().NotPanics(func() {
		_, st := b.Mkdir(context.Background(), "/d", 0o755)
		s.Assert().Equal(proto.FsError_FS_OK, st)
	})
	s.Assert().Nil(capturedDelegation, "no hook → Delegation must be nil (today's wire)")
}

// --- SetAttr: request carries Delegation.Root, reply Grant delivered ---

// TestSetAttr_HookStampsRequestAndDeliversGrant mirrors the Mkdir test for
// SetAttr, providing the ≥2 op coverage called for in the task brief.
func (s *DelegationHookSuite) TestSetAttr_HookStampsRequestAndDeliversGrant() {
	const fixedRoot = "/storage/bucket"
	hook := newFakeHook(fixedRoot)
	b := s.newBackend(WithDelegationHook(hook))

	grant := &proto.DelegationGrant{GrantedRoot: "/storage/bucket"}

	var capturedDelegation *proto.DelegationRequest
	s.fsClient.EXPECT().SetAttr(mock.Anything,
		mock.MatchedBy(func(req *proto.SetAttrRequest) bool {
			capturedDelegation = req.Delegation
			return true
		}),
		mock.Anything,
	).Return(&proto.SetAttrReply{
		Status: proto.FsError_FS_OK,
		Grant:  grant,
		Attributes: &proto.Attr{
			Ino:   1,
			Owner: &proto.Owner{Uid: 1000, Gid: 1000},
		},
	}, nil).Once()

	_, st := b.SetAttr(context.Background(), "/f", backendSetAttrIn())
	s.Require().Equal(proto.FsError_FS_OK, st)

	s.Require().NotNil(capturedDelegation, "request must carry Delegation when hook is set")
	s.Assert().Equal(fixedRoot, capturedDelegation.Root)

	grants := hook.receivedGrants()
	s.Require().Len(grants, 1)
	s.Assert().Equal("/storage/bucket", grants[0].GrantedRoot)
}

// TestSetAttr_NoHook_NoDelegationSet: nil hook → no Delegation, no panic.
func (s *DelegationHookSuite) TestSetAttr_NoHook_NoDelegationSet() {
	b := s.newBackend()

	var capturedDelegation *proto.DelegationRequest
	s.fsClient.EXPECT().SetAttr(mock.Anything,
		mock.MatchedBy(func(req *proto.SetAttrRequest) bool {
			capturedDelegation = req.Delegation
			return true
		}),
		mock.Anything,
	).Return(&proto.SetAttrReply{
		Status: proto.FsError_FS_OK,
		Attributes: &proto.Attr{
			Ino:   1,
			Owner: &proto.Owner{},
		},
	}, nil).Once()

	s.Require().NotPanics(func() {
		_, st := b.SetAttr(context.Background(), "/f", backendSetAttrIn())
		s.Assert().Equal(proto.FsError_FS_OK, st)
	})
	s.Assert().Nil(capturedDelegation)
}

// --- Create: request carries Delegation.Root, reply Grant delivered ---

// TestCreate_HookStampsRequestAndDeliversGrant: Create uses an inline
// retryOp (not mutatePath) — this asserts the delegation stamp is also wired
// for that different code path.
func (s *DelegationHookSuite) TestCreate_HookStampsRequestAndDeliversGrant() {
	const fixedRoot = "/data/writes"
	hook := newFakeHook(fixedRoot)
	b := s.newBackend(WithDelegationHook(hook))

	grant := &proto.DelegationGrant{GrantedRoot: "/data/writes"}

	var capturedDelegation *proto.DelegationRequest
	s.fileClient.EXPECT().Create(mock.Anything,
		mock.MatchedBy(func(req *proto.CreateRequest) bool {
			capturedDelegation = req.Delegation
			return true
		}),
		mock.Anything,
	).Return(&proto.CreateReply{
		Status: proto.FsError_FS_OK,
		Fd:     3,
		Grant:  grant,
	}, nil).Once()

	fh, _, st := b.Create(context.Background(), "/dir", "newfile.txt", 0, 0o644)
	s.Require().Equal(proto.FsError_FS_OK, st)
	s.Require().NotNil(fh)

	s.Require().NotNil(capturedDelegation)
	s.Assert().Equal(fixedRoot, capturedDelegation.Root)

	grants := hook.receivedGrants()
	s.Require().Len(grants, 1)
	s.Assert().Equal("/data/writes", grants[0].GrantedRoot)
}

// --- Write: first-frame stamp only, grant from CloseAndRecv ---

// TestWrite_HookStampsFirstFrameOnly: the header (first) frame must carry
// Delegation; subsequent data frames must have nil Delegation.
// The grant returned by CloseAndRecv is delivered to hook.Apply.
func (s *DelegationHookSuite) TestWrite_HookStampsFirstFrameOnly() {
	const fixedRoot = "/wal-root"
	hook := newFakeHook(fixedRoot)
	b := s.newBackend(WithDelegationHook(hook))

	// payload spans 2 frames (first = writeFrameSizeBytes, second = 1 byte).
	payload := make([]byte, writeFrameSizeBytes+1)

	grant := &proto.DelegationGrant{GrantedRoot: "/wal-root"}
	stub := newBackendWriteStreamStub(s.T(), &proto.WriteReply{
		Written: uint32(len(payload)),
		Status:  proto.FsError_FS_OK,
		Grant:   grant,
	}, nil)
	s.fileClient.EXPECT().Write(mock.Anything, mock.Anything).Return(stub, nil).Once()

	h := newGrpcFileHandle(s.client, "testVol", "/f", 1, 0, nil, 30*time.Second, "test-session", "epoch-1", grpcclient.PerFileConfig{})
	written, st := b.Write(context.Background(), h, 0, payload)
	s.Require().Equal(proto.FsError_FS_OK, st)
	s.Assert().Equal(uint32(len(payload)), written)

	s.Require().Len(stub.frames, 2, "expected 2 frames for the 2-chunk payload")
	// First frame: must carry delegation.
	s.Require().NotNil(stub.frames[0].Delegation, "first frame must carry Delegation")
	s.Assert().Equal(fixedRoot, stub.frames[0].Delegation.Root)
	// Second frame: must NOT carry delegation.
	s.Assert().Nil(stub.frames[1].Delegation, "subsequent frames must not carry Delegation")

	grants := hook.receivedGrants()
	s.Require().Len(grants, 1)
	s.Assert().Equal("/wal-root", grants[0].GrantedRoot)
}

// TestWrite_NoHook_FirstFrameNoDelegation: no hook → first frame Delegation
// stays nil; no panic.
func (s *DelegationHookSuite) TestWrite_NoHook_FirstFrameNoDelegation() {
	b := s.newBackend()

	payload := []byte("hello")
	stub := newBackendWriteStreamStub(s.T(), &proto.WriteReply{Written: uint32(len(payload)), Status: proto.FsError_FS_OK}, nil)
	s.fileClient.EXPECT().Write(mock.Anything, mock.Anything).Return(stub, nil).Once()

	h := newGrpcFileHandle(s.client, "testVol", "/f", 1, 0, nil, 30*time.Second, "test-session", "epoch-1", grpcclient.PerFileConfig{})
	s.Require().NotPanics(func() {
		written, st := b.Write(context.Background(), h, 0, payload)
		s.Assert().Equal(proto.FsError_FS_OK, st)
		s.Assert().Equal(uint32(len(payload)), written)
	})
	s.Require().Len(stub.frames, 1)
	s.Assert().Nil(stub.frames[0].Delegation)
}

// --- Grant not delivered on non-OK reply ---

// TestMkdir_HookApplyNotCalledOnError: when the server returns a non-OK
// status, Apply must NOT be called even if the reply carries a Grant.
func (s *DelegationHookSuite) TestMkdir_HookApplyNotCalledOnError() {
	hook := newFakeHook("/root")
	b := s.newBackend(WithDelegationHook(hook))

	// Server returns EACCES with a grant piggyback — Apply must still be skipped.
	s.fsClient.EXPECT().Mkdir(mock.Anything, mock.Anything, mock.Anything).
		Return(&proto.MkdirReply{
			Status: proto.FsError_FS_EACCES,
			Grant:  &proto.DelegationGrant{GrantedRoot: "/root"},
		}, nil).Once()

	_, st := b.Mkdir(context.Background(), "/denied", 0o755)
	s.Assert().Equal(proto.FsError_FS_EACCES, st)
	s.Assert().Empty(hook.receivedGrants(), "Apply must not be called on a non-OK reply")
}

// backendSetAttrIn returns a minimal SetAttrIn that exercises a valid wire
// path (FATTR_MODE bit set, no timestamps) — avoids the FATTR_MTIME nil-guard.
func backendSetAttrIn() backend.SetAttrIn {
	return backend.SetAttrIn{Valid: backend.FATTR_MODE, Mode: 0o644}
}

func TestDelegationHookSuite(t *testing.T) {
	suite.Run(t, new(DelegationHookSuite))
}
