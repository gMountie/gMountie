package transport

// wal_drain_test.go — tests for WithWriteDrain and the WriteDrain seam (Task 10a).
//
// Tests:
//   1. WithWriteDrain: a WriteDrain injected via the option is called by walHandle.flush
//      (via Flush) with the correct path, data, and offset — verifies the wiring.
//   2. No option (nil): the default wire behavior holds — newWalHandle falls back to
//      writeDrainToWire (the characterization suite covers this more thoroughly, but
//      a targeted wiring test here makes the seam self-documenting).
//   3. WithWriteDrain(nil): passing nil must be a no-op (field stays nil → wire default).

import (
	"context"
	"testing"
	"time"

	grpcmocks "go.gmountie.dev/gmountie/internal/mocks/pkg/client/grpc"
	mockProto "go.gmountie.dev/gmountie/internal/mocks/pkg/proto"
	grpcclient "go.gmountie.dev/gmountie/pkg/client/grpc"
	"go.gmountie.dev/gmountie/pkg/proto"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"
)

// captureWriteDrain is a test double for WriteDrain that records the Drain call
// arguments and delegates to the wireFlush callback (pass-through by default).
type captureWriteDrain struct {
	called    bool
	captPath  string
	captData  []byte
	captOff   int64
	captReqID string
}

func (d *captureWriteDrain) Drain(
	ctx context.Context,
	path string,
	pendingData []byte,
	pendingOff int64,
	requestID string,
	wireFlush func(ctx context.Context, data []byte, off int64, reqID string) proto.FsError,
) proto.FsError {
	d.called = true
	d.captPath = path
	d.captData = append([]byte(nil), pendingData...)
	d.captOff = pendingOff
	d.captReqID = requestID
	// Delegate to wire so we don't need a full mock stack just for routing verification.
	return wireFlush(ctx, pendingData, pendingOff, requestID)
}

// WriteDrainWiringSuite tests the WithWriteDrain option and the path-carrying
// WriteDrain.Drain signature.
type WriteDrainWiringSuite struct {
	suite.Suite
	client     *grpcmocks.MockClient
	fileClient *mockProto.MockRpcFileClient
	backend    *BackendClient
	drain      *captureWriteDrain
}

func (s *WriteDrainWiringSuite) SetupTest() {
	s.client = grpcmocks.NewMockClient(s.T())
	s.fileClient = mockProto.NewMockRpcFileClient(s.T())
	s.client.EXPECT().Fs().Return(nil).Maybe()
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

	s.drain = &captureWriteDrain{}
	s.backend = NewBackendClient(s.client, "testVolume", WithWriteDrain(s.drain))
}

// newHandle constructs a handle with the given config + the backend's wired drain.
// Mirrors BackendClientTestSuite.newHandle but uses b.writeDrain (the real wired one).
func (s *WriteDrainWiringSuite) newHandle(path string, cfg grpcclient.PerFileConfig) *walHandle {
	leaf := newGrpcFileHandle(s.client, "testVolume", path, 1, 0, nil, 30*time.Second, "test-session", "epoch-1", cfg)
	return newWalHandle(s.backend, leaf, cfg.WriteCoalesceBytes, s.backend.writeDrain)
}

// TestWithWriteDrain_DrainIsCalledWithPath verifies that when a WriteDrain is
// wired via WithWriteDrain and a handle is flushed, the drain receives the
// correct path, pending bytes, and offset.
func (s *WriteDrainWiringSuite) TestWithWriteDrain_DrainIsCalledWithPath() {
	const path = "/work/file.dat"
	h := s.newHandle(path, grpcclient.PerFileConfig{WriteCoalesceBytes: 4096})

	pending := []byte("write-drain wiring")

	// Expect the WriteAndFlush RPC that our captureWriteDrain delegates to.
	s.fileClient.EXPECT().WriteAndFlush(
		mock.Anything,
		mock.MatchedBy(func(r *proto.WriteAndFlushRequest) bool {
			return r.Offset == 5 && string(r.Data) == string(pending)
		}),
		mock.Anything,
	).Return(&proto.WriteAndFlushReply{
		Status:  proto.FsError_FS_OK,
		Written: uint32(len(pending)),
	}, nil).Once()

	// Write and then flush to trigger the drain.
	h.dirty.Store(true) // force flush to run (skip the clean-handle fast-path)
	if h.coalescer != nil {
		h.coalescer.Append(pending, 5)
	}

	st := h.flush(context.Background())
	s.Require().Equal(proto.FsError_FS_OK, st)

	// Verify the drain was called with the right path, data, and offset.
	s.Assert().True(s.drain.called, "custom WriteDrain must be called on flush")
	s.Assert().Equal(path, s.drain.captPath, "Drain must receive the handle's file path")
	s.Assert().Equal(pending, s.drain.captData, "Drain must receive the pending bytes")
	s.Assert().Equal(int64(5), s.drain.captOff, "Drain must receive the correct offset")
	s.Assert().NotEmpty(s.drain.captReqID, "Drain must receive a non-empty requestID")
}

// TestWithWriteDrain_NilOptionIsNoop verifies that WithWriteDrain(nil) leaves
// the backend's writeDrain field nil, so newWalHandle falls through to
// writeDrainToWire (the wire default).
func (s *WriteDrainWiringSuite) TestWithWriteDrain_NilOptionIsNoop() {
	b := NewBackendClient(s.client, "testVolume", WithWriteDrain(nil))
	s.Assert().Nil(b.writeDrain, "WithWriteDrain(nil) must be a no-op (field stays nil)")
}

// TestWithWriteDrain_UnsetUsesWireDefault verifies that a BackendClient
// constructed WITHOUT WithWriteDrain has a nil writeDrain field, so
// newWalHandle falls through to writeDrainToWire.
func (s *WriteDrainWiringSuite) TestWithWriteDrain_UnsetUsesWireDefault() {
	b := NewBackendClient(s.client, "testVolume") // no WithWriteDrain
	s.Assert().Nil(b.writeDrain, "absent WithWriteDrain must leave writeDrain nil (wire default)")
}

func TestWriteDrainWiringSuite(t *testing.T) {
	suite.Run(t, new(WriteDrainWiringSuite))
}
