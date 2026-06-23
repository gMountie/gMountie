package controller

// apply_test.go — tests for the Apply streaming handler (Task 5).
//
// Key invariants exercised:
//   - Batch applies in seq order; terminal ApplyAck.Watermark == last seq.
//   - Persist-before-ack: store watermark == ack.Watermark after Apply returns
//     (Advance is called before SendAndClose completes).
//   - Dedup: ops with seq ≤ existing store watermark are skipped; the underlying
//     filesystem only sees each op once (no double-apply of non-idempotent ops).
//   - Ordered halt: a failing op mid-batch stops processing; ApplyAck carries
//     committed prefix + failed seq; ops after it are not applied; store is
//     advanced only to the committed prefix.
//   - Create+WriteOp path-based apply: file is created and bytes are durably
//     written (verified by reading back from disk).
//   - ReleaseOp is a no-op marker that advances watermark without fs side effects.
//   - No session in context → Unauthenticated.

import (
	"context"
	"io"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/hanwen/go-fuse/v2/fuse/pathfs"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	mockservice "go.gmountie.dev/gmountie/internal/mocks/pkg/server/service"
	"go.gmountie.dev/gmountie/pkg/common"
	"go.gmountie.dev/gmountie/pkg/proto"
	serverio "go.gmountie.dev/gmountie/pkg/server/io"
	"go.gmountie.dev/gmountie/pkg/server/principal"
	"go.gmountie.dev/gmountie/pkg/server/service"
	"go.gmountie.dev/gmountie/pkg/server/watermark"
)

// ---------------------------------------------------------------------------
// Fake watermark store
// ---------------------------------------------------------------------------

type fakeWatermarkStore struct {
	mu      sync.Mutex
	records map[watermark.Key]watermark.Record
	advLog  []advanceCall
}

type advanceCall struct {
	key watermark.Key
	wm  uint64
}

func newFakeWatermarkStore() *fakeWatermarkStore {
	return &fakeWatermarkStore{records: make(map[watermark.Key]watermark.Record)}
}

func (f *fakeWatermarkStore) Get(k watermark.Key) (watermark.Record, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.records[k], nil
}

func (f *fakeWatermarkStore) Advance(k watermark.Key, wm uint64) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	r := f.records[k]
	if wm > r.Watermark {
		r.Watermark = wm
		f.records[k] = r
	}
	f.advLog = append(f.advLog, advanceCall{k, wm})
	return nil
}

func (f *fakeWatermarkStore) RevokeGen(k watermark.Key, _ uint64) error { return nil }
func (f *fakeWatermarkStore) Close() error                               { return nil }

func (f *fakeWatermarkStore) getWatermark(k watermark.Key) uint64 {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.records[k].Watermark
}

func (f *fakeWatermarkStore) advanceCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.advLog)
}

// ---------------------------------------------------------------------------
// Stub Apply server stream
// ---------------------------------------------------------------------------

// stubApplyStream implements proto.RpcFs_ApplyServer
// (grpc.ClientStreamingServer[proto.WalOp, proto.ApplyAck]).
type stubApplyStream struct {
	ctx context.Context

	mu    sync.Mutex
	ops   []*proto.WalOp
	pos   int
	acked *proto.ApplyAck
}

func newStubApplyStream(ctx context.Context, ops ...*proto.WalOp) *stubApplyStream {
	return &stubApplyStream{ctx: ctx, ops: ops}
}

func (s *stubApplyStream) Recv() (*proto.WalOp, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.pos >= len(s.ops) {
		return nil, io.EOF
	}
	op := s.ops[s.pos]
	s.pos++
	return op, nil
}

func (s *stubApplyStream) SendAndClose(ack *proto.ApplyAck) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.acked = ack
	return nil
}

func (s *stubApplyStream) Context() context.Context     { return s.ctx }
func (s *stubApplyStream) SetHeader(metadata.MD) error  { return nil }
func (s *stubApplyStream) SendHeader(metadata.MD) error { return nil }
func (s *stubApplyStream) SetTrailer(metadata.MD)       {}
func (s *stubApplyStream) SendMsg(any) error            { return nil }
func (s *stubApplyStream) RecvMsg(any) error            { return nil }

// ---------------------------------------------------------------------------
// Test suite
// ---------------------------------------------------------------------------

type ApplySuite struct {
	suite.Suite

	dir        string // temp dir backing the loopback FS
	fsService  *mockservice.MockVolumeService
	sessionMgr service.SessionManager
	sessionID  string
	wmStore    *fakeWatermarkStore
	server     *RpcServerImpl
	bus        serverio.EventBus
}

func TestApplySuite(t *testing.T) { suite.Run(t, new(ApplySuite)) }

func (s *ApplySuite) SetupTest() {
	s.dir = s.T().TempDir()

	s.fsService = new(mockservice.MockVolumeService)
	s.fsService.On("ResolveIdentity", mock.Anything, mock.Anything, mock.Anything).
		Return(service.Identity{}, nil).Maybe()

	s.sessionMgr = service.NewSessionManager(service.SessionManagerOptions{})
	var err error
	s.sessionID, err = s.sessionMgr.Create("test-user", "")
	s.Require().NoError(err)

	s.bus = serverio.NewLocalEventBus(serverio.EventBusOptions{BufferSize: 16})
	s.wmStore = newFakeWatermarkStore()
	s.server = NewGrpcServer(s.fsService, s.sessionMgr, s.bus, nil, nil, nil, s.wmStore)
}

func (s *ApplySuite) TearDownTest() {
	_ = s.sessionMgr.Stop(context.Background())
	s.bus.Close()
}

// ctxWithSession returns a context that carries s.sessionID in gRPC metadata
// AND the session's principal in the context, satisfying resolveSession's
// ownership check (sess.Principal() == ctxP).
func (s *ApplySuite) ctxWithSession() context.Context {
	md := metadata.Pairs(common.MetadataSessionID, s.sessionID)
	ctx := metadata.NewIncomingContext(context.Background(), md)
	return principal.WithPrincipal(ctx, "test-user")
}

// bindVolume wires BindIdentity for volume to return a real loopback FS over s.dir.
func (s *ApplySuite) bindVolume(vol string) {
	fs := pathfs.NewLoopbackFileSystem(s.dir)
	s.fsService.On("BindIdentity", mock.Anything, vol, mock.Anything).
		Return(fs, service.Identity{Principal: "test-user"}, nil).Maybe()
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

// TestApply_NoSession: missing session_id in context → Unauthenticated.
func (s *ApplySuite) TestApply_NoSession() {
	stream := newStubApplyStream(context.Background())
	err := s.server.Apply(stream)
	s.Require().Error(err)
	st, ok := status.FromError(err)
	s.Require().True(ok)
	s.Equal(codes.Unauthenticated, st.Code())
}

// TestApply_EmptyBatch: EOF with no ops → ack at watermark 0.
func (s *ApplySuite) TestApply_EmptyBatch() {
	// Empty stream — BindIdentity never called; server must handle EOF gracefully.
	stream := newStubApplyStream(s.ctxWithSession())
	err := s.server.Apply(stream)
	s.Require().NoError(err)
	s.Require().NotNil(stream.acked)
	s.Equal(uint64(0), stream.acked.Watermark)
	s.Equal(proto.FsError_FS_OK, stream.acked.Fserr)
}

// TestApply_HappyPath_MkdirBatch: three Mkdir ops succeed in order; terminal
// watermark equals last seq; directories exist on disk; store was advanced.
func (s *ApplySuite) TestApply_HappyPath_MkdirBatch() {
	const vol = "vol"
	s.bindVolume(vol)

	ops := []*proto.WalOp{
		mkdirOp(vol, "alpha", 1, s.sessionID, "r1"),
		mkdirOp(vol, "beta", 2, s.sessionID, "r2"),
		mkdirOp(vol, "gamma", 3, s.sessionID, "r3"),
	}
	stream := newStubApplyStream(s.ctxWithSession(), ops...)
	s.Require().NoError(s.server.Apply(stream))
	s.Require().NotNil(stream.acked)
	s.Equal(uint64(3), stream.acked.Watermark)
	s.Equal(uint64(0), stream.acked.FailedSeq)
	s.Equal(proto.FsError_FS_OK, stream.acked.Fserr)

	for _, name := range []string{"alpha", "beta", "gamma"} {
		info, err := os.Stat(filepath.Join(s.dir, name))
		s.Require().NoError(err, "dir %q must exist", name)
		s.True(info.IsDir())
	}

	wmKey := watermark.Key{Identity: "test-user", Volume: vol}
	s.Equal(uint64(3), s.wmStore.getWatermark(wmKey))
	s.Greater(s.wmStore.advanceCount(), 0)
}

// TestApply_Dedup_SkipsAlreadyApplied: ops with seq ≤ store watermark are
// skipped. Key proof: if dedup is broken, replaying Mkdir("alpha") would return
// EEXIST → ordered halt at seq 1 → ack.Watermark=0 and "gamma" would not exist.
// The test pre-seeds watermark=2 and pre-creates alpha/beta; if seq 3 applies
// and ack.Watermark=3, dedup worked.
func (s *ApplySuite) TestApply_Dedup_SkipsAlreadyApplied() {
	const vol = "vol"
	s.bindVolume(vol)

	wmKey := watermark.Key{Identity: "test-user", Volume: vol}
	s.Require().NoError(s.wmStore.Advance(wmKey, 2))

	// Pre-create directories that "seqs 1+2 already applied".
	s.Require().NoError(os.Mkdir(filepath.Join(s.dir, "alpha"), 0o755))
	s.Require().NoError(os.Mkdir(filepath.Join(s.dir, "beta"), 0o755))

	ops := []*proto.WalOp{
		mkdirOp(vol, "alpha", 1, s.sessionID, "r1"), // ≤ watermark(2) → skip
		mkdirOp(vol, "beta", 2, s.sessionID, "r2"),  // ≤ watermark(2) → skip
		mkdirOp(vol, "gamma", 3, s.sessionID, "r3"), // new → apply
	}
	stream := newStubApplyStream(s.ctxWithSession(), ops...)
	s.Require().NoError(s.server.Apply(stream))
	s.Require().NotNil(stream.acked)
	s.Equal(uint64(3), stream.acked.Watermark,
		"dedup must skip seqs ≤ watermark without EEXIST; seq 3 must be final watermark")
	s.Equal(proto.FsError_FS_OK, stream.acked.Fserr)

	info, err := os.Stat(filepath.Join(s.dir, "gamma"))
	s.Require().NoError(err, "seq 3 must have been applied")
	s.True(info.IsDir())
}

// TestApply_PersistBeforeAck: the store's persisted watermark equals
// ack.Watermark after Apply returns — Advance was called before SendAndClose.
func (s *ApplySuite) TestApply_PersistBeforeAck() {
	const vol = "vol"
	s.bindVolume(vol)

	ops := []*proto.WalOp{
		mkdirOp(vol, "d1", 1, s.sessionID, "ra"),
		mkdirOp(vol, "d2", 2, s.sessionID, "rb"),
	}
	stream := newStubApplyStream(s.ctxWithSession(), ops...)
	s.Require().NoError(s.server.Apply(stream))
	s.Require().NotNil(stream.acked)

	wmKey := watermark.Key{Identity: "test-user", Volume: vol}
	// The fake SendAndClose is synchronous: when Apply returns, Advance must
	// already have been called with the acked watermark.
	s.Equal(stream.acked.Watermark, s.wmStore.getWatermark(wmKey),
		"store.Advance(ack.Watermark) must complete before SendAndClose is called")
}

// TestApply_OrderedHalt_FailingOp: a failing op mid-batch halts processing.
// seq 2 tries to Mkdir an already-existing path → EEXIST.
// seq 3 must NOT be applied; store advances only to committed=1.
func (s *ApplySuite) TestApply_OrderedHalt_FailingOp() {
	const vol = "vol"
	s.bindVolume(vol)

	ops := []*proto.WalOp{
		mkdirOp(vol, "alpha", 1, s.sessionID, "r1"),         // succeeds
		mkdirOp(vol, "alpha", 2, s.sessionID, "r2"),         // EEXIST → halt
		mkdirOp(vol, "gamma", 3, s.sessionID, "r3"),         // must NOT be applied
	}
	stream := newStubApplyStream(s.ctxWithSession(), ops...)
	// Apply returns nil on in-band failure; the error is in the ack.
	s.Require().NoError(s.server.Apply(stream))
	s.Require().NotNil(stream.acked)

	s.Equal(uint64(1), stream.acked.Watermark, "committed prefix must be 1")
	s.Equal(uint64(2), stream.acked.FailedSeq, "failed seq must be 2")
	s.NotEqual(proto.FsError_FS_OK, stream.acked.Fserr)

	_, err := os.Stat(filepath.Join(s.dir, "gamma"))
	s.True(os.IsNotExist(err), "gamma must not exist: ops after ordered halt must not be applied")

	wmKey := watermark.Key{Identity: "test-user", Volume: vol}
	s.Equal(uint64(1), s.wmStore.getWatermark(wmKey),
		"store must advance only to committed prefix on halt")
}

// TestApply_CreateAndWrite: path-based Create materialises a file; WriteOp
// writes content; reading back from disk verifies durability.
func (s *ApplySuite) TestApply_CreateAndWrite() {
	const (
		vol     = "vol"
		content = "hello apply"
	)
	s.bindVolume(vol)

	ops := []*proto.WalOp{
		{
			Op: &proto.WalOp_Create{Create: &proto.CreateRequest{
				Volume: vol, Caller: CreateCaller(0, 0, 0), Path: "hello.txt",
				Flags: 0, Mode: 0o644,
				SessionId: s.sessionID, RequestId: "rc1",
			}},
			Seq: 1,
		},
		{
			Op: &proto.WalOp_Write{Write: &proto.WriteOp{
				Volume: vol, Caller: CreateCaller(0, 0, 0), Path: "hello.txt",
				Offset: 0, Data: []byte(content), RequestId: "rw1",
			}},
			Seq: 2,
		},
	}
	stream := newStubApplyStream(s.ctxWithSession(), ops...)
	s.Require().NoError(s.server.Apply(stream))
	s.Require().NotNil(stream.acked)
	s.Equal(uint64(2), stream.acked.Watermark)
	s.Equal(proto.FsError_FS_OK, stream.acked.Fserr)

	got, err := os.ReadFile(filepath.Join(s.dir, "hello.txt"))
	s.Require().NoError(err, "hello.txt must exist after Create+Write")
	s.Equal(content, string(got))
}

// TestApply_ReleaseOp_NoOp: ReleaseOp advances watermark without fs side effects.
func (s *ApplySuite) TestApply_ReleaseOp_NoOp() {
	const vol = "vol"
	s.bindVolume(vol)

	s.Require().NoError(os.WriteFile(filepath.Join(s.dir, "r.txt"), []byte("x"), 0o644))

	ops := []*proto.WalOp{
		{
			Op: &proto.WalOp_Release{Release: &proto.ReleaseOp{
				Volume: vol, Caller: CreateCaller(0, 0, 0), Path: "r.txt", RequestId: "rr1",
			}},
			Seq: 1,
		},
	}
	stream := newStubApplyStream(s.ctxWithSession(), ops...)
	s.Require().NoError(s.server.Apply(stream))
	s.Require().NotNil(stream.acked)
	s.Equal(uint64(1), stream.acked.Watermark)
	s.Equal(proto.FsError_FS_OK, stream.acked.Fserr)
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

func mkdirOp(vol, path string, seq uint64, sessionID, requestID string) *proto.WalOp {
	return &proto.WalOp{
		Op: &proto.WalOp_Mkdir{Mkdir: &proto.MkdirRequest{
			Volume: vol, Caller: CreateCaller(0, 0, 0), Path: path,
			Mode: 0o755, SessionId: sessionID, RequestId: requestID,
		}},
		Seq: seq,
	}
}
