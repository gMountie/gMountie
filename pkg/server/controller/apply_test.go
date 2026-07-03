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
	"errors"
	"io"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/hanwen/go-fuse/v2/fuse/pathfs"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	mockservice "go.gmountie.dev/gmountie/internal/mocks/pkg/server/service"
	"go.gmountie.dev/gmountie/pkg/common"
	"go.gmountie.dev/gmountie/pkg/proto"
	"go.gmountie.dev/gmountie/pkg/server/delegation"
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

func (f *fakeWatermarkStore) RevokeGen(k watermark.Key, gen uint64) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	r := f.records[k]
	r.RevokedGens = append(r.RevokedGens, gen)
	f.records[k] = r
	return nil
}

func (f *fakeWatermarkStore) NextGen(k watermark.Key) (uint64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	r := f.records[k]
	r.GenHi++
	gen := r.GenHi
	f.records[k] = r
	return gen, nil
}

func (f *fakeWatermarkStore) Close() error { return nil }

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

	// Delegation-arbitration fixtures, mirroring DelegationHandlerSuite
	// (delegation_handler_test.go): a real *delegation.Arbiter backed by a
	// fakeRecallRecorder (defined there), sharing s.wmStore as its fence
	// store — same as production, where Apply's watermark store and the
	// arbiter's are the same instance.
	recaller   *fakeRecallRecorder
	arbiter    *delegation.Arbiter
	principals map[string]string // sessionID -> principal, for grantTo/ctxForSession
	vol        string            // default volume for arbitration tests
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

	s.vol = "vol"
	s.principals = map[string]string{s.sessionID: "test-user"}

	s.bus = serverio.NewLocalEventBus(serverio.EventBusOptions{BufferSize: 16})
	s.wmStore = newFakeWatermarkStore()
	s.recaller = &fakeRecallRecorder{}
	s.arbiter = delegation.NewArbiter(s.recaller, delegation.Config{
		Cooldown: delegation.CooldownConfigDefault(),
	}, time.Now, s.wmStore)
	s.server = NewGrpcServer(s.fsService, s.sessionMgr, s.bus, nil, s.arbiter, nil, s.wmStore)
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

// foreignSession creates an additional session for principal and registers it
// in s.principals so grantTo/ctxForSession can resolve it without the caller
// hardcoding the identity at each call site (mirrors DelegationHandlerSuite's
// sidA/sidB + principals map).
func (s *ApplySuite) foreignSession(principal string) string {
	sid, err := s.sessionMgr.Create(principal, "")
	s.Require().NoError(err)
	s.principals[sid] = principal
	return sid
}

// grantTo grants owner sessionID a delegation rooted at root on s.vol,
// resolving the session's principal from s.principals. Mirrors
// DelegationHandlerSuite.grantTo.
func (s *ApplySuite) grantTo(sessionID, root string) {
	s.arbiter.Request(sessionID, root, s.principals[sessionID], s.vol, "")
}

// ctxForSession returns a context carrying sessionID in gRPC incoming
// metadata AND the session's principal, satisfying resolveSession's ownership
// check for an arbitrary (non-default) session. Mirrors
// DelegationHandlerSuite.ctxForSession.
func (s *ApplySuite) ctxForSession(sessionID string) context.Context {
	return ctxWithSession(testAuthedCtx(s.principals[sessionID]), sessionID)
}

// walCreate builds a path-based Create WalOp on s.vol for delegation-
// arbitration tests: seq is the stream sequence number, gen the delegation
// generation tag (0 = untagged, never fenced). Applied by s.sessionID.
func (s *ApplySuite) walCreate(path string, seq, gen uint64) *proto.WalOp {
	return &proto.WalOp{
		Op: &proto.WalOp_Create{Create: &proto.CreateRequest{
			Volume: s.vol, Caller: CreateCaller(0, 0, 0), Path: path,
			Flags: 0, Mode: 0o644,
			SessionId: s.sessionID, RequestId: "r-wal-create",
		}},
		Seq: seq,
		Gen: gen,
	}
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
	s.Positive(s.wmStore.advanceCount())
}

// TestApply_DuplicateMkdir_SkipsEEXIST: a duplicate mkdir of an ALREADY-EXISTING
// directory (e.g. concurrent npm extraction racing a shared parent) must be
// SKIPPED as an idempotent no-op, not ordered-halted. A halt would discard the
// duplicate AND every later op in the batch — the false data loss + ENOENT
// cascade that broke `npm install`.
func (s *ApplySuite) TestApply_DuplicateMkdir_SkipsEEXIST() {
	const vol = "vol"
	s.bindVolume(vol)
	ops := []*proto.WalOp{
		mkdirOp(vol, "pkg", 1, s.sessionID, "r1"),     // creates pkg
		mkdirOp(vol, "pkg", 2, s.sessionID, "r2"),     // DUPLICATE -> EEXIST -> must be skipped
		mkdirOp(vol, "pkg/sub", 3, s.sessionID, "r3"), // child AFTER the dup -> must still apply
	}
	stream := newStubApplyStream(s.ctxWithSession(), ops...)
	s.Require().NoError(s.server.Apply(stream))
	s.Require().NotNil(stream.acked)
	s.Equal(uint64(3), stream.acked.Watermark, "duplicate mkdir skipped; batch continues to seq 3")
	s.Equal(uint64(0), stream.acked.FailedSeq, "no ordered halt on a benign duplicate mkdir")
	s.Equal(proto.FsError_FS_OK, stream.acked.Fserr)
	// The child created AFTER the duplicate must exist — proves the batch did not halt.
	info, err := os.Stat(filepath.Join(s.dir, "pkg", "sub"))
	s.Require().NoError(err)
	s.True(info.IsDir())
}

// TestApply_MkdirOverFile_StillHalts: a mkdir EEXIST where a FILE (not a dir)
// occupies the path is a REAL type conflict and must still ordered-halt — the
// idempotent-skip must never mask it.
func (s *ApplySuite) TestApply_MkdirOverFile_StillHalts() {
	const vol = "vol"
	s.bindVolume(vol)
	s.Require().NoError(os.WriteFile(filepath.Join(s.dir, "conflict"), []byte("x"), 0o644))
	ops := []*proto.WalOp{
		mkdirOp(vol, "conflict", 1, s.sessionID, "r1"), // mkdir over a file -> EEXIST, real conflict
		mkdirOp(vol, "after", 2, s.sessionID, "r2"),
	}
	stream := newStubApplyStream(s.ctxWithSession(), ops...)
	s.Require().NoError(s.server.Apply(stream))
	s.Require().NotNil(stream.acked)
	s.Equal(uint64(0), stream.acked.Watermark, "type conflict must ordered-halt at seq 1")
	s.Equal(uint64(1), stream.acked.FailedSeq)
	s.Equal(proto.FsError_FS_EEXIST, stream.acked.Fserr)
	_, err := os.Stat(filepath.Join(s.dir, "after"))
	s.Require().Error(err, "ops after a genuine halt must not apply")
}

// TestApply_UnlinkMissing_SkipsENOENT: unlink of an already-absent path is the
// desired end-state of a deletion — skip it, don't halt the batch.
func (s *ApplySuite) TestApply_UnlinkMissing_SkipsENOENT() {
	const vol = "vol"
	s.bindVolume(vol)
	ops := []*proto.WalOp{
		mkdirOp(vol, "keep", 1, s.sessionID, "r1"),
		unlinkOp(vol, "ghost", 2, s.sessionID, "r2"), // never existed -> ENOENT -> skip
		mkdirOp(vol, "after", 3, s.sessionID, "r3"),
	}
	stream := newStubApplyStream(s.ctxWithSession(), ops...)
	s.Require().NoError(s.server.Apply(stream))
	s.Require().NotNil(stream.acked)
	s.Equal(uint64(3), stream.acked.Watermark, "missing-target unlink skipped; batch continues")
	s.Equal(uint64(0), stream.acked.FailedSeq)
	info, err := os.Stat(filepath.Join(s.dir, "after"))
	s.Require().NoError(err)
	s.True(info.IsDir())
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

// TestApply_Epoch_FreshEpochNotDedupSkipped is the bug-#2 regression: a fresh
// client wal.db (new wal-epoch) sends low seqs while a PRIOR epoch's watermark
// is high. Without per-epoch keying these ops (seq ≤ the old watermark) would
// all be dedup-skipped and silently discarded. With the epoch in the key the
// fresh epoch has its own seq namespace and every op applies.
func (s *ApplySuite) TestApply_Epoch_FreshEpochNotDedupSkipped() {
	const vol = "vol"
	s.bindVolume(vol)

	// A prior epoch accumulated a high watermark (dead client / wiped wal.db).
	s.Require().NoError(s.wmStore.Advance(
		watermark.Key{Identity: "test-user", Volume: vol, Epoch: "old"}, 1000))

	// Fresh client (new epoch) replays low seqs.
	ops := []*proto.WalOp{
		mkdirOp(vol, "fresh1", 1, s.sessionID, "r1"),
		mkdirOp(vol, "fresh2", 2, s.sessionID, "r2"),
		mkdirOp(vol, "fresh3", 3, s.sessionID, "r3"),
	}
	for _, op := range ops {
		op.WalEpoch = "new"
	}
	stream := newStubApplyStream(s.ctxWithSession(), ops...)
	s.Require().NoError(s.server.Apply(stream))
	s.Require().NotNil(stream.acked)
	s.Equal(proto.FsError_FS_OK, stream.acked.Fserr)
	s.Equal(uint64(3), stream.acked.Watermark)

	// All three applied — NOT dedup-skipped against the "old" epoch's watermark.
	for _, name := range []string{"fresh1", "fresh2", "fresh3"} {
		info, err := os.Stat(filepath.Join(s.dir, name))
		s.Require().NoError(err, name+" must apply under the fresh epoch (not dedup-skipped)")
		s.True(info.IsDir())
	}

	// Each epoch keeps an independent watermark.
	s.Equal(uint64(3), s.wmStore.getWatermark(
		watermark.Key{Identity: "test-user", Volume: vol, Epoch: "new"}))
	s.Equal(uint64(1000), s.wmStore.getWatermark(
		watermark.Key{Identity: "test-user", Volume: vol, Epoch: "old"}))
}

// TestApply_Dedup_ReplayIdempotent is the canonical double-apply test:
// the same batch is submitted twice. The first Apply creates three dirs and
// advances the store watermark to 3. The second Apply receives the same ops
// (seqs 1-3); dedup must skip them all (seq ≤ stored watermark 3). The dirs
// must exist exactly once (no EEXIST), ack.Watermark must still equal 3, and
// ack.Fserr must be FS_OK.
//
// This tests the round-trip property: a watermark written by Apply is
// read back as the dedup threshold on a subsequent Apply — preventing
// double-application of non-idempotent ops across process restarts.
func (s *ApplySuite) TestApply_Dedup_ReplayIdempotent() {
	const vol = "vol"
	s.bindVolume(vol)

	ops := []*proto.WalOp{
		mkdirOp(vol, "rep1", 1, s.sessionID, "r1"),
		mkdirOp(vol, "rep2", 2, s.sessionID, "r2"),
		mkdirOp(vol, "rep3", 3, s.sessionID, "r3"),
	}

	// First Apply — all three ops are new.
	stream1 := newStubApplyStream(s.ctxWithSession(), ops...)
	s.Require().NoError(s.server.Apply(stream1))
	s.Require().NotNil(stream1.acked)
	s.Equal(uint64(3), stream1.acked.Watermark, "first Apply: watermark must be 3")
	s.Equal(proto.FsError_FS_OK, stream1.acked.Fserr)

	// Second Apply — same ops. All seqs ≤ stored watermark(3) → dedup skips.
	stream2 := newStubApplyStream(s.ctxWithSession(), ops...)
	s.Require().NoError(s.server.Apply(stream2))
	s.Require().NotNil(stream2.acked)
	s.Equal(uint64(3), stream2.acked.Watermark,
		"second Apply: dedup must skip all ops; watermark stays at 3")
	s.Equal(proto.FsError_FS_OK, stream2.acked.Fserr,
		"second Apply must not surface EEXIST: dedup prevents re-creation")

	// Dirs must exist exactly once on disk.
	for _, name := range []string{"rep1", "rep2", "rep3"} {
		info, err := os.Stat(filepath.Join(s.dir, name))
		s.Require().NoError(err, "dir %q must exist after double-apply", name)
		s.True(info.IsDir())
	}
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

	// A GENUINE failure (mkdir into a missing parent → ENOENT) must ordered-halt.
	// (A duplicate mkdir of an existing dir is a BENIGN no-op that is skipped, not
	// halted — see TestApply_DuplicateMkdir_SkipsEEXIST.)
	ops := []*proto.WalOp{
		mkdirOp(vol, "alpha", 1, s.sessionID, "r1"),                // succeeds
		mkdirOp(vol, "no-such-parent/child", 2, s.sessionID, "r2"), // ENOENT → genuine halt
		mkdirOp(vol, "gamma", 3, s.sessionID, "r3"),                // must NOT be applied
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

// TestApply_GenFence_RevokedGenHaltsReplay verifies that a WalOp tagged with a
// revoked delegation gen is rejected (fenced), even though its seq is above
// the watermark (it would otherwise be applied). The ack carries FS_ESTALE and
// the committed prefix; the fenced op itself is NOT applied (no dir on disk).
//
// This is the corruption-critical test for Task 6: without gen-fencing, a
// dead holder's stale WAL replay would clobber the new owner's writes.
func (s *ApplySuite) TestApply_GenFence_RevokedGenHaltsReplay() {
	const vol = "vol"
	s.bindVolume(vol)

	// Pre-seed the watermark store with a revoked gen for the test principal+vol.
	wmKey := watermark.Key{Identity: "test-user", Volume: vol}
	// Pre-apply seq=1 to establish a committed prefix.
	s.Require().NoError(s.wmStore.Advance(wmKey, 1))
	// Mark gen=5 as revoked (simulates a handoff after machine death).
	s.Require().NoError(s.wmStore.RevokeGen(wmKey, 5))

	// Seed the matching directory for seq=1 (already committed).
	s.Require().NoError(os.Mkdir(filepath.Join(s.dir, "committed"), 0o755))

	ops := []*proto.WalOp{
		// seq=2 has revoked gen=5 — must be fenced (not applied).
		{
			Op: &proto.WalOp_Mkdir{Mkdir: &proto.MkdirRequest{
				Volume: vol, Caller: CreateCaller(0, 0, 0), Path: "fenced-dir",
				Mode: 0o755, SessionId: s.sessionID, RequestId: "r-fence",
			}},
			Seq: 2,
			Gen: 5, // revoked gen — must halt
		},
		// seq=3 would apply "safe-dir" if the fence didn't halt us first.
		mkdirOp(vol, "safe-dir", 3, s.sessionID, "r-after"),
	}
	stream := newStubApplyStream(s.ctxWithSession(), ops...)
	s.Require().NoError(s.server.Apply(stream))
	s.Require().NotNil(stream.acked)

	// Ack must carry the committed prefix (seq=1) and signal the fence point.
	s.Equal(uint64(1), stream.acked.Watermark, "watermark must be committed prefix, not fenced seq")
	s.Equal(uint64(2), stream.acked.FailedSeq, "FailedSeq must be the fenced op's seq")
	s.Equal(proto.FsError_FS_ESTALE, stream.acked.Fserr,
		"fenced op must return FS_ESTALE, not FS_OK or FS_EEXIST")

	// The fenced dir must NOT have been created.
	_, err := os.Stat(filepath.Join(s.dir, "fenced-dir"))
	s.True(os.IsNotExist(err), "fenced-dir must not exist: fenced op must not be applied")

	// Ops after the fenced op must also not have been applied.
	_, err = os.Stat(filepath.Join(s.dir, "safe-dir"))
	s.True(os.IsNotExist(err), "safe-dir must not exist: ops after the fence are discarded")
}

// TestApply_GenFence_NonRevokedGenAppliesNormally verifies that an op tagged
// with a valid (non-revoked) gen is applied normally — the fence is selective.
func (s *ApplySuite) TestApply_GenFence_NonRevokedGenAppliesNormally() {
	const vol = "vol"
	s.bindVolume(vol)

	// Revoke gen=5, but use gen=7 (valid) in the op.
	wmKey := watermark.Key{Identity: "test-user", Volume: vol}
	s.Require().NoError(s.wmStore.RevokeGen(wmKey, 5))

	ops := []*proto.WalOp{
		{
			Op: &proto.WalOp_Mkdir{Mkdir: &proto.MkdirRequest{
				Volume: vol, Caller: CreateCaller(0, 0, 0), Path: "live-dir",
				Mode: 0o755, SessionId: s.sessionID, RequestId: "r-live",
			}},
			Seq: 1,
			Gen: 7, // valid gen, not revoked
		},
	}
	stream := newStubApplyStream(s.ctxWithSession(), ops...)
	s.Require().NoError(s.server.Apply(stream))
	s.Require().NotNil(stream.acked)

	s.Equal(uint64(1), stream.acked.Watermark)
	s.Equal(proto.FsError_FS_OK, stream.acked.Fserr)

	info, err := os.Stat(filepath.Join(s.dir, "live-dir"))
	s.Require().NoError(err, "live-dir must exist: non-revoked gen must apply normally")
	s.True(info.IsDir())
}

// TestApply_GenFence_Gen0NeverFenced verifies that an op with gen=0 (pre-fencing
// client, untagged) is never fenced even when the store has revoked gens.
func (s *ApplySuite) TestApply_GenFence_Gen0NeverFenced() {
	const vol = "vol"
	s.bindVolume(vol)

	// Revoke gen=1 (would match a tagged op), but the op carries gen=0.
	wmKey := watermark.Key{Identity: "test-user", Volume: vol}
	s.Require().NoError(s.wmStore.RevokeGen(wmKey, 1))

	ops := []*proto.WalOp{
		{
			Op: &proto.WalOp_Mkdir{Mkdir: &proto.MkdirRequest{
				Volume: vol, Caller: CreateCaller(0, 0, 0), Path: "untagged-dir",
				Mode: 0o755, SessionId: s.sessionID, RequestId: "r-untagged",
			}},
			Seq: 1,
			Gen: 0, // untagged: gen 0 is never fenced
		},
	}
	stream := newStubApplyStream(s.ctxWithSession(), ops...)
	s.Require().NoError(s.server.Apply(stream))
	s.Require().NotNil(stream.acked)

	s.Equal(uint64(1), stream.acked.Watermark)
	s.Equal(proto.FsError_FS_OK, stream.acked.Fserr, "gen=0 must never be fenced")

	info, err := os.Stat(filepath.Join(s.dir, "untagged-dir"))
	s.Require().NoError(err)
	s.True(info.IsDir())
}

// TestApply_GenFence_AlreadyAckedRevokedGenSkippedByDedup verifies that an op
// whose seq is ≤ the durable watermark is skipped by dedup BEFORE the gen
// fence is checked — even if its gen is revoked. This prevents false-fencing of
// already-committed ops during normal recall scenarios.
func (s *ApplySuite) TestApply_GenFence_AlreadyAckedRevokedGenSkippedByDedup() {
	const vol = "vol"
	s.bindVolume(vol)

	wmKey := watermark.Key{Identity: "test-user", Volume: vol}
	// Pre-seed watermark to 2 and revoke gen=3 (the gen of the already-applied ops).
	s.Require().NoError(s.wmStore.Advance(wmKey, 2))
	s.Require().NoError(s.wmStore.RevokeGen(wmKey, 3))

	// Pre-create dirs matching already-applied ops.
	s.Require().NoError(os.Mkdir(filepath.Join(s.dir, "old1"), 0o755))
	s.Require().NoError(os.Mkdir(filepath.Join(s.dir, "old2"), 0o755))

	ops := []*proto.WalOp{
		// seqs 1+2 with revoked gen=3 — dedup must skip them before the fence check.
		{
			Op:  mkdirOp(vol, "old1", 1, s.sessionID, "r1").Op,
			Seq: 1, Gen: 3,
		},
		{
			Op:  mkdirOp(vol, "old2", 2, s.sessionID, "r2").Op,
			Seq: 2, Gen: 3,
		},
		// seq=3 with a live gen (gen=4) — must be applied.
		{
			Op: &proto.WalOp_Mkdir{Mkdir: &proto.MkdirRequest{
				Volume: vol, Caller: CreateCaller(0, 0, 0), Path: "new-dir",
				Mode: 0o755, SessionId: s.sessionID, RequestId: "r3",
			}},
			Seq: 3, Gen: 4,
		},
	}
	stream := newStubApplyStream(s.ctxWithSession(), ops...)
	s.Require().NoError(s.server.Apply(stream))
	s.Require().NotNil(stream.acked)

	s.Equal(uint64(3), stream.acked.Watermark,
		"dedup must skip ≤ watermark ops (even with revoked gen); seq=3 must apply")
	s.Equal(proto.FsError_FS_OK, stream.acked.Fserr,
		"dedup-before-fence: revoked gen on already-acked ops must not halt the batch")

	info, err := os.Stat(filepath.Join(s.dir, "new-dir"))
	s.Require().NoError(err, "seq=3 with live gen must be applied")
	s.True(info.IsDir())
}

// TestApply_NilWatermarkStore verifies that when r.watermark is nil, Apply
// returns an Internal error gracefully instead of panicking at a nil-deref.
func (s *ApplySuite) TestApply_NilWatermarkStore() {
	const vol = "vol"
	s.bindVolume(vol)

	// Create a server with nil watermark store (testing scenario).
	serverNoWM := NewGrpcServer(s.fsService, s.sessionMgr, s.bus, nil, nil, nil, nil)

	ops := []*proto.WalOp{
		mkdirOp(vol, "test", 1, s.sessionID, "r1"),
	}
	stream := newStubApplyStream(s.ctxWithSession(), ops...)
	err := serverNoWM.Apply(stream)

	s.Require().Error(err)
	st, ok := status.FromError(err)
	s.Require().True(ok)
	s.Equal(codes.Internal, st.Code())
	s.Contains(st.Message(), "watermark store not configured")
}

// TestApplyOpUnderForeignDelegationRecallsOrHaltsEAGAIN is the corruption-
// critical test for this task: an op whose path is covered by a FOREIGN
// delegation (held by another client) must arbitrate before dispatch. When
// the recall fails, the op must NOT be applied — Apply halts transiently
// with FS_EAGAIN, acking the committed prefix (0, nothing applied yet) and
// carrying the failed seq, so the client retries the tail from its WAL.
//
// Without arbitration, this op would apply straight through (a stale replay
// writing into a region since delegated to someone else, unrecalled).
func (s *ApplySuite) TestApplyOpUnderForeignDelegationRecallsOrHaltsEAGAIN() {
	s.bindVolume(s.vol)
	// Parent must exist so, absent arbitration, the create would SUCCEED
	// (RED must fail on FailedSeq/Fserr, not be confounded by an ENOENT).
	s.Require().NoError(os.Mkdir(filepath.Join(s.dir, "proj"), 0o755))

	holder := s.foreignSession("holder-user")
	s.grantTo(holder, "proj") // foreign delegation; recaller stubbed to FAIL
	s.recaller.err = errors.New("recall timed out")

	stream := newStubApplyStream(s.ctxForSession(s.sessionID),
		s.walCreate("proj/f.txt", 1 /*seq*/, 0 /*gen: untagged*/),
	)

	err := s.server.Apply(stream)
	s.Require().NoError(err)
	s.Require().NotNil(stream.acked)
	s.Equal(uint64(1), stream.acked.FailedSeq)
	s.Equal(proto.FsError_FS_EAGAIN, stream.acked.Fserr, "foreign-delegation contention is a TRANSIENT halt")
	s.Zero(stream.acked.Watermark)
	s.Equal([]string{holder + ":proj"}, s.recaller.Calls(), "the halt must come from arbitration firing a recall")

	_, statErr := os.Stat(filepath.Join(s.dir, "proj", "f.txt"))
	s.True(os.IsNotExist(statErr), "f.txt must not exist: the op must not be dispatched under contention")
}

// TestApplyOpSelfAccessDoesNotRecall verifies the normal flushing-client path:
// applying an op into a delegation the SAME session holds is self-access —
// OnMutation is a no-op, no recall is attempted, and the op applies normally.
func (s *ApplySuite) TestApplyOpSelfAccessDoesNotRecall() {
	s.bindVolume(s.vol)
	s.grantTo(s.sessionID, "proj") // holder grants itself a delegation on "proj"

	stream := newStubApplyStream(s.ctxForSession(s.sessionID),
		mkdirOp(s.vol, "proj", 1, s.sessionID, "r1"),
	)
	s.Require().NoError(s.server.Apply(stream))
	s.Require().NotNil(stream.acked)
	s.Equal(uint64(1), stream.acked.Watermark)
	s.Equal(uint64(0), stream.acked.FailedSeq)
	s.Equal(proto.FsError_FS_OK, stream.acked.Fserr)
	s.Empty(s.recaller.Calls(), "the holder's own flush into its own delegation must never recall")

	info, statErr := os.Stat(filepath.Join(s.dir, "proj"))
	s.Require().NoError(statErr)
	s.True(info.IsDir())
}

// TestApplyRenameArbitratesBothEndpoints verifies that a Rename op arbitrates
// BOTH old_name and new_name (walOpKindPath collapses a rename to "old ->
// new", which is why arbitrateApplyOp needs an explicit rename branch). A
// foreign delegation covering ONLY the rename's DESTINATION must still halt
// the op: rename's fence obligation isn't satisfied by checking the source
// alone.
func (s *ApplySuite) TestApplyRenameArbitratesBothEndpoints() {
	s.bindVolume(s.vol)
	s.Require().NoError(os.Mkdir(filepath.Join(s.dir, "src"), 0o755))
	s.Require().NoError(os.Mkdir(filepath.Join(s.dir, "dst"), 0o755))

	holder := s.foreignSession("holder-user")
	s.grantTo(holder, "dst") // foreign delegation covers ONLY the destination
	s.recaller.err = errors.New("recall timed out")

	ops := []*proto.WalOp{
		{
			Op: &proto.WalOp_Rename{Rename: &proto.RenameRequest{
				Volume: s.vol, Caller: CreateCaller(0, 0, 0),
				OldName: "src", NewName: "dst/moved",
				SessionId: s.sessionID, RequestId: "r-rename",
			}},
			Seq: 1,
		},
	}
	stream := newStubApplyStream(s.ctxForSession(s.sessionID), ops...)
	s.Require().NoError(s.server.Apply(stream))
	s.Require().NotNil(stream.acked)
	s.Equal(uint64(1), stream.acked.FailedSeq)
	s.Equal(proto.FsError_FS_EAGAIN, stream.acked.Fserr, "foreign delegation on the rename DESTINATION must still halt")
	s.Zero(stream.acked.Watermark)
	s.Equal([]string{holder + ":dst"}, s.recaller.Calls())

	// The rename must not have happened: src still there, dst/moved absent.
	_, statErr := os.Stat(filepath.Join(s.dir, "src"))
	s.Require().NoError(statErr, "src must still exist: rename must not apply under destination contention")
	_, statErr = os.Stat(filepath.Join(s.dir, "dst", "moved"))
	s.True(os.IsNotExist(statErr))
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

func unlinkOp(vol, path string, seq uint64, sessionID, requestID string) *proto.WalOp {
	return &proto.WalOp{
		Op: &proto.WalOp_Unlink{Unlink: &proto.UnlinkRequest{
			Volume: vol, Caller: CreateCaller(0, 0, 0), Path: path,
			SessionId: sessionID, RequestId: requestID,
		}},
		Seq: seq,
	}
}
