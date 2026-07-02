package wal_test

// layer_test.go — TDD tests for the WAL backend layer (Task 10b).
//
// Test setup:
//   - inner: real memfs.MemFS (in-memory reference backend)
//   - mgr:   real delegation.Manager with controlled grant set
//   - log:   real BboltLog (t.TempDir)
//   - overlay: real Overlay
//   - coord: real Coordinator wrapping log+overlay
//   - layer: NewLayer(inner, mgr, coord)
//
// Test coverage:
//   1. Delegated Create → layer.Stat returns it; memfs (inner) never saw it.
//   2. Delegated Mkdir → layer.Stat returns it; inner.Stat returns ENOENT.
//   3. Delegated Unlink → ListDir omits the entry (overlay tombstone via ListMerge).
//   4. BASE-DELTA TYPE PRESERVATION (highest-risk):
//      put a DIRECTORY in memfs; delegated SetAttr(chmod perm) → layer.Stat
//      returns a DIRECTORY with new perm bits. Assert S_IFMT is DIR, NOT file.
//   5. BASE-DELTA SIZE: write past base EOF → layer.Stat reports grown size.
//   6. Non-delegated op → passthrough to memfs; memfs sees the Mkdir.
//   7. Cross-subtree rename → synchronous (memfs sees the rename).
//   8. In-delegation rename → deferred (memfs does not see it, overlay reflects it).
//   9. Read of delegated pending path → overlay bytes merged over inner.
//  10. GetXAttr: pending set → overlay val; pending removal → ENO_XATTR; no pending → inner.

import (
	"context"
	"io"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/suite"
	"go.gmountie.dev/gmountie/pkg/client/backend"
	"go.gmountie.dev/gmountie/pkg/client/backend/delegation"
	"go.gmountie.dev/gmountie/pkg/client/backend/memfs"
	"go.gmountie.dev/gmountie/pkg/client/backend/wal"
	"go.gmountie.dev/gmountie/pkg/proto"
	"google.golang.org/grpc/metadata"
)

// ── test helpers ──────────────────────────────────────────────────────────────

// noopInv satisfies delegation.CacheInvalidator without doing anything.
type noopInv struct{}

func (noopInv) InvalidateSubtree(_ string) {}

// openLog opens a BboltLog in t.TempDir.
func openLayerLog(t *testing.T) *wal.BboltLog {
	t.Helper()
	l, err := wal.Open(filepath.Join(t.TempDir(), "layer_wal.db"))
	if err != nil {
		t.Fatalf("open wal log: %v", err)
	}
	t.Cleanup(func() { _ = l.Close() })
	return l
}

// fakeApplyStream is an external-package fake for proto.RpcFs_ApplyClient
// (a grpc.ClientStreamingClient[proto.WalOp, proto.ApplyAck]). It records
// every sent WalOp and, on CloseAndRecv, acks the highest seq it saw — i.e.
// it always fully commits whatever batch it is given. This lets tests drive
// a REAL wal.Coordinator.Flush / FlushForRecall to completion (the fixture
// guidance's alternative to the unexported commitFlushedForTest hook, which
// this external _test package cannot reach).
type fakeApplyStream struct {
	mu   sync.Mutex
	sent []*proto.WalOp
}

func (f *fakeApplyStream) Send(op *proto.WalOp) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.sent = append(f.sent, op)
	return nil
}

func (f *fakeApplyStream) CloseAndRecv() (*proto.ApplyAck, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	var wm uint64
	if len(f.sent) > 0 {
		wm = f.sent[len(f.sent)-1].Seq
	}
	return &proto.ApplyAck{Watermark: wm}, nil
}

func (f *fakeApplyStream) Header() (metadata.MD, error) { return nil, nil } //nolint:nilnil // fake gRPC stream: no metadata and no error is the correct test-double contract
func (f *fakeApplyStream) Trailer() metadata.MD         { return nil }
func (f *fakeApplyStream) CloseSend() error             { return nil }
func (f *fakeApplyStream) Context() context.Context     { return context.Background() }
func (f *fakeApplyStream) SendMsg(m any) error          { return nil }
func (f *fakeApplyStream) RecvMsg(m any) error          { return io.EOF }

var _ proto.RpcFs_ApplyClient = (*fakeApplyStream)(nil)

// ── LayerSuite ────────────────────────────────────────────────────────────────

type LayerSuite struct {
	suite.Suite
	fs    backend.FileSystemBackend // inner (base) backend — memfs.New()
	mgr   *delegation.Manager
	log   *wal.BboltLog
	ovl   *wal.Overlay
	coord *wal.Coordinator
	layer backend.FileSystemBackend
	ctx   context.Context

	// applyStreamsMu guards applyStreams — the set of fakeApplyStream instances
	// minted by the WithApplyFactory closure below, one per Flush/Fsync call.
	// Tests that need to inspect exactly which ops were sent to the (fake) wire
	// — e.g. asserting a rename's pre-flush never sent an OpRename — read this
	// after the call returns.
	applyStreamsMu sync.Mutex
	applyStreams   []*fakeApplyStream
}

func (s *LayerSuite) SetupTest() {
	s.fs = memfs.New()
	s.mgr = delegation.NewManager(noopInv{})
	s.log = openLayerLog(s.T())
	s.ovl = wal.NewOverlay()
	s.applyStreams = nil
	// WithApplyFactory wires a fake Apply stream that always fully commits
	// whatever it is sent, so tests can drive a REAL Flush/FlushForRecall to
	// completion (see fakeApplyStream doc comment). Harmless for tests that
	// never call Flush. Each minted stream is recorded in s.applyStreams so
	// tests can inspect exactly what was sent.
	s.coord = wal.NewCoordinator(s.mgr, s.log, s.ovl,
		wal.WithApplyFactory(func(_ context.Context) (proto.RpcFs_ApplyClient, error) {
			st := &fakeApplyStream{}
			s.applyStreamsMu.Lock()
			s.applyStreams = append(s.applyStreams, st)
			s.applyStreamsMu.Unlock()
			return st, nil
		}),
	)
	s.layer = wal.NewLayer(s.fs, s.mgr, s.coord)
	s.ctx = context.Background()
}

// sentOps flattens every WalOp sent across all fakeApplyStream instances
// minted so far (i.e. every op that has gone through a Flush/Fsync call).
func (s *LayerSuite) sentOps() []*proto.WalOp {
	s.applyStreamsMu.Lock()
	defer s.applyStreamsMu.Unlock()
	var out []*proto.WalOp
	for _, st := range s.applyStreams {
		st.mu.Lock()
		out = append(out, st.sent...)
		st.mu.Unlock()
	}
	return out
}

// grant makes root and all paths under it delegated.
func (s *LayerSuite) grant(root string) {
	s.mgr.Apply(&proto.DelegationGrant{GrantedRoot: root})
}

// flushAll drives a real, full Flush of every op currently in the WAL log
// (the fakeApplyStream always fully commits). Standing in for a completed
// interval flush.
func (s *LayerSuite) flushAll() {
	ops, err := s.log.Replay(0)
	s.Require().NoError(err)
	s.Require().NotEmpty(ops, "flushAll requires at least one pending op")
	s.Require().NoError(s.coord.Flush(s.ctx, ops[len(ops)-1].Seq))
}

// writeInner materialises path with data directly in the inner memfs
// backend, standing in for the real server-side Apply pipeline (out of scope
// for this unit test — the property under test is the Layer's read/write
// routing, not the Apply pipeline itself).
func (s *LayerSuite) writeInner(path string, data []byte) {
	parent, name := splitInnerPath(path)
	_, _, ferr := s.fs.Create(s.ctx, parent, name, 0, 0o100644)
	s.Require().Equal(proto.FsError_FS_OK, ferr)
	fh, ferr := s.fs.Open(s.ctx, path, 0)
	s.Require().Equal(proto.FsError_FS_OK, ferr)
	_, ferr = s.fs.Write(s.ctx, fh, 0, data)
	s.Require().Equal(proto.FsError_FS_OK, ferr)
	s.Require().Equal(proto.FsError_FS_OK, s.fs.Release(s.ctx, fh))
}

// splitInnerPath splits a memfs-convention path ("dir/f.txt") into parent
// ("dir") and name ("f.txt"); a root-level path ("f.txt") splits to ("", "f.txt").
func splitInnerPath(path string) (parent, name string) {
	idx := strings.LastIndex(path, "/")
	if idx < 0 {
		return "", path
	}
	return path[:idx], path[idx+1:]
}

// ── Test 1: delegated Create → overlay visible; memfs never sees it ───────────

func (s *LayerSuite) TestDelegatedCreate_VisibleViaOverlay_InnerUntouched() {
	s.grant("dir")

	// Pre-create the parent in memfs so Lookup on parent works.
	_, err := s.fs.Mkdir(s.ctx, "dir", 0o40755)
	s.Require().Equal(proto.FsError_FS_OK, err)

	fh, attr, ferr := s.layer.Create(s.ctx, "dir", "file.txt", 0, 0o100644)
	s.Require().Equal(proto.FsError_FS_OK, ferr)
	s.Require().NotNil(fh)
	s.Require().NotNil(attr)
	s.Equal("dir/file.txt", fh.Path())

	// layer.Stat must return the overlay attr.
	got, ferr := s.layer.Stat(s.ctx, "dir/file.txt")
	s.Require().Equal(proto.FsError_FS_OK, ferr)
	s.Require().NotNil(got)
	s.Equal(uint32(0o100644), got.Mode, "mode must match overlay create mode")

	// Inner (memfs) must not have seen the file.
	_, innerErr := s.fs.Stat(s.ctx, "dir/file.txt")
	s.Equal(proto.FsError_FS_ENOENT, innerErr, "memfs must not have the file")
}

// ── Test 2: delegated Mkdir → overlay visible; inner returns ENOENT ───────────

func (s *LayerSuite) TestDelegatedMkdir_VisibleViaOverlay_InnerUntouched() {
	s.grant("repo")

	attr, ferr := s.layer.Mkdir(s.ctx, "repo/src", 0o40755)
	s.Require().Equal(proto.FsError_FS_OK, ferr)
	s.Require().NotNil(attr)
	s.Equal(uint32(0o40755), attr.Mode)

	// layer.Stat serves from overlay.
	got, ferr := s.layer.Stat(s.ctx, "repo/src")
	s.Require().Equal(proto.FsError_FS_OK, ferr)
	s.Equal(uint32(0o40755), got.Mode)

	// inner has no knowledge.
	_, innerErr := s.fs.Stat(s.ctx, "repo/src")
	s.Equal(proto.FsError_FS_ENOENT, innerErr)
}

// ── Test 3: delegated Unlink → ListDir omits the unlinked entry ───────────────

func (s *LayerSuite) TestDelegatedUnlink_ListDirOmitsEntry() {
	// Create dir + file in memfs.
	_, err := s.fs.Mkdir(s.ctx, "proj", 0o40755)
	s.Require().Equal(proto.FsError_FS_OK, err)
	_, _, err = s.fs.Create(s.ctx, "proj", "main.go", 0, 0o100644)
	s.Require().Equal(proto.FsError_FS_OK, err)

	s.grant("proj")

	// Confirm it appears before unlink.
	entries, ferr := s.layer.ListDir(s.ctx, "proj")
	s.Require().Equal(proto.FsError_FS_OK, ferr)
	names := dirNames(entries)
	s.Contains(names, "main.go", "should appear before unlink")

	// Delegated Unlink.
	ferr = s.layer.Unlink(s.ctx, "proj/main.go")
	s.Require().Equal(proto.FsError_FS_OK, ferr)

	// Must not appear in listing after overlay tombstone.
	entries, ferr = s.layer.ListDir(s.ctx, "proj")
	s.Require().Equal(proto.FsError_FS_OK, ferr)
	names = dirNames(entries)
	s.NotContains(names, "main.go", "tombstoned entry must be absent from listing")

	// memfs still has the file (deferred).
	_, innerErr := s.fs.Stat(s.ctx, "proj/main.go")
	s.Equal(proto.FsError_FS_OK, innerErr, "memfs must still have the file (unlink deferred)")
}

// ── Test 4: BASE-DELTA TYPE PRESERVATION ──────────────────────────────────────
//
// Put a DIRECTORY in memfs; apply a delegated SetAttr (chmod) on it. The layer
// must return S_IFDIR with the new permission bits — NOT a plain file mode.

func (s *LayerSuite) TestBaseDelta_DirectoryTypeBitsPreserved_AfterChmod() {
	// Create a real directory in memfs with mode 0o40755.
	_, err := s.fs.Mkdir(s.ctx, "shared", 0o40755)
	s.Require().Equal(proto.FsError_FS_OK, err)

	s.grant("shared")

	// SetAttr: chmod only (FATTR_MODE, new perm 0o700).
	newPerm := uint32(0o700)
	in := backend.SetAttrIn{
		Valid: backend.FATTR_MODE,
		Mode:  newPerm, // permission bits only — no S_IFMT
	}
	got, ferr := s.layer.SetAttr(s.ctx, "shared", in)
	s.Require().Equal(proto.FsError_FS_OK, ferr)
	s.Require().NotNil(got)

	// TYPE BITS: must still be S_IFDIR.
	typeBits := got.Mode & uint32(syscall.S_IFMT)
	s.Equal(uint32(syscall.S_IFDIR), typeBits,
		"base-delta SetAttr must preserve the S_IFDIR type bits (got mode %#o)", got.Mode)

	// PERMISSION BITS: must reflect the new chmod.
	permBits := got.Mode & 0o7777
	s.Equal(newPerm, permBits,
		"base-delta SetAttr must apply the new permission bits (got mode %#o)", got.Mode)

	// Stat must return the same merged view.
	statGot, ferr := s.layer.Stat(s.ctx, "shared")
	s.Require().Equal(proto.FsError_FS_OK, ferr)
	s.Equal(got.Mode, statGot.Mode, "Stat must agree with SetAttr result")
}

// ── Test 5: BASE-DELTA SIZE — write past base EOF ─────────────────────────────

func (s *LayerSuite) TestBaseDelta_SizeGrowsAfterWritePastEOF() {
	// Create a 5-byte file in memfs.
	_, _, ferr := s.fs.Create(s.ctx, "", "data.bin", 0, 0o100644)
	s.Require().Equal(proto.FsError_FS_OK, ferr)
	fh, ferr := s.fs.Open(s.ctx, "data.bin", 0)
	s.Require().Equal(proto.FsError_FS_OK, ferr)
	_, ferr = s.fs.Write(s.ctx, fh, 0, []byte("hello"))
	s.Require().Equal(proto.FsError_FS_OK, ferr)
	_ = s.fs.Release(s.ctx, fh)

	// Confirm base size = 5.
	baseAttr, ferr := s.fs.Stat(s.ctx, "data.bin")
	s.Require().Equal(proto.FsError_FS_OK, ferr)
	s.Equal(uint64(5), baseAttr.Size)

	s.grant("data.bin") // grant the specific file path

	// Inject a write-past-EOF op into the overlay manually via RecordOp.
	// (The actual byte-write routing is Task 10a's drain; here we simulate
	// what the drain produces in the overlay.)
	recErr := s.coord.RecordOp(wal.Op{
		Kind:   wal.OpWrite,
		Path:   "data.bin",
		Offset: 5,
		Data:   []byte("world"),
	})
	s.Require().NoError(recErr)

	// layer.Stat must report size = 10 (max of base=5, pending=[5,10)).
	got, ferr := s.layer.Stat(s.ctx, "data.bin")
	s.Require().Equal(proto.FsError_FS_OK, ferr)
	s.Equal(uint64(10), got.Size,
		"size must be max(base=5, pending-end=10)=10 after write-past-EOF")
}

// ── Test 6: non-delegated op → passthrough; memfs sees it ────────────────────

func (s *LayerSuite) TestNonDelegated_PassthroughToInner() {
	// No grants → path is not delegated.
	attr, ferr := s.layer.Mkdir(s.ctx, "work", 0o40755)
	s.Require().Equal(proto.FsError_FS_OK, ferr)
	s.Require().NotNil(attr)

	// memfs must have the directory.
	got, ferr := s.fs.Stat(s.ctx, "work")
	s.Require().Equal(proto.FsError_FS_OK, ferr)
	s.Equal(uint32(syscall.S_IFDIR)|uint32(0o755), got.Mode)
}

// ── Test 7: cross-subtree rename → synchronous (memfs sees it) ───────────────

func (s *LayerSuite) TestCrossSubtreeRename_Synchronous() {
	// Create two top-level entries in memfs.
	_, err := s.fs.Mkdir(s.ctx, "a", 0o40755)
	s.Require().Equal(proto.FsError_FS_OK, err)
	_, _, err = s.fs.Create(s.ctx, "a", "f.txt", 0, 0o100644)
	s.Require().Equal(proto.FsError_FS_OK, err)
	_, err = s.fs.Mkdir(s.ctx, "b", 0o40755)
	s.Require().Equal(proto.FsError_FS_OK, err)

	// Grant only "a"; "b" is NOT delegated.
	s.grant("a")

	// Rename a/f.txt → b/f.txt crosses the delegated/non-delegated boundary.
	ferr := s.layer.Rename(s.ctx, "a/f.txt", "b/f.txt")
	s.Require().Equal(proto.FsError_FS_OK, ferr)

	// memfs must have received the rename synchronously.
	_, innerErr := s.fs.Stat(s.ctx, "b/f.txt")
	s.Equal(proto.FsError_FS_OK, innerErr, "memfs must see the cross-subtree rename")
	_, innerErr = s.fs.Stat(s.ctx, "a/f.txt")
	s.Equal(proto.FsError_FS_ENOENT, innerErr, "old path must be gone in memfs")
}

// ── Test 8: base-only rename (source pre-exists in inner) goes synchronous ───
//
// TestRenameOfBaseOnlyPathGoesSynchronous replaces the old
// TestInDelegationRename_Deferred, which pinned the BUG this task fixes: a
// deferred rename of a base-only (not overlay-created) file tombstoned the
// source in the overlay but synthesised nothing at the destination, so
// `mv delegated/old.txt delegated/new.txt && cat delegated/new.txt` returned
// ENOENT until the next flush. Under the new contract (overlayOwns gates
// deferral) a base-only source is not overlay-owned, so the rename executes
// synchronously against inner immediately — no ENOENT window, nothing
// deferred in the WAL.
func (s *LayerSuite) TestRenameOfBaseOnlyPathGoesSynchronous() {
	// Create source file in memfs (base-only — never touched via the layer).
	_, err := s.fs.Mkdir(s.ctx, "delegated", 0o40755)
	s.Require().Equal(proto.FsError_FS_OK, err)
	_, _, err = s.fs.Create(s.ctx, "delegated", "old.txt", 0, 0o100644)
	s.Require().Equal(proto.FsError_FS_OK, err)

	s.grant("delegated")

	ferr := s.layer.Rename(s.ctx, "delegated/old.txt", "delegated/new.txt")
	s.Require().Equal(proto.FsError_FS_OK, ferr)

	// memfs must have received the rename synchronously — no deferred window.
	_, innerErr := s.fs.Stat(s.ctx, "delegated/new.txt")
	s.Equal(proto.FsError_FS_OK, innerErr, "base-only rename must reach inner synchronously")
	_, innerErr = s.fs.Stat(s.ctx, "delegated/old.txt")
	s.Equal(proto.FsError_FS_ENOENT, innerErr, "old path must be gone in memfs")

	// Nothing deferred in the WAL.
	ops, logerr := s.log.Replay(0)
	s.Require().NoError(logerr)
	s.Empty(ops, "no deferred rename recorded for a base-only source")
}

// ── Test 8b: overlay-owned rename (atomic-write fast path) stays deferred ────
//
// TestRenameOfOverlayCreatedFileStaysDeferred pins the surviving fast path:
// when the rename's source is entirely the overlay's own creation (the
// `create tmp → rename over` atomic-write idiom), the overlay CAN represent
// the destination, so the rename still defers via the WAL — inner is never
// touched and the destination is visible immediately (RYOW).
func (s *LayerSuite) TestRenameOfOverlayCreatedFileStaysDeferred() {
	s.grant("dir")
	_, err := s.fs.Mkdir(s.ctx, "dir", 0o40755)
	s.Require().Equal(proto.FsError_FS_OK, err)

	_, _, st := s.layer.Create(s.ctx, "dir", "tmp.txt", 0, 0o644)
	s.Require().Equal(proto.FsError_FS_OK, st)

	st = s.layer.Rename(s.ctx, "dir/tmp.txt", "dir/final.txt")
	s.Require().Equal(proto.FsError_FS_OK, st)

	// inner must NOT have seen either path — the rename is purely in the overlay.
	_, innerErr := s.fs.Stat(s.ctx, "dir/final.txt")
	s.Equal(proto.FsError_FS_ENOENT, innerErr, "overlay-owned rename must not reach inner")

	// The WAL log must contain the deferred OpRename.
	ops, logerr := s.log.Replay(0)
	s.Require().NoError(logerr)
	found := false
	for _, op := range ops {
		if op.Kind == wal.OpRename {
			found = true
		}
	}
	s.True(found, "deferred OpRename must be recorded in the WAL")

	// RYOW: destination visible immediately; source gone.
	attr, ferr := s.layer.Stat(s.ctx, "dir/final.txt")
	s.Equal(proto.FsError_FS_OK, ferr)
	s.NotNil(attr, "RYOW: destination visible immediately")

	_, ferr = s.layer.Stat(s.ctx, "dir/tmp.txt")
	s.Equal(proto.FsError_FS_ENOENT, ferr, "RYOW: source tombstoned")
}

// ── Test 8c: rename of a pending base-delta source flushes then syncs ────────
//
// TestRenameFlushesPendingBaseDeltaBeforeSyncRename is the discriminating case
// for the flush-first rule: the source is NOT overlay-owned (it is a
// base-delta node — a pre-existing inner file with a pending SetAttr), so the
// rename must run synchronously against inner. But if it ran synchronously
// WITHOUT first flushing, the pending SetAttr recorded against the old path
// would be stranded — the next flush would try to apply it to a path the
// rename already moved away from, and the server would ENOENT it (ordered
// halt = data loss). Rename must therefore flush the pending SetAttr first
// (observed here as it landing on the fake Apply stream, with the WAL log
// left empty), and only then execute the rename against inner. Crucially, the
// fake Apply stream must never see an OpRename — the rename itself always
// took the synchronous inner path, never the WAL.
func (s *LayerSuite) TestRenameFlushesPendingBaseDeltaBeforeSyncRename() {
	_, err := s.fs.Mkdir(s.ctx, "dir", 0o40755)
	s.Require().Equal(proto.FsError_FS_OK, err)
	s.writeInner("dir/data.txt", []byte("AAAA"))
	s.grant("dir")

	// SetAttr (mtime touch) on a base-only path creates a base-delta overlay
	// node with pending state — overlayOwns must be false for it.
	mtime := time.Unix(1_700_000_000, 0)
	_, ferr := s.layer.SetAttr(s.ctx, "dir/data.txt", backend.SetAttrIn{
		Valid: backend.FATTR_MTIME,
		Mtime: &mtime,
	})
	s.Require().Equal(proto.FsError_FS_OK, ferr)
	s.True(s.coord.Has("dir/data.txt"), "SetAttr must have created a pending base-delta node")

	ferr = s.layer.Rename(s.ctx, "dir/data.txt", "dir/moved.txt")
	s.Require().Equal(proto.FsError_FS_OK, ferr)

	// The pending SetAttr must have been flushed (WAL log now empty) before the
	// synchronous rename ran.
	ops, logerr := s.log.Replay(0)
	s.Require().NoError(logerr)
	s.Empty(ops, "pending base-delta state must be flushed before the sync rename, leaving the WAL empty")

	// The synchronous rename must have reached inner.
	_, innerErr := s.fs.Stat(s.ctx, "dir/moved.txt")
	s.Equal(proto.FsError_FS_OK, innerErr, "base-delta rename must reach inner synchronously")
	_, innerErr = s.fs.Stat(s.ctx, "dir/data.txt")
	s.Equal(proto.FsError_FS_ENOENT, innerErr, "old path must be gone in memfs")

	// The flush must have carried the pending SetAttr — and the rename itself
	// must NEVER have gone through the WAL (no OpRename ever sent to Apply).
	var sawSetAttr, sawRename bool
	for _, op := range s.sentOps() {
		if op.GetSetAttr() != nil {
			sawSetAttr = true
		}
		if op.GetRename() != nil {
			sawRename = true
		}
	}
	s.True(sawSetAttr, "the flush must have sent the pending SetAttr")
	s.False(sawRename, "the rename must never be deferred through the WAL for a base-delta source")
}

// ── Test 9: Read on delegated pending path → overlay bytes merged ─────────────

func (s *LayerSuite) TestDelegatedRead_OverlayBytesMerged() {
	// Create a file in memfs with initial bytes "hello".
	_, _, ferr := s.fs.Create(s.ctx, "", "msg.txt", 0, 0o100644)
	s.Require().Equal(proto.FsError_FS_OK, ferr)
	wfh, ferr := s.fs.Open(s.ctx, "msg.txt", 0)
	s.Require().Equal(proto.FsError_FS_OK, ferr)
	_, ferr = s.fs.Write(s.ctx, wfh, 0, []byte("hello"))
	s.Require().Equal(proto.FsError_FS_OK, ferr)
	_ = s.fs.Release(s.ctx, wfh)

	s.grant("msg.txt") // grant the specific file path

	// Inject a pending overlay write for the last byte (override 'o' with 'O').
	recErr := s.coord.RecordOp(wal.Op{
		Kind:   wal.OpWrite,
		Path:   "msg.txt",
		Offset: 4,
		Data:   []byte("O"),
	})
	s.Require().NoError(recErr)

	// Open a handle (from inner, since the file really exists there).
	fh, ferr := s.layer.Open(s.ctx, "msg.txt", 0)
	s.Require().Equal(proto.FsError_FS_OK, ferr)
	defer func() { _ = s.layer.Release(s.ctx, fh) }()

	dest := make([]byte, 5)
	n, ferr := s.layer.Read(s.ctx, fh, 0, dest)
	s.Require().Equal(proto.FsError_FS_OK, ferr)
	s.Equal(5, n)
	s.Equal([]byte("hellO"), dest, "pending overlay byte must override base byte")
}

// ── Test 10: GetXAttr — pending set/removal/passthrough ──────────────────────

func (s *LayerSuite) TestGetXAttr_OverlayState() {
	// Create a base file in memfs.
	_, _, err := s.fs.Create(s.ctx, "", "xtest.txt", 0, 0o100644)
	s.Require().Equal(proto.FsError_FS_OK, err)
	// Set a base xattr in memfs.
	ferr := s.fs.SetXAttr(s.ctx, "xtest.txt", "user.color", []byte("red"), 0)
	s.Require().Equal(proto.FsError_FS_OK, ferr)

	s.grant("")

	// 1. No pending state → falls through to inner → "red".
	val, ferr := s.layer.GetXAttr(s.ctx, "xtest.txt", "user.color")
	s.Require().Equal(proto.FsError_FS_OK, ferr)
	s.Equal([]byte("red"), val, "no pending state: must fall through to inner")

	// 2. Delegated SetXAttr → pending set → layer returns new value.
	ferr = s.layer.SetXAttr(s.ctx, "xtest.txt", "user.color", []byte("blue"), 0)
	s.Require().Equal(proto.FsError_FS_OK, ferr)
	val, ferr = s.layer.GetXAttr(s.ctx, "xtest.txt", "user.color")
	s.Require().Equal(proto.FsError_FS_OK, ferr)
	s.Equal([]byte("blue"), val, "pending set must override inner xattr")

	// 3. Delegated RemoveXAttr → pending removal → ENO_XATTR.
	ferr = s.layer.RemoveXAttr(s.ctx, "xtest.txt", "user.color")
	s.Require().Equal(proto.FsError_FS_OK, ferr)
	_, ferr = s.layer.GetXAttr(s.ctx, "xtest.txt", "user.color")
	s.Equal(proto.FsError_FS_ENO_XATTR, ferr, "pending removal must return ENO_XATTR without hitting inner")
}

// ── Test 11: HEADLINE — Create → Write → Read, no flush, memfs untouched ─────
//
// THE HEADLINE TEST (Task 14b): a delegated Create returns a syntheticHandle
// that is immediately writable and readable through the overlay, before any
// WAL flush. inner (memfs) must never see the file.

func (s *LayerSuite) TestSyntheticHandle_CreateWriteRead_PreFlush() {
	s.grant("d")

	// Pre-create parent in memfs so delegation oracle knows the subtree.
	_, err := s.fs.Mkdir(s.ctx, "d", 0o40755)
	s.Require().Equal(proto.FsError_FS_OK, err)

	// Create a new file under the delegation — returns a syntheticHandle.
	fh, _, ferr := s.layer.Create(s.ctx, "d", "f.txt", 0, 0o100644)
	s.Require().Equal(proto.FsError_FS_OK, ferr)
	s.Require().NotNil(fh)

	// Write "hello" via the syntheticHandle.
	n, ferr := s.layer.Write(s.ctx, fh, 0, []byte("hello"))
	s.Require().Equal(proto.FsError_FS_OK, ferr)
	s.Equal(uint32(5), n)

	// Read back "hello" via the same handle (overlay only, no inner).
	dest := make([]byte, 5)
	rn, ferr := s.layer.Read(s.ctx, fh, 0, dest)
	s.Require().Equal(proto.FsError_FS_OK, ferr)
	s.Equal(5, rn)
	s.Equal([]byte("hello"), dest, "Read must return the bytes written via syntheticHandle")

	// Stat: size must reflect the write.
	attr, ferr := s.layer.Stat(s.ctx, "d/f.txt")
	s.Require().Equal(proto.FsError_FS_OK, ferr)
	s.Equal(uint64(5), attr.Size, "Stat must report the written size")

	// Release the handle.
	s.Require().Equal(proto.FsError_FS_OK, s.layer.Release(s.ctx, fh))

	// memfs (inner) must NOT have seen the file at all.
	_, innerErr := s.fs.Stat(s.ctx, "d/f.txt")
	s.Equal(proto.FsError_FS_ENOENT, innerErr, "memfs must not have the file before WAL flush")
}

// ── Test 12: Close + reopen → still readable via new syntheticHandle ──────────
//
// After Release + Open on the same overlay-created path, Open must return a
// fresh syntheticHandle (not call inner), and Read must still return the
// previously written bytes.

func (s *LayerSuite) TestSyntheticHandle_Reopen_StillReadable() {
	s.grant("d")
	_, err := s.fs.Mkdir(s.ctx, "d", 0o40755)
	s.Require().Equal(proto.FsError_FS_OK, err)

	fh, _, ferr := s.layer.Create(s.ctx, "d", "g.txt", 0, 0o100644)
	s.Require().Equal(proto.FsError_FS_OK, ferr)

	_, ferr = s.layer.Write(s.ctx, fh, 0, []byte("world"))
	s.Require().Equal(proto.FsError_FS_OK, ferr)
	s.Require().Equal(proto.FsError_FS_OK, s.layer.Release(s.ctx, fh))

	// Reopen — must return a syntheticHandle (not ENOENT from inner).
	fh2, ferr := s.layer.Open(s.ctx, "d/g.txt", 0)
	s.Require().Equal(proto.FsError_FS_OK, ferr)
	s.Require().NotNil(fh2)
	defer func() { _ = s.layer.Release(s.ctx, fh2) }()

	// Read must serve the previously written bytes.
	dest := make([]byte, 5)
	rn, ferr := s.layer.Read(s.ctx, fh2, 0, dest)
	s.Require().Equal(proto.FsError_FS_OK, ferr)
	s.Equal(5, rn)
	s.Equal([]byte("world"), dest, "re-opened syntheticHandle must serve previously written bytes")
}

// ── Test 13: Write recorded exactly once (no double-record) ──────────────────
//
// Write to a syntheticHandle must produce exactly one WAL entry (one RecordOp).
// Non-delegated Create → inner passthrough; Write on that inner handle should
// not produce any WAL entry.

func (s *LayerSuite) TestSyntheticHandle_WriteRecordedExactlyOnce() {
	s.grant("wal")
	_, err := s.fs.Mkdir(s.ctx, "wal", 0o40755)
	s.Require().Equal(proto.FsError_FS_OK, err)

	fh, _, ferr := s.layer.Create(s.ctx, "wal", "only.txt", 0, 0o100644)
	s.Require().Equal(proto.FsError_FS_OK, ferr)

	_, ferr = s.layer.Write(s.ctx, fh, 0, []byte("once"))
	s.Require().Equal(proto.FsError_FS_OK, ferr)
	s.Require().Equal(proto.FsError_FS_OK, s.layer.Release(s.ctx, fh))

	// The WAL log must contain exactly 2 entries: OpCreate + OpWrite.
	ops, logerr := s.log.Replay(0)
	s.Require().NoError(logerr)
	s.Require().Len(ops, 2, "WAL must have exactly OpCreate + OpWrite, no duplicates")
	s.Equal(wal.OpCreate, ops[0].Kind)
	s.Equal(wal.OpWrite, ops[1].Kind)
	s.Equal([]byte("once"), ops[1].Data)
}

// ── Test 14: non-delegated Create → passthrough; inner sees it; no WAL ────────

func (s *LayerSuite) TestNonDelegated_Create_PassthroughToInner() {
	// No grant → not delegated.
	fh, _, ferr := s.layer.Create(s.ctx, "", "direct.txt", 0, 0o100644)
	s.Require().Equal(proto.FsError_FS_OK, ferr)
	s.Require().NotNil(fh)
	defer func() { _ = s.layer.Release(s.ctx, fh) }()

	// memfs must have the file.
	_, innerErr := s.fs.Stat(s.ctx, "direct.txt")
	s.Equal(proto.FsError_FS_OK, innerErr, "non-delegated Create must reach inner")

	// WAL log must be empty.
	ops, logerr := s.log.Replay(0)
	s.Require().NoError(logerr)
	s.Empty(ops, "non-delegated Create must not produce a WAL entry")
}

// ── Test 15: undelegated mutation falls back to inner (contract pin) ─────────
//
// This pins the post-refactor contract: admission now lives in
// Coordinator.RecordOp (Task 3), not an IsDelegated gate in the Layer. With no
// grant applied, every mutating op must still reach inner synchronously, and
// nothing may be recorded in the WAL.

func (s *LayerSuite) TestMutationFallsBackToInnerWhenNotDelegated() {
	// No grant applied.
	_, err := s.fs.Mkdir(s.ctx, "plain", 0o40755)
	s.Require().Equal(proto.FsError_FS_OK, err)
	_, _, err = s.fs.Create(s.ctx, "plain", "file.txt", 0, 0o100644)
	s.Require().Equal(proto.FsError_FS_OK, err)

	st := s.layer.Unlink(s.ctx, "plain/file.txt")
	s.Equal(proto.FsError_FS_OK, st)

	_, innerErr := s.fs.Stat(s.ctx, "plain/file.txt")
	s.Equal(proto.FsError_FS_ENOENT, innerErr, "undelegated unlink must reach inner")

	ops, logerr := s.log.Replay(0)
	s.Require().NoError(logerr)
	s.Empty(ops, "nothing recorded in the WAL")
}

// ── Test 16: draining region blocks, then falls back after handoff ───────────
//
// blockingRecallFlusher implements delegation.RecallFlusher and blocks
// FlushForRecall until release is closed — used to hold a root in the
// "draining" state for the duration of the test, mirroring the pattern in
// wal_test.go's TestDrain_FallsBackToWireWhileRootIsDraining.
type blockingRecallFlusher struct {
	release chan struct{}
}

func (b *blockingRecallFlusher) FlushForRecall(_ context.Context, _ string) error {
	<-b.release
	return nil
}

// TestMutationBlocksOnDrainThenFallsBackAfterHandoff is the discriminating
// test for recordDeferred: while a recall is draining the covering root,
// RecordOp refuses immediately (ErrNotDelegated), so a concurrent Unlink must
// block in mgr.WaitDrained rather than either succeeding early or forwarding
// to inner prematurely. Once the recall flush completes, the handoff drops
// the grant, the retried RecordOp is refused again, and Unlink falls back to
// inner synchronously — recording nothing in the WAL.
func (s *LayerSuite) TestMutationBlocksOnDrainThenFallsBackAfterHandoff() {
	_, err := s.fs.Mkdir(s.ctx, "dir", 0o40755)
	s.Require().Equal(proto.FsError_FS_OK, err)
	_, _, err = s.fs.Create(s.ctx, "dir", "f.txt", 0, 0o100644)
	s.Require().Equal(proto.FsError_FS_OK, err)

	s.grant("dir")

	release := make(chan struct{})
	s.mgr.SetRecallFlusher(&blockingRecallFlusher{release: release})

	recallDone := make(chan struct{})
	go func() {
		_ = s.mgr.OnRecall(s.ctx, "dir")
		close(recallDone)
	}()

	// Wait for the recall to mark "dir" draining.
	for s.mgr.IsWriteDelegated("dir/f.txt") {
		time.Sleep(time.Millisecond)
	}
	s.True(s.mgr.IsDelegated("dir/f.txt"), "grant is still held while draining")

	unlinkDone := make(chan proto.FsError, 1)
	go func() {
		unlinkDone <- s.layer.Unlink(s.ctx, "dir/f.txt")
	}()

	// Must block while the recall flush is in flight.
	select {
	case <-unlinkDone:
		s.Fail("Unlink must block while the region is draining")
	case <-time.After(50 * time.Millisecond):
	}

	// Complete the recall flush — the handoff drops the grant.
	close(release)
	<-recallDone

	var st proto.FsError
	select {
	case st = <-unlinkDone:
	case <-time.After(2 * time.Second):
		s.FailNow("Unlink did not unblock after the drain completed")
	}
	s.Equal(proto.FsError_FS_OK, st)

	// Fell back to inner: memfs must have the unlink.
	_, innerErr := s.fs.Stat(s.ctx, "dir/f.txt")
	s.Equal(proto.FsError_FS_ENOENT, innerErr, "unlink must have reached inner after fallback")

	// Nothing recorded in the WAL (the grant was already dropped by the time
	// the retried RecordOp ran).
	ops, logerr := s.log.Replay(0)
	s.Require().NoError(logerr)
	s.Empty(ops, "nothing must be recorded in the WAL after fallback")
}

// ── Test 17: flushed synthetic handle reads through to inner ─────────────────
//
// TestSyntheticHandleReadsAfterFlushServeFromInner is the headline regression
// test (Task 5): a still-open syntheticHandle whose overlay node was cleared
// by a completed flush must serve reads from inner (the server), not empty
// data from a nil-base overlay merge.

func (s *LayerSuite) TestSyntheticHandleReadsAfterFlushServeFromInner() {
	s.grant("dir")
	_, err := s.fs.Mkdir(s.ctx, "dir", 0o40755)
	s.Require().Equal(proto.FsError_FS_OK, err)

	fh, _, st := s.layer.Create(s.ctx, "dir", "f.txt", 0, 0o644)
	s.Require().Equal(proto.FsError_FS_OK, st)
	_, st = s.layer.Write(s.ctx, fh, 0, []byte("hello"))
	s.Require().Equal(proto.FsError_FS_OK, st)

	// Simulate a completed interval flush: server has the bytes, overlay is
	// clear. The real Apply pipeline is out of scope for this unit test — we
	// prime inner directly with the bytes the (fake) flush would have sent,
	// then drive the coordinator's REAL Flush so the overlay actually clears.
	s.writeInner("dir/f.txt", []byte("hello"))
	s.flushAll()
	s.False(s.coord.Has("dir/f.txt"), "overlay must be cleared after the flush")

	dest := make([]byte, 5)
	n, st := s.layer.Read(s.ctx, fh, 0, dest)
	s.Equal(proto.FsError_FS_OK, st)
	s.Equal("hello", string(dest[:n]), "read through a flushed synthetic handle must serve server bytes, not empty")
}

// ── Test 18: orphaned synthetic handle writes through after recall ───────────
//
// TestOrphanedSyntheticHandleWritesThroughAfterRecall proves the write side
// of the same bug: a still-open syntheticHandle whose delegation was recalled
// (grant dropped by a completed handoff) has nowhere left to defer its write
// to; it must write through a transient inner handle instead of failing.

func (s *LayerSuite) TestOrphanedSyntheticHandleWritesThroughAfterRecall() {
	s.grant("dir")
	_, err := s.fs.Mkdir(s.ctx, "dir", 0o40755)
	s.Require().Equal(proto.FsError_FS_OK, err)

	fh, _, st := s.layer.Create(s.ctx, "dir", "f.txt", 0, 0o644)
	s.Require().Equal(proto.FsError_FS_OK, st)
	_, st = s.layer.Write(s.ctx, fh, 0, []byte("hello"))
	s.Require().Equal(proto.FsError_FS_OK, st)

	// Simulate the recall flush materialising the file on the server: prime
	// inner directly (the real Apply pipeline is out of scope here).
	s.writeInner("dir/f.txt", []byte("hello"))

	// Wire the coordinator as the recall flusher and drive a REAL recall —
	// FlushForRecall flushes the WAL (the fake Apply stream acks it in full)
	// and the handoff drops the grant.
	s.mgr.SetRecallFlusher(s.coord)
	s.Require().NoError(s.mgr.OnRecall(s.ctx, "dir"))
	s.False(s.mgr.IsDelegated("dir/f.txt"), "grant must be dropped after a completed recall handoff")

	// fh is still open (never Released) — an orphaned synthetic handle. Write
	// through it must land on inner via writeThrough.
	n, st := s.layer.Write(s.ctx, fh, 5, []byte(" world"))
	s.Require().Equal(proto.FsError_FS_OK, st)
	s.Equal(uint32(6), n)

	rfh, ferr := s.fs.Open(s.ctx, "dir/f.txt", 0)
	s.Require().Equal(proto.FsError_FS_OK, ferr)
	defer func() { _ = s.fs.Release(s.ctx, rfh) }()
	dest := make([]byte, 11)
	rn, ferr := s.fs.Read(s.ctx, rfh, 0, dest)
	s.Require().Equal(proto.FsError_FS_OK, ferr)
	s.Equal("hello world", string(dest[:rn]), "orphaned synthetic handle's write must land on inner via writeThrough")
}

// ── Test 19: synthetic handle read merges pending over flushed base ──────────
//
// TestSyntheticHandleReadMergesPendingOverFlushedBase pins the baseDelta arm
// of the Read routing condition. A synthetic file is flushed (overlay cleared),
// then re-written through the same open handle while still delegated, creating
// a base-delta overlay node (pending intervals only). Reading it must merge
// pending bytes over the flushed base via readThrough, not serve a nil-base
// overlay view with holes.

func (s *LayerSuite) TestSyntheticHandleReadMergesPendingOverFlushedBase() {
	s.grant("dir")
	_, err := s.fs.Mkdir(s.ctx, "dir", 0o40755)
	s.Require().Equal(proto.FsError_FS_OK, err)

	// 1. Create and write "AAAA" via synthetic handle.
	fh, _, ferr := s.layer.Create(s.ctx, "dir", "f.txt", 0, 0o100644)
	s.Require().Equal(proto.FsError_FS_OK, ferr)
	_, ferr = s.layer.Write(s.ctx, fh, 0, []byte("AAAA"))
	s.Require().Equal(proto.FsError_FS_OK, ferr)

	// 2. Flush the overlay: prime inner with flushed bytes and drive a real flush
	// (fakeApplyStream commits the batch). Overlay node is now cleared.
	s.writeInner("dir/f.txt", []byte("AAAA"))
	s.flushAll()
	s.False(s.coord.Has("dir/f.txt"), "overlay must be cleared after flush")

	// 3. Write "BB" at offset 1 through the SAME still-open handle.
	// Since the overlay node was cleared by flush, RecordOp sees ok=false and
	// creates a base-delta node with pending interval [1,3).
	n, ferr := s.layer.Write(s.ctx, fh, 1, []byte("BB"))
	s.Require().Equal(proto.FsError_FS_OK, ferr)
	s.Equal(uint32(2), n)
	s.True(s.coord.Has("dir/f.txt"), "overlay must have a base-delta node after write")

	// 4. Read 4 bytes at offset 0 through the same handle. Must merge:
	//    - bytes 0, 3 from inner (flushed base "AAAA")
	//    - bytes 1-2 from overlay (pending "BB")
	//    Result: "ABBA"
	dest := make([]byte, 4)
	rn, ferr := s.layer.Read(s.ctx, fh, 0, dest)
	s.Require().Equal(proto.FsError_FS_OK, ferr)
	s.Equal(4, rn)
	s.Equal([]byte("ABBA"), dest[:rn],
		"read through base-delta synthetic handle must merge pending over flushed base")

	s.Require().Equal(proto.FsError_FS_OK, s.layer.Release(s.ctx, fh))
}

// ── Test: Fsync on transport-backed handle flushes pending WAL ─────────────────

func (s *LayerSuite) TestFsyncOnTransportHandleFlushesPendingWal() {
	s.grant("dir")

	// Create the parent in memfs.
	_, err := s.fs.Mkdir(s.ctx, "dir", 0o40755)
	s.Require().Equal(proto.FsError_FS_OK, err)

	// Write a base file to memfs (pre-existing, not created by overlay).
	s.writeInner("dir/f.txt", []byte("base"))

	// SetAttr on the base file creates a base-delta pending state.
	newPerm := uint32(0o600)
	in := backend.SetAttrIn{
		Valid: backend.FATTR_MODE,
		Mode:  newPerm,
	}
	_, ferr := s.layer.SetAttr(s.ctx, "dir/f.txt", in)
	s.Require().Equal(proto.FsError_FS_OK, ferr)

	// Confirm pending state exists in the overlay.
	s.True(s.coord.Has("dir/f.txt"), "SetAttr must create base-delta pending state")

	// Open the file (returns inner-backed handle for base-delta path).
	fh, ferr := s.layer.Open(s.ctx, "dir/f.txt", 0)
	s.Require().Equal(proto.FsError_FS_OK, ferr)
	s.Require().NotNil(fh)

	// Fsync on the inner-backed handle must flush pending WAL state.
	ferr = s.layer.Fsync(s.ctx, fh, 0)
	s.Equal(proto.FsError_FS_OK, ferr)

	// Release the handle.
	s.Require().Equal(proto.FsError_FS_OK, s.layer.Release(s.ctx, fh))

	// Verify the log is empty: pending SetAttr was flushed to the server.
	ops, logerr := s.log.Replay(0)
	s.Require().NoError(logerr)
	s.Empty(ops, "fsync must flush pending WAL ops for the path, leaving log empty")

	// Verify the pending SetAttr crossed the Apply stream — not just removed from log.
	var sawSetAttr bool
	for _, op := range s.sentOps() {
		if op.GetSetAttr() != nil {
			sawSetAttr = true
			break
		}
	}
	s.True(sawSetAttr, "fsync must flush the pending SetAttr over the Apply stream")
}

// ── helpers ───────────────────────────────────────────────────────────────────

func dirNames(entries []backend.DirEntryPlus) []string {
	names := make([]string, len(entries))
	for i, e := range entries {
		names[i] = e.Name
	}
	return names
}

func TestLayerSuite(t *testing.T) {
	suite.Run(t, new(LayerSuite))
}
