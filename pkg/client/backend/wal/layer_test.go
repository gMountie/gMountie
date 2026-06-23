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
	"path/filepath"
	"syscall"
	"testing"

	"github.com/stretchr/testify/suite"
	"go.gmountie.dev/gmountie/pkg/client/backend"
	"go.gmountie.dev/gmountie/pkg/client/backend/delegation"
	"go.gmountie.dev/gmountie/pkg/client/backend/memfs"
	"go.gmountie.dev/gmountie/pkg/client/backend/wal"
	"go.gmountie.dev/gmountie/pkg/proto"
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
}

func (s *LayerSuite) SetupTest() {
	s.fs = memfs.New()
	s.mgr = delegation.NewManager(noopInv{})
	s.log = openLayerLog(s.T())
	s.ovl = wal.NewOverlay()
	s.coord = wal.NewCoordinator(s.mgr, s.log, s.ovl)
	s.layer = wal.NewLayer(s.fs, s.mgr, s.coord)
	s.ctx = context.Background()
}

// grant makes root and all paths under it delegated.
func (s *LayerSuite) grant(root string) {
	s.mgr.Apply(&proto.DelegationGrant{GrantedRoot: root})
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

// ── Test 8: in-delegation rename → deferred (memfs does not see it) ──────────

func (s *LayerSuite) TestInDelegationRename_Deferred() {
	// Create source file in memfs.
	_, err := s.fs.Mkdir(s.ctx, "delegated", 0o40755)
	s.Require().Equal(proto.FsError_FS_OK, err)
	_, _, err = s.fs.Create(s.ctx, "delegated", "old.txt", 0, 0o100644)
	s.Require().Equal(proto.FsError_FS_OK, err)

	s.grant("delegated")

	ferr := s.layer.Rename(s.ctx, "delegated/old.txt", "delegated/new.txt")
	s.Require().Equal(proto.FsError_FS_OK, ferr)

	// memfs must NOT have seen the rename.
	_, innerErr := s.fs.Stat(s.ctx, "delegated/old.txt")
	s.Equal(proto.FsError_FS_OK, innerErr, "memfs must still have the old path (deferred)")
	_, innerErr = s.fs.Stat(s.ctx, "delegated/new.txt")
	s.Equal(proto.FsError_FS_ENOENT, innerErr, "memfs must not have the new path (deferred)")
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
