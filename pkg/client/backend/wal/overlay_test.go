package wal_test

import (
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/suite"

	"go.gmountie.dev/gmountie/pkg/client/backend"
	"go.gmountie.dev/gmountie/pkg/client/backend/wal"
)

// OverlaySuite exercises the Overlay's merge logic exhaustively.
type OverlaySuite struct {
	suite.Suite
}

func TestOverlaySuite(t *testing.T) {
	suite.Run(t, new(OverlaySuite))
}

// ── helpers ──────────────────────────────────────────────────────────────────

func (s *OverlaySuite) newOverlay() *wal.Overlay {
	return wal.NewOverlay()
}

func (s *OverlaySuite) createOp(path string, mode uint32) wal.Op {
	return wal.Op{Kind: wal.OpCreate, Path: path, Mode: mode}
}

func (s *OverlaySuite) mkdirOp(path string, mode uint32) wal.Op {
	return wal.Op{Kind: wal.OpMkdir, Path: path, Mode: mode}
}

func (s *OverlaySuite) writeOp(path string, off int64, data []byte) wal.Op {
	return wal.Op{Kind: wal.OpWrite, Path: path, Offset: off, Data: data}
}

func (s *OverlaySuite) unlinkOp(path string) wal.Op {
	return wal.Op{Kind: wal.OpUnlink, Path: path}
}

func (s *OverlaySuite) rmdirOp(path string) wal.Op {
	return wal.Op{Kind: wal.OpRmdir, Path: path}
}

func (s *OverlaySuite) renameOp(oldPath, newPath string) wal.Op {
	return wal.Op{Kind: wal.OpRename, Path: oldPath, NewPath: newPath}
}

func (s *OverlaySuite) setAttrOp(path string, mode uint32) wal.Op {
	return wal.Op{Kind: wal.OpSetAttr, Path: path, Mode: mode, Valid: backend.FATTR_MODE}
}

func (s *OverlaySuite) setAttrUIDGIDOp(path string, uid, gid uint32) wal.Op {
	return wal.Op{
		Kind:  wal.OpSetAttr,
		Path:  path,
		UID:   uid,
		GID:   gid,
		Valid: backend.FATTR_UID | backend.FATTR_GID,
	}
}

func (s *OverlaySuite) setAttrSizeOp(path string, size uint64) wal.Op {
	return wal.Op{
		Kind:  wal.OpSetAttr,
		Path:  path,
		Size:  size,
		Valid: backend.FATTR_SIZE,
	}
}

func (s *OverlaySuite) setAttrTimesOp(path string, atimeSec, mtimeSec int64, atimeNsec, mtimeNsec uint32) wal.Op {
	return wal.Op{
		Kind:      wal.OpSetAttr,
		Path:      path,
		AtimeSec:  atimeSec,
		AtimeNsec: atimeNsec,
		MtimeSec:  mtimeSec,
		MtimeNsec: mtimeNsec,
		Valid:     backend.FATTR_ATIME | backend.FATTR_MTIME,
	}
}

func (s *OverlaySuite) setXAttrOp(path, name string, value []byte, flags uint32) wal.Op {
	return wal.Op{
		Kind:       wal.OpSetXAttr,
		Path:       path,
		XattrName:  name,
		XattrValue: value,
		XattrFlags: flags,
	}
}

func (s *OverlaySuite) removeXAttrOp(path, name string) wal.Op {
	return wal.Op{Kind: wal.OpRemoveXAttr, Path: path, XattrName: name}
}

func dirEntry(name string, mode uint32) backend.DirEntryPlus {
	return backend.DirEntryPlus{
		DirEntry: backend.DirEntry{Name: name, Mode: mode},
	}
}

// ── create / Stat ─────────────────────────────────────────────────────────────

func (s *OverlaySuite) TestCreateStatSeesPendingFile() {
	ov := s.newOverlay()
	ov.Apply(s.createOp("docs/readme.txt", 0o100644))

	attr, ok, tomb, baseDelta, _ := ov.Stat("docs/readme.txt")
	s.True(ok, "Stat should find the pending file")
	s.False(tomb, "should not be tombstoned")
	s.False(baseDelta, "fresh create is not a base-delta")
	s.NotNil(attr)
	s.Equal(uint32(0o100644), attr.Mode)
}

func (s *OverlaySuite) TestMkdirStatSeesPendingDir() {
	ov := s.newOverlay()
	ov.Apply(s.mkdirOp("newdir", 0o40755))

	attr, ok, tomb, baseDelta, _ := ov.Stat("newdir")
	s.True(ok)
	s.False(tomb)
	s.False(baseDelta, "fresh mkdir is not a base-delta")
	s.NotNil(attr)
}

func (s *OverlaySuite) TestStatAbsentReturnsNotOk() {
	ov := s.newOverlay()
	_, ok, _, _, _ := ov.Stat("nonexistent")
	s.False(ok)
}

// ── write / ReadMerge ─────────────────────────────────────────────────────────

func (s *OverlaySuite) TestWriteThenReadMergeOverlaysBytes() {
	ov := s.newOverlay()
	ov.Apply(s.createOp("file.txt", 0o100644))
	ov.Apply(s.writeOp("file.txt", 0, []byte("hello")))

	base := []byte("XXXXX")
	out := ov.ReadMerge("file.txt", 0, base)
	s.Equal([]byte("hello"), out)
}

func (s *OverlaySuite) TestByteRangeOverlay() {
	// Pending covers [10,20); base covers [0,100).
	// Expect: base[0:10] + pending[0:10] + base[20:100].
	ov := s.newOverlay()
	ov.Apply(s.createOp("file.txt", 0o100644))

	pendingData := bytes10to20() // 10 bytes: "PPPPPPPPPP"
	ov.Apply(s.writeOp("file.txt", 10, pendingData))

	base := make([]byte, 100)
	for i := range base {
		base[i] = 'B'
	}

	out := ov.ReadMerge("file.txt", 0, base)
	s.Len(out, 100) // same length as base (pending is within base)
	// bytes [0,10) must come from base
	s.Equal(base[:10], out[:10], "prefix should be from base")
	// bytes [10,20) must come from pending
	s.Equal(pendingData, out[10:20], "middle should be from pending")
	// bytes [20,100) must come from base
	s.Equal(base[20:], out[20:], "suffix should be from base")
}

func (s *OverlaySuite) TestReadMergePendingWritePastEOFExtends() {
	ov := s.newOverlay()
	ov.Apply(s.createOp("file.txt", 0o100644))
	ov.Apply(s.writeOp("file.txt", 50, []byte("tail")))

	base := make([]byte, 10) // base is only 10 bytes
	out := ov.ReadMerge("file.txt", 0, base)
	s.GreaterOrEqual(len(out), 54, "result should extend to cover pending write")
	s.Equal([]byte("tail"), out[50:54])
}

func (s *OverlaySuite) TestReadMergeNoPendingReturnsBase() {
	ov := s.newOverlay()
	base := []byte("unchanged")
	out := ov.ReadMerge("file.txt", 0, base)
	s.Equal(base, out, "no pending state → return base unchanged")
}

// ── unlink / tombstone ────────────────────────────────────────────────────────

func (s *OverlaySuite) TestUnlinkSetsTombstone() {
	ov := s.newOverlay()
	ov.Apply(s.createOp("file.txt", 0o100644))
	ov.Apply(s.unlinkOp("file.txt"))

	_, ok, tomb, _, _ := ov.Stat("file.txt")
	s.True(ok, "Has should still see the entry (tombstone IS a pending state)")
	s.True(tomb, "should be tombstoned")
}

func (s *OverlaySuite) TestRmdirSetsTombstone() {
	ov := s.newOverlay()
	ov.Apply(s.mkdirOp("emptydir", 0o40755))
	ov.Apply(s.rmdirOp("emptydir"))

	_, ok, tomb, _, _ := ov.Stat("emptydir")
	s.True(ok)
	s.True(tomb)
}

func (s *OverlaySuite) TestTombstoneResurrection() {
	// unlink then recreate → NOT tombstoned.
	ov := s.newOverlay()
	ov.Apply(s.createOp("file.txt", 0o100644))
	ov.Apply(s.unlinkOp("file.txt"))
	ov.Apply(s.createOp("file.txt", 0o100644))

	_, ok, tomb, _, _ := ov.Stat("file.txt")
	s.True(ok)
	s.False(tomb, "recreated file must not be tombstoned")
}

func (s *OverlaySuite) TestTombstoneOnlyFromBaseUnlink() {
	// Unlink a path that was never in pending → still creates a tombstone.
	ov := s.newOverlay()
	ov.Apply(s.unlinkOp("base-only-file.txt"))

	_, ok, tomb, _, _ := ov.Stat("base-only-file.txt")
	s.True(ok, "tombstone for a base-only file is still pending state")
	s.True(tomb)
}

// ── ListMerge ─────────────────────────────────────────────────────────────────

func (s *OverlaySuite) TestListMergeAddsPendingCreates() {
	ov := s.newOverlay()
	ov.Apply(s.createOp("dir/newfile.txt", 0o100644))

	base := []backend.DirEntryPlus{
		dirEntry("existing.txt", 0o100644),
	}
	result := ov.ListMerge("dir", base)

	names := entryNames(result)
	s.Contains(names, "existing.txt")
	s.Contains(names, "newfile.txt")
}

func (s *OverlaySuite) TestListMergeOmitsTombstonedBaseEntries() {
	ov := s.newOverlay()
	ov.Apply(s.unlinkOp("dir/deleted.txt"))

	base := []backend.DirEntryPlus{
		dirEntry("deleted.txt", 0o100644),
		dirEntry("kept.txt", 0o100644),
	}
	result := ov.ListMerge("dir", base)

	names := entryNames(result)
	s.NotContains(names, "deleted.txt", "tombstoned entry must be omitted")
	s.Contains(names, "kept.txt")
}

func (s *OverlaySuite) TestListMergeDoesNotDuplicateWrittenFile() {
	// A file that was written (not created) appears in both base and overlay.
	// ListMerge must list it exactly once.
	ov := s.newOverlay()
	ov.Apply(s.writeOp("dir/file.txt", 0, []byte("data")))

	base := []backend.DirEntryPlus{
		dirEntry("file.txt", 0o100644),
	}
	result := ov.ListMerge("dir", base)

	var count int
	for _, e := range result {
		if e.Name == "file.txt" {
			count++
		}
	}
	s.Equal(1, count, "written (existing) file must appear exactly once")
}

func (s *OverlaySuite) TestListMergeEmptyBase() {
	ov := s.newOverlay()
	ov.Apply(s.createOp("dir/only.txt", 0o100644))

	result := ov.ListMerge("dir", nil)
	names := entryNames(result)
	s.Contains(names, "only.txt")
	s.Len(result, 1)
}

func (s *OverlaySuite) TestListMergeRootDir() {
	ov := s.newOverlay()
	ov.Apply(s.createOp("toplevel.txt", 0o100644))

	base := []backend.DirEntryPlus{dirEntry("base.txt", 0o100644)}
	result := ov.ListMerge("", base)
	names := entryNames(result)
	s.Contains(names, "toplevel.txt")
	s.Contains(names, "base.txt")
}

// ── rename ────────────────────────────────────────────────────────────────────

func (s *OverlaySuite) TestRenameMovesNodeAndTombstonesOld() {
	ov := s.newOverlay()
	ov.Apply(s.createOp("old.txt", 0o100644))
	ov.Apply(s.renameOp("old.txt", "new.txt"))

	// new path is accessible
	attr, ok, tomb, _, _ := ov.Stat("new.txt")
	s.True(ok)
	s.False(tomb)
	s.NotNil(attr)

	// old path is tombstoned
	_, ok, tomb, _, _ = ov.Stat("old.txt")
	s.True(ok, "old path has a tombstone, which is pending state")
	s.True(tomb, "old path should be tombstoned after rename")
}

func (s *OverlaySuite) TestRenameOfBaseOnlyPathTombstonesOld() {
	// Rename a path that only exists in base.
	ov := s.newOverlay()
	ov.Apply(s.renameOp("base/old.txt", "base/new.txt"))

	_, ok, tomb, _, _ := ov.Stat("base/old.txt")
	s.True(ok)
	s.True(tomb)

	// The new path should NOT be in overlay (it came from base, not created).
	// However Has("base/new.txt") may return true if a synthetic placeholder is stored.
	// The critical requirement is that Stat("base/old.txt") is tombstoned.
}

func (s *OverlaySuite) TestRenameSubtreeMovesPendingChildren() {
	ov := s.newOverlay()
	ov.Apply(s.mkdirOp("dir", 0o40755))
	ov.Apply(s.createOp("dir/file.txt", 0o100644))
	ov.Apply(s.renameOp("dir", "newdir"))

	// Children should appear under newdir.
	_, ok, tomb, _, _ := ov.Stat("newdir/file.txt")
	s.True(ok)
	s.False(tomb)

	// Old children should be tombstoned.
	_, ok, tomb, _, _ = ov.Stat("dir/file.txt")
	s.True(ok)
	s.True(tomb)
}

// ── Has ───────────────────────────────────────────────────────────────────────

func (s *OverlaySuite) TestHasPendingFile() {
	ov := s.newOverlay()
	s.False(ov.Has("file.txt"))
	ov.Apply(s.createOp("file.txt", 0o100644))
	s.True(ov.Has("file.txt"))
}

func (s *OverlaySuite) TestHasTombstone() {
	ov := s.newOverlay()
	ov.Apply(s.unlinkOp("file.txt"))
	s.True(ov.Has("file.txt"), "tombstones count as pending state")
}

// ── Paths ─────────────────────────────────────────────────────────────────────

func (s *OverlaySuite) TestPathsEnumeratesAllPendingIncludingTombstones() {
	ov := s.newOverlay()
	ov.Apply(s.createOp("a.txt", 0o100644))
	ov.Apply(s.mkdirOp("b", 0o40755))
	ov.Apply(s.unlinkOp("c.txt"))                // tombstone only
	ov.Apply(s.writeOp("d.txt", 0, []byte("x"))) // write without prior create

	paths := ov.Paths()
	s.Contains(paths, "a.txt")
	s.Contains(paths, "b")
	s.Contains(paths, "c.txt", "tombstones must appear in Paths()")
	s.Contains(paths, "d.txt")
}

func (s *OverlaySuite) TestPathsEmptyOnFreshOverlay() {
	ov := s.newOverlay()
	s.Empty(ov.Paths())
}

// ── DropSubtree ───────────────────────────────────────────────────────────────

func (s *OverlaySuite) TestDropSubtreeRemovesPendingUnderRoot() {
	ov := s.newOverlay()
	ov.Apply(s.createOp("kept/a.txt", 0o100644))
	ov.Apply(s.createOp("drop/b.txt", 0o100644))
	ov.Apply(s.createOp("drop/c.txt", 0o100644))
	ov.Apply(s.unlinkOp("drop/d.txt")) // tombstone under drop

	ov.DropSubtree("drop")

	s.True(ov.Has("kept/a.txt"), "kept path must survive")
	s.False(ov.Has("drop/b.txt"), "b.txt under drop must be gone")
	s.False(ov.Has("drop/c.txt"), "c.txt under drop must be gone")
	s.False(ov.Has("drop/d.txt"), "tombstone under drop must be gone")
}

func (s *OverlaySuite) TestDropSubtreeExactMatch() {
	// Ensure "drop" does not match "dropx" — prefix must be "/" bounded.
	ov := s.newOverlay()
	ov.Apply(s.createOp("drop/a.txt", 0o100644))
	ov.Apply(s.createOp("dropx/b.txt", 0o100644))

	ov.DropSubtree("drop")

	s.False(ov.Has("drop/a.txt"), "drop/a.txt must be removed")
	s.True(ov.Has("dropx/b.txt"), "dropx/b.txt must be kept (different prefix)")
}

func (s *OverlaySuite) TestDropSubtreeDropsExactPath() {
	ov := s.newOverlay()
	ov.Apply(s.createOp("file.txt", 0o100644))
	ov.DropSubtree("file.txt")
	s.False(ov.Has("file.txt"))
}

func (s *OverlaySuite) TestDropSubtreeAll() {
	ov := s.newOverlay()
	ov.Apply(s.createOp("a.txt", 0o100644))
	ov.Apply(s.createOp("b/c.txt", 0o100644))
	ov.DropSubtree("")
	s.Empty(ov.Paths())
}

// ── SetAttr ───────────────────────────────────────────────────────────────────

func (s *OverlaySuite) TestSetAttrUpdatesPendingNode() {
	ov := s.newOverlay()
	ov.Apply(s.createOp("file.txt", 0o100644))
	ov.Apply(s.setAttrOp("file.txt", 0o100600))

	attr, ok, tomb, baseDelta, _ := ov.Stat("file.txt")
	s.True(ok)
	s.False(tomb)
	s.False(baseDelta, "setattr on a pending-created file is NOT a base-delta")
	s.Equal(uint32(0o100600), attr.Mode)
}

func (s *OverlaySuite) TestSetAttrOnBaseOnlyPathCreatesEntry() {
	// SetAttr on a path that only exists in base should record it.
	ov := s.newOverlay()
	ov.Apply(s.setAttrOp("basefile.txt", 0o100600))

	s.True(ov.Has("basefile.txt"))
}

// ── SetAttr extended fields (UID/GID/SIZE/times) ─────────────────────────────

func (s *OverlaySuite) TestSetAttrUIDGIDOnCreatedFile() {
	ov := s.newOverlay()
	ov.Apply(s.createOp("file.txt", 0o100644))
	ov.Apply(s.setAttrUIDGIDOp("file.txt", 1000, 2000))

	attr, ok, tomb, baseDelta, valid := ov.Stat("file.txt")
	s.True(ok)
	s.False(tomb)
	s.False(baseDelta, "created file stays non-base-delta after setattr")
	s.Equal(uint32(1000), attr.Uid)
	s.Equal(uint32(2000), attr.Gid)
	// valid is meaningful only for base-delta nodes; for full-create it may still be set
	_ = valid
}

func (s *OverlaySuite) TestSetAttrSizeTruncatesData() {
	ov := s.newOverlay()
	ov.Apply(s.createOp("file.txt", 0o100644))
	// Write 10 bytes, then truncate to 5.
	ov.Apply(s.writeOp("file.txt", 0, []byte("0123456789")))
	ov.Apply(s.setAttrSizeOp("file.txt", 5))

	attr, ok, _, _, valid := ov.Stat("file.txt")
	s.True(ok)
	s.Equal(uint64(5), attr.Size)
	s.NotZero(valid & backend.FATTR_SIZE)

	// ReadMerge on a base of 10 bytes should see the truncation — the pending
	// data slice was cut to 5 bytes.
	base := []byte("XXXXXXXXXX") // 10 bytes base
	out := ov.ReadMerge("file.txt", 0, base)
	// The overlaid bytes are only 0-5; bytes [5,10) come from base.
	s.Equal([]byte("01234"), out[:5])
}

func (s *OverlaySuite) TestSetAttrSizeExtendsData() {
	ov := s.newOverlay()
	ov.Apply(s.createOp("file.txt", 0o100644))
	ov.Apply(s.writeOp("file.txt", 0, []byte("hello")))
	ov.Apply(s.setAttrSizeOp("file.txt", 10))

	attr, ok, _, _, valid := ov.Stat("file.txt")
	s.True(ok)
	s.Equal(uint64(10), attr.Size)
	s.NotZero(valid & backend.FATTR_SIZE)
}

func (s *OverlaySuite) TestSetAttrTimesRoundTrip() {
	ov := s.newOverlay()
	ov.Apply(s.createOp("file.txt", 0o100644))
	ov.Apply(s.setAttrTimesOp("file.txt", 1000, 2000, 500, 750))

	attr, ok, _, _, valid := ov.Stat("file.txt")
	s.True(ok)
	s.Equal(uint64(1000), attr.Atime)
	s.Equal(uint32(500), attr.Atimensec)
	s.Equal(uint64(2000), attr.Mtime)
	s.Equal(uint32(750), attr.Mtimensec)
	s.NotZero(valid & backend.FATTR_ATIME)
	s.NotZero(valid & backend.FATTR_MTIME)
}

// ── base-delta model ──────────────────────────────────────────────────────────

// TestBaseDeltaSetAttrModeOnly: SetAttr (chmod) on a path the overlay did NOT
// create → baseDelta=true, valid has only FATTR_MODE set, type bits NOT
// fabricated (mode field carries only the perm bits merged with zero type).
func (s *OverlaySuite) TestBaseDeltaSetAttrModeOnly() {
	ov := s.newOverlay()
	// Path only exists in base — no prior Create/Mkdir in overlay.
	ov.Apply(s.setAttrOp("basefile.txt", 0o100600))

	attr, ok, tomb, baseDelta, valid := ov.Stat("basefile.txt")
	s.True(ok)
	s.False(tomb)
	s.True(baseDelta, "setattr on a base-only path must be a base-delta")
	s.NotNil(attr)
	// Only FATTR_MODE should be set — uid/gid/size/times were not touched.
	s.Equal(uint32(backend.FATTR_MODE), valid, "only FATTR_MODE should be set")
	// The mode field carries only the perm bits — type bits are zero because
	// the delta has no type information (Task 10 supplies the base type bits).
	// setAttrOp uses Mode=0o100600; applySetAttr strips S_IFMT: 0o100600 & 0o7777 = 0o600.
	s.Equal(uint32(0o600), attr.Mode, "perm bits only, no type bits in base-delta mode")
}

// TestBaseDeltaUIDGID: SetAttr with UID+GID on a base-only path → baseDelta=true,
// valid=FATTR_UID|FATTR_GID, uid and gid carry the correct values.
func (s *OverlaySuite) TestBaseDeltaUIDGID() {
	ov := s.newOverlay()
	ov.Apply(s.setAttrUIDGIDOp("basefile.txt", 500, 600))

	attr, ok, _, baseDelta, valid := ov.Stat("basefile.txt")
	s.True(ok)
	s.True(baseDelta)
	s.Equal(uint32(backend.FATTR_UID|backend.FATTR_GID), valid)
	s.Equal(uint32(500), attr.Uid)
	s.Equal(uint32(600), attr.Gid)
}

// TestBaseDeltaWriteNoFATTR_SIZE: a plain write on a base-delta node must NOT
// set FATTR_SIZE in valid. The final size determination belongs to the caller
// (max(base.Size, overlay.attr.Size)).
func (s *OverlaySuite) TestBaseDeltaWriteNoFATTR_SIZE() {
	ov := s.newOverlay()
	// No prior Create — this is a write onto a base-only path.
	ov.Apply(s.writeOp("basefile.txt", 10, []byte("hello")))

	_, ok, _, baseDelta, valid := ov.Stat("basefile.txt")
	s.True(ok)
	s.True(baseDelta, "write on base-only path must be a base-delta")
	s.Zero(valid&backend.FATTR_SIZE, "plain write must NOT set FATTR_SIZE in valid")
}

// TestBaseDeltaReadMergeOverlaysBytes: base-delta write → ReadMerge correctly
// overlays the pending bytes over a base slice.
func (s *OverlaySuite) TestBaseDeltaReadMergeOverlaysBytes() {
	ov := s.newOverlay()
	ov.Apply(s.writeOp("basefile.txt", 5, []byte("XYZ")))

	base := []byte("0123456789")
	out := ov.ReadMerge("basefile.txt", 0, base)
	s.Equal([]byte("01234XYZ89"), out)
}

// TestBaseDeltaOnlyCreatedNodeIsNotBaseDelta: a path created by this overlay
// must never be a base-delta, even after writes or setattr.
func (s *OverlaySuite) TestBaseDeltaOnlyCreatedNodeIsNotBaseDelta() {
	ov := s.newOverlay()
	ov.Apply(s.createOp("created.txt", 0o100644))
	ov.Apply(s.writeOp("created.txt", 0, []byte("data")))
	ov.Apply(s.setAttrOp("created.txt", 0o100600))

	_, ok, _, baseDelta, _ := ov.Stat("created.txt")
	s.True(ok)
	s.False(baseDelta, "created file must never become a base-delta")
}

// ── xattr ─────────────────────────────────────────────────────────────────────

func (s *OverlaySuite) TestSetXAttrOnCreatedFile() {
	ov := s.newOverlay()
	ov.Apply(s.createOp("file.txt", 0o100644))
	ov.Apply(s.setXAttrOp("file.txt", "user.color", []byte("red"), 0))

	val, set, removed := ov.Xattr("file.txt", "user.color")
	s.True(set, "xattr must be set")
	s.False(removed)
	s.Equal([]byte("red"), val)
}

func (s *OverlaySuite) TestRemoveXAttrOnCreatedFile() {
	ov := s.newOverlay()
	ov.Apply(s.createOp("file.txt", 0o100644))
	ov.Apply(s.setXAttrOp("file.txt", "user.color", []byte("red"), 0))
	ov.Apply(s.removeXAttrOp("file.txt", "user.color"))

	val, set, removed := ov.Xattr("file.txt", "user.color")
	s.False(set, "removed xattr must not appear as set")
	s.True(removed, "xattr removal must be recorded")
	s.Nil(val)
}

func (s *OverlaySuite) TestRemoveXAttrOnBaseOnlyFile() {
	// RemoveXAttr on a base-only path: the overlay must record a tombstone so
	// the caller does NOT fall through to base.
	ov := s.newOverlay()
	ov.Apply(s.removeXAttrOp("basefile.txt", "user.tag"))

	val, set, removed := ov.Xattr("basefile.txt", "user.tag")
	s.False(set)
	s.True(removed, "xattr removal on base-only path must be recorded as tombstone")
	s.Nil(val)
}

func (s *OverlaySuite) TestSetXAttrAfterRemoveResurrects() {
	// SetXAttr after RemoveXAttr should clear the tombstone and set the value.
	ov := s.newOverlay()
	ov.Apply(s.createOp("file.txt", 0o100644))
	ov.Apply(s.removeXAttrOp("file.txt", "user.x"))
	ov.Apply(s.setXAttrOp("file.txt", "user.x", []byte("new"), 0))

	val, set, removed := ov.Xattr("file.txt", "user.x")
	s.True(set, "resurrected xattr must be set")
	s.False(removed)
	s.Equal([]byte("new"), val)
}

func (s *OverlaySuite) TestXattrAbsentReturnsNoPendingState() {
	ov := s.newOverlay()
	ov.Apply(s.createOp("file.txt", 0o100644))

	val, set, removed := ov.Xattr("file.txt", "user.nonexistent")
	s.False(set)
	s.False(removed)
	s.Nil(val)
}

func (s *OverlaySuite) TestXattrOnAbsentPathReturnsNoPendingState() {
	ov := s.newOverlay()
	val, set, removed := ov.Xattr("not-in-overlay.txt", "user.x")
	s.False(set)
	s.False(removed)
	s.Nil(val)
}

func (s *OverlaySuite) TestSetXAttrBaseDeltaCreatesEntry() {
	// SetXAttr on a base-only path creates a base-delta node.
	ov := s.newOverlay()
	ov.Apply(s.setXAttrOp("basefile.txt", "user.k", []byte("v"), 0))

	_, ok, _, baseDelta, _ := ov.Stat("basefile.txt")
	s.True(ok)
	s.True(baseDelta)

	val, set, removed := ov.Xattr("basefile.txt", "user.k")
	s.True(set)
	s.False(removed)
	s.Equal([]byte("v"), val)
}

// ── concurrency (race detector) ───────────────────────────────────────────────

func (s *OverlaySuite) TestConcurrentApplyAndRead() {
	ov := s.newOverlay()
	done := make(chan struct{})

	go func() {
		for i := 0; i < 200; i++ {
			ov.Apply(s.createOp("concurrent.txt", 0o100644))
			ov.Apply(s.writeOp("concurrent.txt", 0, []byte("x")))
		}
		close(done)
	}()

	for {
		select {
		case <-done:
			return
		default:
			ov.Stat("concurrent.txt")
			ov.Has("concurrent.txt")
			ov.Paths()
			ov.ReadMerge("concurrent.txt", 0, []byte("base"))
			ov.ListMerge("", nil)
		}
	}
}

// ── helpers ───────────────────────────────────────────────────────────────────

func entryNames(entries []backend.DirEntryPlus) []string {
	names := make([]string, 0, len(entries))
	for _, e := range entries {
		names = append(names, e.Name)
	}
	return names
}

func bytes10to20() []byte {
	b := make([]byte, 10)
	for i := range b {
		b[i] = 'P'
	}
	return b
}

// Ensure time is imported (used by future attrs).
var _ = time.Now
var _ = strings.HasPrefix
