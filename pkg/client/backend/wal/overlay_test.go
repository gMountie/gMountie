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
	return wal.Op{Kind: wal.OpSetAttr, Path: path, Mode: mode, Flags: backend.FATTR_MODE}
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

	attr, ok, tomb := ov.Stat("docs/readme.txt")
	s.True(ok, "Stat should find the pending file")
	s.False(tomb, "should not be tombstoned")
	s.NotNil(attr)
	s.Equal(uint32(0o100644), attr.Mode)
}

func (s *OverlaySuite) TestMkdirStatSeesPendingDir() {
	ov := s.newOverlay()
	ov.Apply(s.mkdirOp("newdir", 0o40755))

	attr, ok, tomb := ov.Stat("newdir")
	s.True(ok)
	s.False(tomb)
	s.NotNil(attr)
}

func (s *OverlaySuite) TestStatAbsentReturnsNotOk() {
	ov := s.newOverlay()
	_, ok, _ := ov.Stat("nonexistent")
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

	_, ok, tomb := ov.Stat("file.txt")
	s.True(ok, "Has should still see the entry (tombstone IS a pending state)")
	s.True(tomb, "should be tombstoned")
}

func (s *OverlaySuite) TestRmdirSetsTombstone() {
	ov := s.newOverlay()
	ov.Apply(s.mkdirOp("emptydir", 0o40755))
	ov.Apply(s.rmdirOp("emptydir"))

	_, ok, tomb := ov.Stat("emptydir")
	s.True(ok)
	s.True(tomb)
}

func (s *OverlaySuite) TestTombstoneResurrection() {
	// unlink then recreate → NOT tombstoned.
	ov := s.newOverlay()
	ov.Apply(s.createOp("file.txt", 0o100644))
	ov.Apply(s.unlinkOp("file.txt"))
	ov.Apply(s.createOp("file.txt", 0o100644))

	_, ok, tomb := ov.Stat("file.txt")
	s.True(ok)
	s.False(tomb, "recreated file must not be tombstoned")
}

func (s *OverlaySuite) TestTombstoneOnlyFromBaseUnlink() {
	// Unlink a path that was never in pending → still creates a tombstone.
	ov := s.newOverlay()
	ov.Apply(s.unlinkOp("base-only-file.txt"))

	_, ok, tomb := ov.Stat("base-only-file.txt")
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
	attr, ok, tomb := ov.Stat("new.txt")
	s.True(ok)
	s.False(tomb)
	s.NotNil(attr)

	// old path is tombstoned
	_, ok, tomb = ov.Stat("old.txt")
	s.True(ok, "old path has a tombstone, which is pending state")
	s.True(tomb, "old path should be tombstoned after rename")
}

func (s *OverlaySuite) TestRenameOfBaseOnlyPathTombstonesOld() {
	// Rename a path that only exists in base.
	ov := s.newOverlay()
	ov.Apply(s.renameOp("base/old.txt", "base/new.txt"))

	_, ok, tomb := ov.Stat("base/old.txt")
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
	_, ok, tomb := ov.Stat("newdir/file.txt")
	s.True(ok)
	s.False(tomb)

	// Old children should be tombstoned.
	_, ok, tomb = ov.Stat("dir/file.txt")
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
	ov.Apply(s.unlinkOp("c.txt"))     // tombstone only
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

	attr, ok, tomb := ov.Stat("file.txt")
	s.True(ok)
	s.False(tomb)
	s.Equal(uint32(0o100600), attr.Mode)
}

func (s *OverlaySuite) TestSetAttrOnBaseOnlyPathCreatesEntry() {
	// SetAttr on a path that only exists in base should record it.
	ov := s.newOverlay()
	ov.Apply(s.setAttrOp("basefile.txt", 0o100600))

	s.True(ov.Has("basefile.txt"))
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
