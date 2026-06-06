package io

import (
	"math"
	"os"
	"path/filepath"
	"syscall"
	"testing"

	"github.com/hanwen/go-fuse/v2/fuse"
	"github.com/hanwen/go-fuse/v2/fuse/nodefs"
	"github.com/stretchr/testify/suite"
	"golang.org/x/sys/unix"
)

type CopyRangeSuite struct {
	suite.Suite
	dir string
}

func TestCopyRangeSuite(t *testing.T) { suite.Run(t, new(CopyRangeSuite)) }

func (s *CopyRangeSuite) SetupTest() { s.dir = s.T().TempDir() }

// rawFile creates path with content and opens it read-write as a RawFdFile.
func (s *CopyRangeSuite) rawFile(name string, content []byte) *RawFdFile {
	p := filepath.Join(s.dir, name)
	s.Require().NoError(os.WriteFile(p, content, 0o644))
	f, err := os.OpenFile(p, os.O_RDWR, 0)
	s.Require().NoError(err)
	rf := NewRawFdFile(f)
	s.T().Cleanup(func() { rf.Release() })
	return rf
}

func (s *CopyRangeSuite) readBack(name string) []byte {
	b, err := os.ReadFile(filepath.Join(s.dir, name))
	s.Require().NoError(err)
	return b
}

func (s *CopyRangeSuite) TestCopyWholeFile() {
	src := s.rawFile("src", []byte("hello copy_file_range"))
	dst := s.rawFile("dst", nil)
	n, st := CopyFileRange(src, dst, 0, 0, 21)
	s.Equal(fuse.OK, st)
	s.Equal(uint64(21), n)
	s.Equal([]byte("hello copy_file_range"), s.readBack("dst"))
}

func (s *CopyRangeSuite) TestCopyAtOffsets() {
	src := s.rawFile("src", []byte("0123456789"))
	dst := s.rawFile("dst", []byte("XXXXXXXXXX"))
	n, st := CopyFileRange(src, dst, 2, 5, 3) // "234" -> dst@5
	s.Equal(fuse.OK, st)
	s.Equal(uint64(3), n)
	s.Equal([]byte("XXXXX234XX"), s.readBack("dst"))
}

func (s *CopyRangeSuite) TestCopyShortAtEOF() {
	src := s.rawFile("src", []byte("short"))
	dst := s.rawFile("dst", nil)
	n, st := CopyFileRange(src, dst, 0, 0, 4096)
	s.Equal(fuse.OK, st)
	s.Equal(uint64(5), n)
}

func (s *CopyRangeSuite) TestCopyZeroLength() {
	src := s.rawFile("src", []byte("abc"))
	dst := s.rawFile("dst", nil)
	n, st := CopyFileRange(src, dst, 0, 0, 0)
	s.Equal(fuse.OK, st)
	s.Equal(uint64(0), n)
}

// Overlapping ranges within one file must surface EINVAL (kernel contract),
// not silently "succeed" via the fallback loop.
func (s *CopyRangeSuite) TestCopyOverlapSameFile_EINVAL() {
	p := filepath.Join(s.dir, "same")
	s.Require().NoError(os.WriteFile(p, make([]byte, 8192), 0o644))
	f1, err := os.OpenFile(p, os.O_RDWR, 0)
	s.Require().NoError(err)
	f2, err := os.OpenFile(p, os.O_RDWR, 0)
	s.Require().NoError(err)
	src, dst := NewRawFdFile(f1), NewRawFdFile(f2)
	s.T().Cleanup(func() { src.Release(); dst.Release() })

	_, st := CopyFileRange(src, dst, 0, 1024, 4096) // [0,4096) vs [1024,5120) overlap
	s.Equal(fuse.Status(syscall.EINVAL), st)
}

// The generic loop must produce identical results to the fd path and apply
// its own overlap check (it can't rely on the kernel's).
func (s *CopyRangeSuite) TestCopyGenericFallback() {
	src := s.rawFile("gsrc", []byte("generic fallback data"))
	dst := s.rawFile("gdst", nil)
	n, st := copyRangeGeneric(src, dst, 8, 0, 8) // "fallback"
	s.Equal(fuse.OK, st)
	s.Equal(uint64(8), n)
	s.Equal([]byte("fallback"), s.readBack("gdst"))
}

func (s *CopyRangeSuite) TestCopyGenericOverlap_EINVAL() {
	p := filepath.Join(s.dir, "gsame")
	s.Require().NoError(os.WriteFile(p, make([]byte, 8192), 0o644))
	f1, err := os.OpenFile(p, os.O_RDWR, 0)
	s.Require().NoError(err)
	f2, err := os.OpenFile(p, os.O_RDWR, 0)
	s.Require().NoError(err)
	src, dst := NewRawFdFile(f1), NewRawFdFile(f2)
	s.T().Cleanup(func() { src.Release(); dst.Release() })

	_, st := copyRangeGeneric(src, dst, 0, 1024, 4096)
	s.Equal(fuse.Status(syscall.EINVAL), st)
}

func (s *CopyRangeSuite) TestLseekDataAndHole() {
	f := s.rawFile("lseek", []byte("0123456789"))
	off, st := Lseek(f, 0, unix.SEEK_DATA)
	s.Equal(fuse.OK, st)
	s.Equal(uint64(0), off)
	off, st = Lseek(f, 0, unix.SEEK_HOLE)
	s.Equal(fuse.OK, st)
	s.GreaterOrEqual(off, uint64(10)) // implicit hole at EOF
}

func (s *CopyRangeSuite) TestLseekPastEOF_ENXIO() {
	f := s.rawFile("lseek2", []byte("abc"))
	_, st := Lseek(f, 100, unix.SEEK_DATA)
	s.Equal(fuse.Status(syscall.ENXIO), st)
}

func (s *CopyRangeSuite) TestLseekNonRawFile_ENOTSUP() {
	_, st := Lseek(nodefs.NewDefaultFile(), 0, unix.SEEK_DATA)
	s.Equal(fuse.ENOTSUP, st)
}

// TestLseekSparseHole exercises real hole geometry: punch a hole in the
// middle of a file and verify SEEK_DATA/SEEK_HOLE land on its edges.
func (s *CopyRangeSuite) TestLseekSparseHole() {
	p := filepath.Join(s.dir, "sparse")
	s.Require().NoError(os.WriteFile(p, make([]byte, 65536), 0o644))
	fd, err := unix.Open(p, unix.O_RDWR, 0)
	s.Require().NoError(err)
	if err := unix.Fallocate(fd, unix.FALLOC_FL_PUNCH_HOLE|unix.FALLOC_FL_KEEP_SIZE, 4096, 4096); err != nil {
		unix.Close(fd)
		s.T().Skipf("filesystem doesn't support hole-punching: %v", err)
	}
	f := NewRawFdFile(os.NewFile(uintptr(fd), "sparse"))
	s.T().Cleanup(func() { f.Release() })

	off, st := Lseek(f, 0, unix.SEEK_DATA)
	s.Equal(fuse.OK, st)
	s.Equal(uint64(0), off)

	off, st = Lseek(f, 0, unix.SEEK_HOLE)
	s.Equal(fuse.OK, st)
	s.Equal(uint64(4096), off)

	off, st = Lseek(f, 4096, unix.SEEK_DATA)
	s.Equal(fuse.OK, st)
	s.Equal(uint64(8192), off)
}

func (s *CopyRangeSuite) TestRangesOverlapSaturates() {
	s.True(rangesOverlap(math.MaxUint64-1, 0, 4)) // would wrap without the guard
	s.True(rangesOverlap(0, math.MaxUint64-1, 4))
	s.False(rangesOverlap(0, 8192, 4096)) // disjoint stays false
}

// A non-RawFdFile on either side must route CopyFileRange through the
// generic loop — guard against the type assertion silently taking the
// wrong branch.
func (s *CopyRangeSuite) TestCopyFallbackViaPublicEntry() {
	src := s.rawFile("psrc", []byte("public entry fallback"))
	dst := s.rawFile("pdst", nil)
	// Wrap src so it is not a *RawFdFile; reads still work via the inner file.
	wrapped := nodefs.NewReadOnlyFile(src)
	n, st := CopyFileRange(wrapped, dst, 0, 0, 21)
	s.Equal(fuse.OK, st)
	s.Equal(uint64(21), n)
	s.Equal([]byte("public entry fallback"), s.readBack("pdst"))
}

// Confined Open/Create must hand back fd-backed files so the controller's
// copy/lseek paths get the fast path.
func (s *CopyRangeSuite) TestConfinedOpenReturnsRawFdFile() {
	s.Require().NoError(os.WriteFile(filepath.Join(s.dir, "x"), []byte("x"), 0o644))
	cfs, err := NewConfinedLoopbackFileSystem(s.dir)
	s.Require().NoError(err)
	s.T().Cleanup(func() { unix.Close(cfs.rootFd) })

	f, st := cfs.Open("x", uint32(os.O_RDONLY), nil)
	s.Require().Equal(fuse.OK, st)
	s.T().Cleanup(func() { f.Release() })
	_, ok := f.(*RawFdFile)
	s.True(ok, "confined Open should return *RawFdFile")

	g, st := cfs.Create("y", uint32(os.O_RDWR), 0o644, nil)
	s.Require().Equal(fuse.OK, st)
	s.T().Cleanup(func() { g.Release() })
	_, ok = g.(*RawFdFile)
	s.True(ok, "confined Create should return *RawFdFile")
}
