package cache

import (
	"context"
	"testing"
	"time"

	iomocks "gmountie/internal/mocks/pkg/client/io"
	"gmountie/pkg/client/io"

	"github.com/hanwen/go-fuse/v2/fuse"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"
)

// CachedBackendTestSuite covers the per-op invalidation contract from the
// Phase 4 Sub-spec B spec. One test per row of the table plus read-path
// hit/miss/negative coverage.
type CachedBackendTestSuite struct {
	suite.Suite
	inner *iomocks.MockFileSystemBackend
	b     *cachedBackend
}

func (s *CachedBackendTestSuite) SetupTest() {
	s.inner = iomocks.NewMockFileSystemBackend(s.T())
	cb := NewCachedBackend(s.inner, Config{
		MaxSizeBytes:   0, // no cap for these tests
		ChunkSizeBytes: 1024,
		AttrTTL:        5 * time.Second,
		DirTTL:         5 * time.Second,
		NegativeTTL:    2 * time.Second,
	}).(*cachedBackend)
	s.b = cb
}

// openCachedHandle is a tiny helper: registers an Open expectation on the
// inner mock returning a fresh MockFileHandle, then calls the cached
// backend's Open and returns both wrapper handle and inner mock handle.
func (s *CachedBackendTestSuite) openCachedHandle(path string) (io.FileHandle, *iomocks.MockFileHandle) {
	innerH := iomocks.NewMockFileHandle(s.T())
	innerH.EXPECT().Unwrap().Return(innerH).Maybe()
	s.inner.EXPECT().Open(mock.Anything, path, mock.Anything).Return(innerH, fuse.OK).Once()
	h, st := s.b.Open(context.Background(), path, 0)
	s.Require().Equal(fuse.OK, st)
	return h, innerH
}

// --- Read path ---

func (s *CachedBackendTestSuite) TestStatHitAfterMiss() {
	s.inner.EXPECT().Stat(mock.Anything, "/x").Return(&io.Attr{Ino: 1, Size: 10}, fuse.OK).Once()
	// Miss: hits inner.
	a, st := s.b.Stat(context.Background(), "/x")
	s.Require().Equal(fuse.OK, st)
	s.Assert().Equal(uint64(1), a.Ino)
	// Hit: inner NOT called (Once above proves this).
	a2, st2 := s.b.Stat(context.Background(), "/x")
	s.Require().Equal(fuse.OK, st2)
	s.Assert().Equal(uint64(1), a2.Ino)
}

func (s *CachedBackendTestSuite) TestStatCachesNegativeOnENOENT() {
	s.inner.EXPECT().Stat(mock.Anything, "/missing").Return(nil, fuse.ENOENT).Once()
	_, st := s.b.Stat(context.Background(), "/missing")
	s.Require().Equal(fuse.ENOENT, st)
	// Second call: cached negative, inner NOT called.
	_, st2 := s.b.Stat(context.Background(), "/missing")
	s.Assert().Equal(fuse.ENOENT, st2)
}

func (s *CachedBackendTestSuite) TestLookupCachesPositiveOnSuccess() {
	s.inner.EXPECT().Lookup(mock.Anything, "/d", "child").
		Return(&io.Attr{Ino: 7}, fuse.OK).Once()
	a, st := s.b.Lookup(context.Background(), "/d", "child")
	s.Require().Equal(fuse.OK, st)
	s.Assert().Equal(uint64(7), a.Ino)
	// Second call: hit, keyed on joined path. Inner NOT called.
	a2, st2 := s.b.Lookup(context.Background(), "/d", "child")
	s.Require().Equal(fuse.OK, st2)
	s.Assert().Equal(uint64(7), a2.Ino)
	// Verify cache keyed on joined path "/d/child".
	cached, hit, pos := s.b.attr.get("/d/child")
	s.Require().True(hit)
	s.Require().True(pos)
	s.Assert().Equal(uint64(7), cached.Ino)
}

func (s *CachedBackendTestSuite) TestLookupCachesNegativeOnENOENT() {
	s.inner.EXPECT().Lookup(mock.Anything, "/d", "nope").Return(nil, fuse.ENOENT).Once()
	_, st := s.b.Lookup(context.Background(), "/d", "nope")
	s.Require().Equal(fuse.ENOENT, st)
	// Cached negative.
	_, hit, pos := s.b.attr.get("/d/nope")
	s.Require().True(hit)
	s.Assert().False(pos)
	// Second call uses cache.
	_, st2 := s.b.Lookup(context.Background(), "/d", "nope")
	s.Assert().Equal(fuse.ENOENT, st2)
}

func (s *CachedBackendTestSuite) TestListDirHitAfterMiss() {
	entries := []io.DirEntry{{Name: "a"}, {Name: "b"}}
	s.inner.EXPECT().ListDir(mock.Anything, "/d").Return(entries, fuse.OK).Once()
	got, st := s.b.ListDir(context.Background(), "/d")
	s.Require().Equal(fuse.OK, st)
	s.Assert().Len(got, 2)
	// Second call: hit.
	got2, st2 := s.b.ListDir(context.Background(), "/d")
	s.Require().Equal(fuse.OK, st2)
	s.Assert().Len(got2, 2)
}

func (s *CachedBackendTestSuite) TestReadFromCacheChunk() {
	// Cache chunk 0 of /f: 1024 bytes.
	chunk := make([]byte, 1024)
	for i := range chunk {
		chunk[i] = byte(i % 251)
	}
	s.b.data.put("/f", 0, chunk)
	// No inner.EXPECT().Read — we expect a cache hit.
	h, _ := s.openCachedHandle("/f")

	dest := make([]byte, 100)
	n, st := s.b.Read(context.Background(), h, 0, dest)
	s.Require().Equal(fuse.OK, st)
	s.Assert().Equal(100, n)
	s.Assert().Equal(chunk[:100], dest)
}

func (s *CachedBackendTestSuite) TestReadFetchesAndCachesOnMiss() {
	h, innerH := s.openCachedHandle("/f")
	// Inner.Read called ONCE for chunk 0; returns 1024 bytes.
	s.inner.EXPECT().Read(mock.Anything, innerH, int64(0), mock.MatchedBy(func(b []byte) bool { return len(b) == 1024 })).
		RunAndReturn(func(_ context.Context, _ io.FileHandle, _ int64, buf []byte) (int, fuse.Status) {
			for i := range buf {
				buf[i] = byte(i % 251)
			}
			return 1024, fuse.OK
		}).Once()

	dest := make([]byte, 100)
	n, st := s.b.Read(context.Background(), h, 0, dest)
	s.Require().Equal(fuse.OK, st)
	s.Assert().Equal(100, n)

	// Second read of same range: cache hit, inner.Read NOT called again
	// (mock's Once would fail).
	dest2 := make([]byte, 100)
	n2, st2 := s.b.Read(context.Background(), h, 0, dest2)
	s.Require().Equal(fuse.OK, st2)
	s.Assert().Equal(100, n2)
	s.Assert().Equal(dest, dest2)
}

func (s *CachedBackendTestSuite) TestReadHandlesEOFMidStream() {
	// File is shorter than a chunk — inner returns 500 bytes for a 1024-byte
	// read request. The cached backend should propagate that as a short
	// read of 500 without padding or looping forever.
	h, innerH := s.openCachedHandle("/short")
	s.inner.EXPECT().Read(mock.Anything, innerH, int64(0), mock.Anything).
		RunAndReturn(func(_ context.Context, _ io.FileHandle, _ int64, buf []byte) (int, fuse.Status) {
			for i := 0; i < 500; i++ {
				buf[i] = byte(i)
			}
			return 500, fuse.OK
		}).Once()

	dest := make([]byte, 1024)
	n, st := s.b.Read(context.Background(), h, 0, dest)
	s.Require().Equal(fuse.OK, st)
	s.Assert().Equal(500, n)

	// Second read at offset 0 hits cache (no second inner.Read).
	dest2 := make([]byte, 1024)
	n2, st2 := s.b.Read(context.Background(), h, 0, dest2)
	s.Require().Equal(fuse.OK, st2)
	s.Assert().Equal(500, n2)
}

// --- Invalidation table ---

func (s *CachedBackendTestSuite) TestWriteInvalidatesDataAndAttr() {
	s.b.attr.putPositive("/f", &io.Attr{Ino: 1, Size: 100})
	s.b.data.put("/f", 0, []byte("OLD-CONTENT"))
	h, innerH := s.openCachedHandle("/f")
	s.inner.EXPECT().Write(mock.Anything, innerH, int64(0), mock.Anything).
		Return(uint32(4), fuse.OK).Once()
	_, st := s.b.Write(context.Background(), h, 0, []byte("NEW!"))
	s.Require().Equal(fuse.OK, st)
	// Cache invalidated:
	s.Assert().Nil(s.b.data.get("/f", 0))
	_, hit, _ := s.b.attr.get("/f")
	s.Assert().False(hit)
}

func (s *CachedBackendTestSuite) TestWriteOnlyInvalidatesOverlappingChunks() {
	// Pre-populate chunks 0, 1, 2. Write at chunk 1 boundary covering
	// only chunk 1. Chunks 0 and 2 must remain intact.
	s.b.data.put("/f", 0, make([]byte, 1024))
	s.b.data.put("/f", 1, make([]byte, 1024))
	s.b.data.put("/f", 2, make([]byte, 1024))
	h, innerH := s.openCachedHandle("/f")
	s.inner.EXPECT().Write(mock.Anything, innerH, int64(1024), mock.Anything).
		Return(uint32(100), fuse.OK).Once()
	_, st := s.b.Write(context.Background(), h, 1024, make([]byte, 100))
	s.Require().Equal(fuse.OK, st)
	s.Assert().NotNil(s.b.data.get("/f", 0))
	s.Assert().Nil(s.b.data.get("/f", 1))
	s.Assert().NotNil(s.b.data.get("/f", 2))
}

func (s *CachedBackendTestSuite) TestCreateInvalidatesParentDirAndDropsNegative() {
	// Prior negative-cached attr at the create path; cached parent listing.
	s.b.attr.putNegative("/d/new")
	s.b.attr.putPositive("/d", &io.Attr{Ino: 1})
	s.b.dir.put("/d", []io.DirEntry{{Name: "x"}})
	innerH := iomocks.NewMockFileHandle(s.T())
	innerH.EXPECT().Unwrap().Return(innerH).Maybe()
	s.inner.EXPECT().Create(mock.Anything, "/d", "new", mock.Anything, mock.Anything).
		Return(innerH, nil, fuse.OK).Once()
	h, a, st := s.b.Create(context.Background(), "/d", "new", 0, 0o644)
	s.Require().Equal(fuse.OK, st)
	s.Assert().Nil(a)
	s.Assert().NotNil(h)
	// Parent dir + parent attr invalidated.
	_, dirHit := s.b.dir.get("/d")
	s.Assert().False(dirHit)
	_, parentHit, _ := s.b.attr.get("/d")
	s.Assert().False(parentHit)
	// Negative attr for the new path dropped.
	_, hit, _ := s.b.attr.get("/d/new")
	s.Assert().False(hit)
}

func (s *CachedBackendTestSuite) TestCreatePopulatesPositiveAttrIfBackendReturns() {
	innerH := iomocks.NewMockFileHandle(s.T())
	innerH.EXPECT().Unwrap().Return(innerH).Maybe()
	returnedAttr := &io.Attr{Ino: 42, Size: 0, Mode: 0o100644}
	s.inner.EXPECT().Create(mock.Anything, "/d", "new", mock.Anything, mock.Anything).
		Return(innerH, returnedAttr, fuse.OK).Once()
	_, a, st := s.b.Create(context.Background(), "/d", "new", 0, 0o644)
	s.Require().Equal(fuse.OK, st)
	s.Assert().Equal(uint64(42), a.Ino)
	// Positive attr populated for joined path.
	cached, hit, pos := s.b.attr.get("/d/new")
	s.Require().True(hit)
	s.Require().True(pos)
	s.Assert().Equal(uint64(42), cached.Ino)
}

func (s *CachedBackendTestSuite) TestMkdirInvalidatesParentAndDropsNegative() {
	s.b.attr.putNegative("/d")
	s.b.attr.putPositive("", &io.Attr{Ino: 1}) // parent attr cached
	s.b.dir.put("", []io.DirEntry{})
	s.inner.EXPECT().Mkdir(mock.Anything, "/d", mock.Anything).Return(fuse.OK).Once()
	st := s.b.Mkdir(context.Background(), "/d", 0o755)
	s.Require().Equal(fuse.OK, st)
	_, hit, _ := s.b.attr.get("/d")
	s.Assert().False(hit) // negative dropped
	_, parentHit, _ := s.b.attr.get("")
	s.Assert().False(parentHit) // parent attr invalidated
	_, dirHit := s.b.dir.get("")
	s.Assert().False(dirHit) // parent dir invalidated
}

func (s *CachedBackendTestSuite) TestRmdirInvalidatesAndNegativesPath() {
	s.b.attr.putPositive("/d", &io.Attr{Ino: 1})
	s.b.dir.put("/d", []io.DirEntry{})
	s.b.dir.put("", []io.DirEntry{{Name: "d"}})
	s.inner.EXPECT().Rmdir(mock.Anything, "/d").Return(fuse.OK).Once()
	st := s.b.Rmdir(context.Background(), "/d")
	s.Require().Equal(fuse.OK, st)
	// Negative-cached.
	_, hit, pos := s.b.attr.get("/d")
	s.Require().True(hit)
	s.Assert().False(pos)
	// Own dir listing dropped.
	_, dirHit := s.b.dir.get("/d")
	s.Assert().False(dirHit)
	// Parent listing dropped.
	_, parentDirHit := s.b.dir.get("")
	s.Assert().False(parentDirHit)
}

func (s *CachedBackendTestSuite) TestUnlinkInvalidatesAndNegativesPath() {
	s.b.attr.putPositive("/f", &io.Attr{Ino: 1})
	s.b.data.put("/f", 0, []byte("c"))
	s.b.dir.put("", []io.DirEntry{{Name: "f"}})
	s.inner.EXPECT().Unlink(mock.Anything, "/f").Return(fuse.OK).Once()
	st := s.b.Unlink(context.Background(), "/f")
	s.Require().Equal(fuse.OK, st)
	// Negative attr cached for /f.
	_, hit, pos := s.b.attr.get("/f")
	s.Require().True(hit)
	s.Assert().False(pos)
	// Data dropped.
	s.Assert().Nil(s.b.data.get("/f", 0))
	// Parent listing invalidated.
	_, dirHit := s.b.dir.get("")
	s.Assert().False(dirHit)
}

func (s *CachedBackendTestSuite) TestRenameInvalidatesBothPaths() {
	s.b.attr.putPositive("/a", &io.Attr{Ino: 1})
	s.b.attr.putPositive("/b", &io.Attr{Ino: 2})
	s.b.data.put("/a", 0, []byte("aa"))
	s.b.data.put("/b", 0, []byte("bb"))
	s.b.dir.put("", []io.DirEntry{})
	s.inner.EXPECT().Rename(mock.Anything, "/a", "/b").Return(fuse.OK).Once()
	st := s.b.Rename(context.Background(), "/a", "/b")
	s.Require().Equal(fuse.OK, st)
	// /a now negative-cached.
	_, hitA, posA := s.b.attr.get("/a")
	s.Require().True(hitA)
	s.Assert().False(posA)
	// /b's prior cached attr cleared.
	_, hitB, _ := s.b.attr.get("/b")
	s.Assert().False(hitB)
	// Data on both dropped.
	s.Assert().Nil(s.b.data.get("/a", 0))
	s.Assert().Nil(s.b.data.get("/b", 0))
	// Parent dir invalidated.
	_, dirHit := s.b.dir.get("")
	s.Assert().False(dirHit)
}

func (s *CachedBackendTestSuite) TestTruncateInvalidatesAllChunks() {
	s.b.data.put("/f", 0, make([]byte, 1024))
	s.b.data.put("/f", 1, make([]byte, 1024))
	s.b.attr.putPositive("/f", &io.Attr{Size: 2048})
	s.inner.EXPECT().Truncate(mock.Anything, "/f", uint64(100)).Return(fuse.OK).Once()
	st := s.b.Truncate(context.Background(), "/f", 100)
	s.Require().Equal(fuse.OK, st)
	s.Assert().Nil(s.b.data.get("/f", 0))
	s.Assert().Nil(s.b.data.get("/f", 1))
	_, hit, _ := s.b.attr.get("/f")
	s.Assert().False(hit)
}

func (s *CachedBackendTestSuite) TestChmodInvalidatesAttrOnly() {
	s.b.attr.putPositive("/f", &io.Attr{Mode: 0o644})
	s.b.data.put("/f", 0, []byte("DATA"))
	s.inner.EXPECT().Chmod(mock.Anything, "/f", uint32(0o600)).Return(fuse.OK).Once()
	st := s.b.Chmod(context.Background(), "/f", 0o600)
	s.Require().Equal(fuse.OK, st)
	_, hit, _ := s.b.attr.get("/f")
	s.Assert().False(hit)
	s.Assert().NotNil(s.b.data.get("/f", 0)) // data untouched
}

func (s *CachedBackendTestSuite) TestChownInvalidatesAttrOnly() {
	s.b.attr.putPositive("/f", &io.Attr{Uid: 0, Gid: 0})
	s.b.data.put("/f", 0, []byte("DATA"))
	s.inner.EXPECT().Chown(mock.Anything, "/f", uint32(1000), uint32(1000)).
		Return(fuse.OK).Once()
	st := s.b.Chown(context.Background(), "/f", 1000, 1000)
	s.Require().Equal(fuse.OK, st)
	_, hit, _ := s.b.attr.get("/f")
	s.Assert().False(hit)
	s.Assert().NotNil(s.b.data.get("/f", 0))
}

func (s *CachedBackendTestSuite) TestAllocateInvalidatesDataRangeAndAttr() {
	// Pre-populate 3 chunks. Allocate covers only chunks 0 and 1.
	s.b.data.put("/f", 0, make([]byte, 1024))
	s.b.data.put("/f", 1, make([]byte, 1024))
	s.b.data.put("/f", 2, make([]byte, 1024))
	s.b.attr.putPositive("/f", &io.Attr{Size: 3072})
	h, innerH := s.openCachedHandle("/f")
	s.inner.EXPECT().Allocate(mock.Anything, innerH, uint64(0), uint64(1500), mock.Anything).
		Return(fuse.OK).Once()
	st := s.b.Allocate(context.Background(), h, 0, 1500, 0)
	s.Require().Equal(fuse.OK, st)
	// Chunks 0 and 1 invalidated; chunk 2 untouched.
	s.Assert().Nil(s.b.data.get("/f", 0))
	s.Assert().Nil(s.b.data.get("/f", 1))
	s.Assert().NotNil(s.b.data.get("/f", 2))
	// Attr invalidated.
	_, hit, _ := s.b.attr.get("/f")
	s.Assert().False(hit)
}

// --- Pass-through ops (no cache mutations) ---

func (s *CachedBackendTestSuite) TestReleaseDoesNotInvalidate() {
	s.b.attr.putPositive("/f", &io.Attr{Ino: 1})
	s.b.data.put("/f", 0, []byte("DATA"))
	h, innerH := s.openCachedHandle("/f")
	s.inner.EXPECT().Release(mock.Anything, innerH).Return(fuse.OK).Once()
	st := s.b.Release(context.Background(), h)
	s.Require().Equal(fuse.OK, st)
	// Cache untouched.
	_, hit, pos := s.b.attr.get("/f")
	s.Require().True(hit)
	s.Assert().True(pos)
	s.Assert().NotNil(s.b.data.get("/f", 0))
}

func (s *CachedBackendTestSuite) TestFlushDoesNotInvalidate() {
	s.b.attr.putPositive("/f", &io.Attr{Ino: 1})
	s.b.data.put("/f", 0, []byte("DATA"))
	h, innerH := s.openCachedHandle("/f")
	s.inner.EXPECT().Flush(mock.Anything, innerH).Return(fuse.OK).Once()
	st := s.b.Flush(context.Background(), h)
	s.Require().Equal(fuse.OK, st)
	_, hit, _ := s.b.attr.get("/f")
	s.Assert().True(hit)
	s.Assert().NotNil(s.b.data.get("/f", 0))
}

func (s *CachedBackendTestSuite) TestFsyncDoesNotInvalidate() {
	s.b.attr.putPositive("/f", &io.Attr{Ino: 1})
	s.b.data.put("/f", 0, []byte("DATA"))
	h, innerH := s.openCachedHandle("/f")
	s.inner.EXPECT().Fsync(mock.Anything, innerH, int64(0)).Return(fuse.OK).Once()
	st := s.b.Fsync(context.Background(), h, 0)
	s.Require().Equal(fuse.OK, st)
	_, hit, _ := s.b.attr.get("/f")
	s.Assert().True(hit)
	s.Assert().NotNil(s.b.data.get("/f", 0))
}

func (s *CachedBackendTestSuite) TestLockOpsPassthroughNoCacheMutations() {
	s.b.attr.putPositive("/f", &io.Attr{Ino: 1})
	s.b.data.put("/f", 0, []byte("DATA"))
	h, innerH := s.openCachedHandle("/f")
	lk := &fuse.FileLock{}
	out := &fuse.FileLock{}
	// All three lock ops should pass through with the unwrapped handle.
	s.inner.EXPECT().GetLk(mock.Anything, innerH, uint64(1), lk, uint32(0), out).
		Return(fuse.OK).Once()
	s.inner.EXPECT().SetLk(mock.Anything, innerH, uint64(1), lk, uint32(0)).
		Return(fuse.OK).Once()
	s.inner.EXPECT().SetLkw(mock.Anything, innerH, uint64(1), lk, uint32(0)).
		Return(fuse.OK).Once()
	s.Require().Equal(fuse.OK, s.b.GetLk(context.Background(), h, 1, lk, 0, out))
	s.Require().Equal(fuse.OK, s.b.SetLk(context.Background(), h, 1, lk, 0))
	s.Require().Equal(fuse.OK, s.b.SetLkw(context.Background(), h, 1, lk, 0))
	// Cache untouched.
	_, hit, _ := s.b.attr.get("/f")
	s.Assert().True(hit)
	s.Assert().NotNil(s.b.data.get("/f", 0))
}

// --- Failure-path no-invalidation guards ---

func (s *CachedBackendTestSuite) TestMutationFailureDoesNotInvalidate() {
	// If the inner backend rejects the mutation we must NOT invalidate
	// the cache — invalidation is conditional on inner success.
	s.b.attr.putPositive("/f", &io.Attr{Ino: 1})
	s.inner.EXPECT().Chmod(mock.Anything, "/f", uint32(0o600)).Return(fuse.EACCES).Once()
	st := s.b.Chmod(context.Background(), "/f", 0o600)
	s.Require().Equal(fuse.EACCES, st)
	// Cache still has the old attr.
	_, hit, pos := s.b.attr.get("/f")
	s.Require().True(hit)
	s.Assert().True(pos)
}

func TestCachedBackendTestSuite(t *testing.T) {
	suite.Run(t, new(CachedBackendTestSuite))
}
