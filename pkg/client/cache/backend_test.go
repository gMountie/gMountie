package cache

import (
	"context"
	"testing"
	"time"

	iomocks "go.gmountie.dev/gmountie/internal/mocks/pkg/client/io"
	"go.gmountie.dev/gmountie/pkg/client/io"
	"go.gmountie.dev/gmountie/pkg/proto"

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
		MemoryMaxBytes: 0, // no cap for these tests
		ChunkSizeBytes: 1024,
		AttrTTL:        5 * time.Second,
		DirTTL:         5 * time.Second,
		NegativeTTL:    2 * time.Second,
	}, nil, nil, "").(*cachedBackend)
	// Mark the tracker as globally verified so the existing invalidation-table
	// tests exercise pure cache hit/miss semantics without triggering the
	// validity-gating path. Tests that specifically target gating
	// (TestUnverified*) construct their own backend without this flip.
	cb.validity.markGlobalVerified()
	s.b = cb
}

// openCachedHandle is a tiny helper: registers an Open expectation on the
// inner mock returning a fresh MockFileHandle, then calls the cached
// backend's Open and returns both wrapper handle and inner mock handle.
func (s *CachedBackendTestSuite) openCachedHandle(path string) (io.FileHandle, *iomocks.MockFileHandle) {
	innerH := iomocks.NewMockFileHandle(s.T())
	innerH.EXPECT().Unwrap().Return(innerH).Maybe()
	s.inner.EXPECT().Open(mock.Anything, path, mock.Anything).Return(innerH, proto.FsError_FS_OK).Once()
	h, st := s.b.Open(context.Background(), path, 0)
	s.Require().Equal(proto.FsError_FS_OK, st)
	return h, innerH
}

// --- Read path ---

func (s *CachedBackendTestSuite) TestStatHitAfterMiss() {
	s.inner.EXPECT().Stat(mock.Anything, "/x").Return(&io.Attr{Ino: 1, Size: 10}, proto.FsError_FS_OK).Once()
	// Miss: hits inner.
	a, st := s.b.Stat(context.Background(), "/x")
	s.Require().Equal(proto.FsError_FS_OK, st)
	s.Assert().Equal(uint64(1), a.Ino)
	// Hit: inner NOT called (Once above proves this).
	a2, st2 := s.b.Stat(context.Background(), "/x")
	s.Require().Equal(proto.FsError_FS_OK, st2)
	s.Assert().Equal(uint64(1), a2.Ino)
}

func (s *CachedBackendTestSuite) TestStatCachesNegativeOnENOENT() {
	s.inner.EXPECT().Stat(mock.Anything, "/missing").Return(nil, proto.FsError_FS_ENOENT).Once()
	_, st := s.b.Stat(context.Background(), "/missing")
	s.Require().Equal(proto.FsError_FS_ENOENT, st)
	// Second call: cached negative, inner NOT called.
	_, st2 := s.b.Stat(context.Background(), "/missing")
	s.Assert().Equal(proto.FsError_FS_ENOENT, st2)
}

func (s *CachedBackendTestSuite) TestLookupCachesPositiveOnSuccess() {
	s.inner.EXPECT().Lookup(mock.Anything, "/d", "child").
		Return(&io.Attr{Ino: 7}, proto.FsError_FS_OK).Once()
	a, st := s.b.Lookup(context.Background(), "/d", "child")
	s.Require().Equal(proto.FsError_FS_OK, st)
	s.Assert().Equal(uint64(7), a.Ino)
	// Second call: hit, keyed on joined path. Inner NOT called.
	a2, st2 := s.b.Lookup(context.Background(), "/d", "child")
	s.Require().Equal(proto.FsError_FS_OK, st2)
	s.Assert().Equal(uint64(7), a2.Ino)
	// Verify cache keyed on joined path "/d/child".
	cached, hit, pos := s.b.attr.get("/d/child")
	s.Require().True(hit)
	s.Require().True(pos)
	s.Assert().Equal(uint64(7), cached.Ino)
}

func (s *CachedBackendTestSuite) TestLookupCachesNegativeOnENOENT() {
	s.inner.EXPECT().Lookup(mock.Anything, "/d", "nope").Return(nil, proto.FsError_FS_ENOENT).Once()
	_, st := s.b.Lookup(context.Background(), "/d", "nope")
	s.Require().Equal(proto.FsError_FS_ENOENT, st)
	// Cached negative.
	_, hit, pos := s.b.attr.get("/d/nope")
	s.Require().True(hit)
	s.Assert().False(pos)
	// Second call uses cache.
	_, st2 := s.b.Lookup(context.Background(), "/d", "nope")
	s.Assert().Equal(proto.FsError_FS_ENOENT, st2)
}

func (s *CachedBackendTestSuite) TestListDirHitAfterMiss() {
	entries := []io.DirEntryPlus{
		{DirEntry: io.DirEntry{Name: "a"}},
		{DirEntry: io.DirEntry{Name: "b"}},
	}
	s.inner.EXPECT().ListDir(mock.Anything, "/d").Return(entries, proto.FsError_FS_OK).Once()
	got, st := s.b.ListDir(context.Background(), "/d")
	s.Require().Equal(proto.FsError_FS_OK, st)
	s.Assert().Len(got, 2)
	// Second call: hit.
	got2, st2 := s.b.ListDir(context.Background(), "/d")
	s.Require().Equal(proto.FsError_FS_OK, st2)
	s.Assert().Len(got2, 2)
	s.Assert().Equal("a", got2[0].Name)
}

// TestListDirPlusPrimesAttrCache is THE priming property of plus listings:
// after a ListDir whose entries carry attrs, a Stat on a listed child is
// served from the attr cache with ZERO backend calls — that's what makes the
// kernel's READDIRPLUS-driven per-child lookups free. The strict mock has
// only the single .Once() ListDir expectation, so any inner Stat would fail
// the test (absence proof).
func (s *CachedBackendTestSuite) TestListDirPlusPrimesAttrCache() {
	entries := []io.DirEntryPlus{
		{
			DirEntry: io.DirEntry{Name: "child", Ino: 7, Mode: 0o100644},
			Attr:     &io.Attr{Ino: 7, Size: 42, Mode: 0o100644},
		},
	}
	s.inner.EXPECT().ListDir(mock.Anything, "/d").Return(entries, proto.FsError_FS_OK).Once()
	_, st := s.b.ListDir(context.Background(), "/d")
	s.Require().Equal(proto.FsError_FS_OK, st)

	a, st2 := s.b.Stat(context.Background(), "/d/child")
	s.Require().Equal(proto.FsError_FS_OK, st2)
	s.Require().NotNil(a)
	s.Assert().Equal(uint64(7), a.Ino)
	s.Assert().Equal(uint64(42), a.Size)

	// Lookup is keyed on the same joined path — also a zero-RPC hit.
	a2, st3 := s.b.Lookup(context.Background(), "/d", "child")
	s.Require().Equal(proto.FsError_FS_OK, st3)
	s.Assert().Equal(uint64(7), a2.Ino)
}

// TestListDirNilAttrEntriesDoNotPrime: an entry without attrs (plus
// disabled, or the per-entry stat failed server-side) must NOT prime the
// attr cache — a later Stat goes to the backend.
func (s *CachedBackendTestSuite) TestListDirNilAttrEntriesDoNotPrime() {
	entries := []io.DirEntryPlus{{DirEntry: io.DirEntry{Name: "noattr", Ino: 8}}}
	s.inner.EXPECT().ListDir(mock.Anything, "/d").Return(entries, proto.FsError_FS_OK).Once()
	_, st := s.b.ListDir(context.Background(), "/d")
	s.Require().Equal(proto.FsError_FS_OK, st)

	s.inner.EXPECT().Stat(mock.Anything, "/d/noattr").
		Return(&io.Attr{Ino: 8}, proto.FsError_FS_OK).Once()
	a, st2 := s.b.Stat(context.Background(), "/d/noattr")
	s.Require().Equal(proto.FsError_FS_OK, st2)
	s.Assert().Equal(uint64(8), a.Ino)
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
	s.Require().Equal(proto.FsError_FS_OK, st)
	s.Assert().Equal(100, n)
	s.Assert().Equal(chunk[:100], dest)
}

func (s *CachedBackendTestSuite) TestReadFetchesAndCachesOnMiss() {
	h, innerH := s.openCachedHandle("/f")
	// Inner.Read called ONCE for chunk 0; returns 1024 bytes.
	s.inner.EXPECT().Read(mock.Anything, innerH, int64(0), mock.MatchedBy(func(b []byte) bool { return len(b) == 1024 })).
		RunAndReturn(func(_ context.Context, _ io.FileHandle, _ int64, buf []byte) (int, proto.FsError) {
			for i := range buf {
				buf[i] = byte(i % 251)
			}
			return 1024, proto.FsError_FS_OK
		}).Once()

	dest := make([]byte, 100)
	n, st := s.b.Read(context.Background(), h, 0, dest)
	s.Require().Equal(proto.FsError_FS_OK, st)
	s.Assert().Equal(100, n)

	// Second read of same range: cache hit, inner.Read NOT called again
	// (mock's Once would fail).
	dest2 := make([]byte, 100)
	n2, st2 := s.b.Read(context.Background(), h, 0, dest2)
	s.Require().Equal(proto.FsError_FS_OK, st2)
	s.Assert().Equal(100, n2)
	s.Assert().Equal(dest, dest2)
}

// fillReturn fills buf with deterministic bytes and reports a full read.
func fillReturn(_ context.Context, _ io.FileHandle, _ int64, buf []byte) (int, proto.FsError) {
	for i := range buf {
		buf[i] = byte(i % 251)
	}
	return len(buf), proto.FsError_FS_OK
}

// TestSequentialReadOverReadsSpan is the WAN readahead-defeat fix: once a read
// stream is detected sequential (after prefetchSeqThreshold), a miss fetches a
// whole span of chunks in ONE inner Read RPC, and the upcoming sequential reads
// are served from cache (no further inner.Read). The .Once() expectations fail
// if the cache instead issues a per-chunk RPC.
func (s *CachedBackendTestSuite) TestSequentialReadOverReadsSpan() {
	h, innerH := s.openCachedHandle("/seq")
	const cs = 1024
	// Reads below the threshold: single-chunk fetches.
	for i := 0; i < prefetchSeqThreshold-1; i++ {
		s.inner.EXPECT().Read(mock.Anything, innerH, int64(i*cs),
			mock.MatchedBy(func(b []byte) bool { return len(b) == cs })).
			RunAndReturn(fillReturn).Once()
	}
	// The threshold-hitting read over-reads exactly one span in one RPC.
	thIdx := prefetchSeqThreshold - 1
	span := prefetchSpanChunks
	s.inner.EXPECT().Read(mock.Anything, innerH, int64(thIdx*cs),
		mock.MatchedBy(func(b []byte) bool { return len(b) == span*cs })).
		RunAndReturn(fillReturn).Once()

	// Drive a sequential stream over the whole over-read span. Only the calls
	// expected above may reach inner; the span-covered chunks must hit cache.
	for i := 0; i < thIdx+span; i++ {
		dest := make([]byte, cs)
		n, st := s.b.Read(context.Background(), h, int64(i*cs), dest)
		s.Require().Equal(proto.FsError_FS_OK, st)
		s.Require().Equal(cs, n)
	}
}

// TestRandomReadDoesNotOverRead guards the gating: non-sequential reads must NOT
// over-read (else random access amplifies bandwidth W×). Each miss fetches one
// chunk; MatchedBy(len==cs) fails if a span read is issued.
func (s *CachedBackendTestSuite) TestRandomReadDoesNotOverRead() {
	h, innerH := s.openCachedHandle("/rand")
	const cs = 1024
	for _, ci := range []int{0, 5, 2, 9} {
		s.inner.EXPECT().Read(mock.Anything, innerH, int64(ci*cs),
			mock.MatchedBy(func(b []byte) bool { return len(b) == cs })).
			RunAndReturn(fillReturn).Once()
	}
	for _, ci := range []int{0, 5, 2, 9} {
		dest := make([]byte, cs)
		n, st := s.b.Read(context.Background(), h, int64(ci*cs), dest)
		s.Require().Equal(proto.FsError_FS_OK, st)
		s.Require().Equal(cs, n)
	}
}

func (s *CachedBackendTestSuite) TestReadHandlesEOFMidStream() {
	// File is shorter than a chunk — inner returns 500 bytes for a 1024-byte
	// read request. The cached backend should propagate that as a short
	// read of 500 without padding or looping forever.
	h, innerH := s.openCachedHandle("/short")
	s.inner.EXPECT().Read(mock.Anything, innerH, int64(0), mock.Anything).
		RunAndReturn(func(_ context.Context, _ io.FileHandle, _ int64, buf []byte) (int, proto.FsError) {
			for i := 0; i < 500; i++ {
				buf[i] = byte(i)
			}
			return 500, proto.FsError_FS_OK
		}).Once()

	dest := make([]byte, 1024)
	n, st := s.b.Read(context.Background(), h, 0, dest)
	s.Require().Equal(proto.FsError_FS_OK, st)
	s.Assert().Equal(500, n)

	// Second read at offset 0 hits cache (no second inner.Read).
	dest2 := make([]byte, 1024)
	n2, st2 := s.b.Read(context.Background(), h, 0, dest2)
	s.Require().Equal(proto.FsError_FS_OK, st2)
	s.Assert().Equal(500, n2)
}

func (s *CachedBackendTestSuite) TestReadAcrossChunkBoundaryFromCache() {
	// Regression: a Read whose destination spans more than one chunk must
	// continue past the chunk boundary when the chunk is full-sized. The
	// earlier early-return on "filled this chunk" treated a full chunk as
	// EOF, truncating reads of multi-chunk files to a single chunk.
	chunk0 := make([]byte, 1024)
	chunk1 := make([]byte, 1024)
	for i := range chunk0 {
		chunk0[i] = byte(i % 251)
		chunk1[i] = byte((i + 100) % 251)
	}
	s.b.data.put("/f", 0, chunk0)
	s.b.data.put("/f", 1, chunk1)
	h, _ := s.openCachedHandle("/f")

	dest := make([]byte, 2048)
	n, st := s.b.Read(context.Background(), h, 0, dest)
	s.Require().Equal(proto.FsError_FS_OK, st)
	s.Assert().Equal(2048, n, "Read spanning two full chunks must return both")
	s.Assert().Equal(chunk0, dest[:1024])
	s.Assert().Equal(chunk1, dest[1024:])
}

func (s *CachedBackendTestSuite) TestReadAcrossChunkBoundaryOnMiss() {
	// Same regression on the miss path: when inner.Read returns a full
	// chunk-sized response, we must NOT treat that as EOF; we must
	// continue to the next chunk to satisfy the rest of dest.
	h, innerH := s.openCachedHandle("/f")
	s.inner.EXPECT().Read(mock.Anything, innerH, int64(0), mock.MatchedBy(func(b []byte) bool { return len(b) == 1024 })).
		RunAndReturn(func(_ context.Context, _ io.FileHandle, _ int64, buf []byte) (int, proto.FsError) {
			for i := range buf {
				buf[i] = 0xAA
			}
			return 1024, proto.FsError_FS_OK
		}).Once()
	s.inner.EXPECT().Read(mock.Anything, innerH, int64(1024), mock.MatchedBy(func(b []byte) bool { return len(b) == 1024 })).
		RunAndReturn(func(_ context.Context, _ io.FileHandle, _ int64, buf []byte) (int, proto.FsError) {
			for i := range buf {
				buf[i] = 0xBB
			}
			return 1024, proto.FsError_FS_OK
		}).Once()

	dest := make([]byte, 2048)
	n, st := s.b.Read(context.Background(), h, 0, dest)
	s.Require().Equal(proto.FsError_FS_OK, st)
	s.Assert().Equal(2048, n, "Read spanning two chunks on miss must fetch both")
	for i := 0; i < 1024; i++ {
		s.Require().Equal(byte(0xAA), dest[i])
	}
	for i := 1024; i < 2048; i++ {
		s.Require().Equal(byte(0xBB), dest[i])
	}
}

// --- Invalidation table ---

func (s *CachedBackendTestSuite) TestWriteUpdatesAttrAndInvalidatesData() {
	s.b.attr.putPositive("/f", &io.Attr{Ino: 1, Size: 100})
	s.b.data.put("/f", 0, []byte("OLD-CONTENT"))
	h, innerH := s.openCachedHandle("/f")
	s.inner.EXPECT().Write(mock.Anything, innerH, int64(0), mock.Anything).
		Return(uint32(4), proto.FsError_FS_OK).Once()
	_, st := s.b.Write(context.Background(), h, 0, []byte("NEW!"))
	s.Require().Equal(proto.FsError_FS_OK, st)
	// Data range invalidated (a later read re-fetches):
	s.Assert().Nil(s.b.data.get("/f", 0))
	// Attr is NOT evicted — optimistically kept and stamped verified so the
	// GetAttr macOS/FUSE-T fires after the write is served from cache, not a RPC.
	a, hit, pos := s.b.attr.get("/f")
	s.Require().True(hit)
	s.Require().True(pos)
	s.Assert().Equal(uint64(100), a.Size, "in-file write keeps the larger cached size")
	s.Assert().True(s.b.validity.isPathVerified("/f"), "write stamps the path verified")
}

func (s *CachedBackendTestSuite) TestWriteGrowsCachedSize() {
	s.b.attr.putPositive("/f", &io.Attr{Ino: 1, Size: 100})
	h, innerH := s.openCachedHandle("/f")
	s.inner.EXPECT().Write(mock.Anything, innerH, int64(100), mock.Anything).
		Return(uint32(50), proto.FsError_FS_OK).Once()
	_, st := s.b.Write(context.Background(), h, 100, make([]byte, 50))
	s.Require().Equal(proto.FsError_FS_OK, st)
	a, hit, _ := s.b.attr.get("/f")
	s.Require().True(hit)
	s.Assert().Equal(uint64(150), a.Size, "append bumps cached size to off+n (acknowledged bytes)")
}

func (s *CachedBackendTestSuite) TestReleaseKeepsAttrForRevalidation() {
	// Release must NOT evict the written file's attr: on a cold restart the
	// persisted (optimistic, pre-write-version) attr must still be a hit so the
	// next Stat runs GetAttrIfChanged and invalidates stale data on a version
	// mismatch. Evicting here made restart serve stale chunks (regression in
	// e2e TestRestartRevalidatesAfterMutation).
	s.b.attr.putPositive("/f", &io.Attr{Ino: 1, Size: 100})
	h, innerH := s.openCachedHandle("/f")
	s.inner.EXPECT().Write(mock.Anything, innerH, int64(0), mock.Anything).
		Return(uint32(4), proto.FsError_FS_OK).Once()
	s.inner.EXPECT().Release(mock.Anything, innerH).Return(proto.FsError_FS_OK).Once()
	_, _ = s.b.Write(context.Background(), h, 0, []byte("NEW!"))
	s.b.Release(context.Background(), h)
	_, hit, _ := s.b.attr.get("/f")
	s.Assert().True(hit, "attr must survive Release so a cold restart can revalidate")
}

func (s *CachedBackendTestSuite) TestWriteOnlyInvalidatesOverlappingChunks() {
	// Pre-populate chunks 0, 1, 2. Write at chunk 1 boundary covering
	// only chunk 1. Chunks 0 and 2 must remain intact.
	s.b.data.put("/f", 0, make([]byte, 1024))
	s.b.data.put("/f", 1, make([]byte, 1024))
	s.b.data.put("/f", 2, make([]byte, 1024))
	h, innerH := s.openCachedHandle("/f")
	s.inner.EXPECT().Write(mock.Anything, innerH, int64(1024), mock.Anything).
		Return(uint32(100), proto.FsError_FS_OK).Once()
	_, st := s.b.Write(context.Background(), h, 1024, make([]byte, 100))
	s.Require().Equal(proto.FsError_FS_OK, st)
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
		Return(innerH, nil, proto.FsError_FS_OK).Once()
	h, a, st := s.b.Create(context.Background(), "/d", "new", 0, 0o644)
	s.Require().Equal(proto.FsError_FS_OK, st)
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
		Return(innerH, returnedAttr, proto.FsError_FS_OK).Once()
	_, a, st := s.b.Create(context.Background(), "/d", "new", 0, 0o644)
	s.Require().Equal(proto.FsError_FS_OK, st)
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
	// nil reply attrs (server's post-create stat failed): nothing is primed.
	s.inner.EXPECT().Mkdir(mock.Anything, "/d", mock.Anything).Return(nil, proto.FsError_FS_OK).Once()
	a, st := s.b.Mkdir(context.Background(), "/d", 0o755)
	s.Require().Equal(proto.FsError_FS_OK, st)
	s.Assert().Nil(a)
	_, hit, _ := s.b.attr.get("/d")
	s.Assert().False(hit) // negative dropped
	_, parentHit, _ := s.b.attr.get("")
	s.Assert().False(parentHit) // parent attr invalidated
	_, dirHit := s.b.dir.get("")
	s.Assert().False(dirHit) // parent dir invalidated
}

// TestMkdirPrimesAttrCacheFromReply: the reply attrs prime the attr cache
// (like Create), so the kernel's immediate Getattr on the new dir is a
// zero-RPC hit — no inner Stat expectation registered (absence proof).
func (s *CachedBackendTestSuite) TestMkdirPrimesAttrCacheFromReply() {
	returned := &io.Attr{Ino: 21, Mode: 0o40755}
	s.inner.EXPECT().Mkdir(mock.Anything, "/d", mock.Anything).Return(returned, proto.FsError_FS_OK).Once()
	a, st := s.b.Mkdir(context.Background(), "/d", 0o755)
	s.Require().Equal(proto.FsError_FS_OK, st)
	s.Require().NotNil(a)

	cached, st2 := s.b.Stat(context.Background(), "/d")
	s.Require().Equal(proto.FsError_FS_OK, st2)
	s.Assert().Equal(uint64(21), cached.Ino)
}

// TestSymlinkInvalidatesParentAndPrimes: parent dir+attr invalidation stays
// (a new dirent bumps the parent's mtime) and the reply attrs prime the
// link's attr entry — a follow-up Stat is a zero-RPC hit (absence proof via
// the strict mock).
func (s *CachedBackendTestSuite) TestSymlinkInvalidatesParentAndPrimes() {
	s.b.attr.putPositive("/d", &io.Attr{Ino: 1}) // parent attr cached
	s.b.dir.put("/d", []io.DirEntry{})
	returned := &io.Attr{Ino: 31, Mode: 0o120777}
	s.inner.EXPECT().Symlink(mock.Anything, "/target", "/d/lnk").Return(returned, proto.FsError_FS_OK).Once()
	a, st := s.b.Symlink(context.Background(), "/target", "/d/lnk")
	s.Require().Equal(proto.FsError_FS_OK, st)
	s.Require().NotNil(a)

	_, parentHit, _ := s.b.attr.get("/d")
	s.Assert().False(parentHit) // parent attr invalidated
	_, dirHit := s.b.dir.get("/d")
	s.Assert().False(dirHit) // parent listing invalidated

	cached, st2 := s.b.Stat(context.Background(), "/d/lnk")
	s.Require().Equal(proto.FsError_FS_OK, st2)
	s.Assert().Equal(uint64(31), cached.Ino)
}

// TestSymlinkNilAttrsJustInvalidates: nil reply attrs leave the link's attr
// entry invalidated (any stale negative dropped), so the next Stat refetches.
func (s *CachedBackendTestSuite) TestSymlinkNilAttrsJustInvalidates() {
	s.b.attr.putNegative("/d/lnk") // prior failed Lookup
	s.inner.EXPECT().Symlink(mock.Anything, "/target", "/d/lnk").Return(nil, proto.FsError_FS_OK).Once()
	a, st := s.b.Symlink(context.Background(), "/target", "/d/lnk")
	s.Require().Equal(proto.FsError_FS_OK, st)
	s.Assert().Nil(a)
	_, hit, _ := s.b.attr.get("/d/lnk")
	s.Assert().False(hit, "negative entry dropped, nothing primed")
}

func (s *CachedBackendTestSuite) TestRmdirInvalidatesAndNegativesPath() {
	s.b.attr.putPositive("/d", &io.Attr{Ino: 1})
	s.b.dir.put("/d", []io.DirEntry{})
	s.b.dir.put("", []io.DirEntry{{Name: "d"}})
	s.inner.EXPECT().Rmdir(mock.Anything, "/d").Return(proto.FsError_FS_OK).Once()
	st := s.b.Rmdir(context.Background(), "/d")
	s.Require().Equal(proto.FsError_FS_OK, st)
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

func (s *CachedBackendTestSuite) TestRmdirInvalidatesParentAttr() {
	s.b.attr.putPositive("", &io.Attr{Ino: 1}) // parent attr cached
	s.inner.EXPECT().Rmdir(mock.Anything, "/d").Return(proto.FsError_FS_OK).Once()
	st := s.b.Rmdir(context.Background(), "/d")
	s.Require().Equal(proto.FsError_FS_OK, st)
	// Rmdir removes a dir entry, bumping the parent's mtime — parent attr
	// must be invalidated so the originating client doesn't serve stale mtime.
	_, parentHit, _ := s.b.attr.get("")
	s.Assert().False(parentHit, "parent attr must be invalidated on Rmdir")
}

func (s *CachedBackendTestSuite) TestUnlinkInvalidatesAndNegativesPath() {
	s.b.attr.putPositive("/f", &io.Attr{Ino: 1})
	s.b.data.put("/f", 0, []byte("c"))
	s.b.dir.put("", []io.DirEntry{{Name: "f"}})
	s.inner.EXPECT().Unlink(mock.Anything, "/f").Return(proto.FsError_FS_OK).Once()
	st := s.b.Unlink(context.Background(), "/f")
	s.Require().Equal(proto.FsError_FS_OK, st)
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

func (s *CachedBackendTestSuite) TestUnlinkInvalidatesParentAttr() {
	s.b.attr.putPositive("", &io.Attr{Ino: 1}) // parent attr cached
	s.inner.EXPECT().Unlink(mock.Anything, "/f").Return(proto.FsError_FS_OK).Once()
	st := s.b.Unlink(context.Background(), "/f")
	s.Require().Equal(proto.FsError_FS_OK, st)
	// Unlink removes a dir entry, bumping the parent's mtime — parent attr
	// must be invalidated so the originating client doesn't serve stale mtime.
	_, parentHit, _ := s.b.attr.get("")
	s.Assert().False(parentHit, "parent attr must be invalidated on Unlink")
}

func (s *CachedBackendTestSuite) TestRenameInvalidatesBothPaths() {
	s.b.attr.putPositive("/a", &io.Attr{Ino: 1})
	s.b.attr.putPositive("/b", &io.Attr{Ino: 2})
	s.b.data.put("/a", 0, []byte("aa"))
	s.b.data.put("/b", 0, []byte("bb"))
	s.b.dir.put("", []io.DirEntry{})
	s.inner.EXPECT().Rename(mock.Anything, "/a", "/b").Return(proto.FsError_FS_OK).Once()
	st := s.b.Rename(context.Background(), "/a", "/b")
	s.Require().Equal(proto.FsError_FS_OK, st)
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

func (s *CachedBackendTestSuite) TestRenameInvalidatesOldParentAttr() {
	// Rename from "src/a" to "src/b" (same parent): old-parent attr must be
	// invalidated because the rename changes the parent directory's mtime.
	s.b.attr.putPositive("src", &io.Attr{Ino: 10})
	s.inner.EXPECT().Rename(mock.Anything, "src/a", "src/b").Return(proto.FsError_FS_OK).Once()
	st := s.b.Rename(context.Background(), "src/a", "src/b")
	s.Require().Equal(proto.FsError_FS_OK, st)
	_, oldParentHit, _ := s.b.attr.get("src")
	s.Assert().False(oldParentHit, "old parent attr must be invalidated on Rename")
}

func (s *CachedBackendTestSuite) TestRenameInvalidatesNewParentAttr() {
	// Rename across directories: new-parent attr must also be invalidated.
	s.b.attr.putPositive("dst", &io.Attr{Ino: 20})
	s.inner.EXPECT().Rename(mock.Anything, "src/a", "dst/a").Return(proto.FsError_FS_OK).Once()
	st := s.b.Rename(context.Background(), "src/a", "dst/a")
	s.Require().Equal(proto.FsError_FS_OK, st)
	_, newParentHit, _ := s.b.attr.get("dst")
	s.Assert().False(newParentHit, "new parent attr must be invalidated on Rename")
}

// TestSetAttrWithSizeInvalidatesDataAndPrimesAttr: FATTR_SIZE means truncate
// — every cached chunk's relationship to the new length is suspect (same
// conservatism as Truncate), and the attr cache is re-primed from the
// reply's final attrs (no extra Stat RTT to refill).
func (s *CachedBackendTestSuite) TestSetAttrWithSizeInvalidatesDataAndPrimesAttr() {
	s.b.data.put("/f", 0, make([]byte, 1024))
	s.b.data.put("/f", 1, make([]byte, 1024))
	s.b.attr.putPositive("/f", &io.Attr{Size: 2048, Mode: 0o644})
	in := io.SetAttrIn{Valid: fuse.FATTR_SIZE | fuse.FATTR_MODE, Size: 100, Mode: 0o600}
	final := &io.Attr{Ino: 9, Size: 100, Mode: 0o600}
	s.inner.EXPECT().SetAttr(mock.Anything, "/f", in).Return(final, proto.FsError_FS_OK).Once()

	a, st := s.b.SetAttr(context.Background(), "/f", in)
	s.Require().Equal(proto.FsError_FS_OK, st)
	s.Require().Equal(final, a)
	// Data chunks dropped (truncate changes content).
	s.Assert().Nil(s.b.data.get("/f", 0))
	s.Assert().Nil(s.b.data.get("/f", 1))
	// Attr primed from the reply, not invalidated.
	cached, hit, pos := s.b.attr.get("/f")
	s.Require().True(hit)
	s.Require().True(pos)
	s.Assert().Equal(uint64(100), cached.Size)
	s.Assert().Equal(uint32(0o600), cached.Mode)
}

// TestSetAttrWithoutSizeKeepsDataAndPrimesAttr: a chmod/utimes-shaped
// SetAttr doesn't touch file content — data chunks must survive.
func (s *CachedBackendTestSuite) TestSetAttrWithoutSizeKeepsDataAndPrimesAttr() {
	s.b.data.put("/f", 0, []byte("DATA"))
	s.b.attr.putPositive("/f", &io.Attr{Mode: 0o644})
	in := io.SetAttrIn{Valid: fuse.FATTR_MODE, Mode: 0o600}
	s.inner.EXPECT().SetAttr(mock.Anything, "/f", in).
		Return(&io.Attr{Mode: 0o600}, proto.FsError_FS_OK).Once()

	_, st := s.b.SetAttr(context.Background(), "/f", in)
	s.Require().Equal(proto.FsError_FS_OK, st)
	s.Assert().NotNil(s.b.data.get("/f", 0)) // data untouched
	cached, hit, pos := s.b.attr.get("/f")
	s.Require().True(hit)
	s.Require().True(pos)
	s.Assert().Equal(uint32(0o600), cached.Mode)
}

// TestSetAttrFailureStillInvalidates: unlike the per-field wrappers, a
// failed SetAttr may have applied EARLIER fields before stopping
// (size→mode→owner→times server order), so caches are dropped even on a
// non-OK status.
func (s *CachedBackendTestSuite) TestSetAttrFailureStillInvalidates() {
	s.b.data.put("/f", 0, []byte("DATA"))
	s.b.attr.putPositive("/f", &io.Attr{Size: 4, Mode: 0o644})
	in := io.SetAttrIn{Valid: fuse.FATTR_SIZE | fuse.FATTR_MODE, Size: 0, Mode: 0o600}
	s.inner.EXPECT().SetAttr(mock.Anything, "/f", in).Return(nil, proto.FsError_FS_EPERM).Once()

	_, st := s.b.SetAttr(context.Background(), "/f", in)
	s.Require().Equal(proto.FsError_FS_EPERM, st)
	s.Assert().Nil(s.b.data.get("/f", 0)) // SIZE was requested; truncate may have applied
	_, hit, _ := s.b.attr.get("/f")
	s.Assert().False(hit)
}

func (s *CachedBackendTestSuite) TestAllocateInvalidatesDataRangeAndKeepsAttr() {
	// Pre-populate 3 chunks. Allocate covers only chunks 0 and 1.
	s.b.data.put("/f", 0, make([]byte, 1024))
	s.b.data.put("/f", 1, make([]byte, 1024))
	s.b.data.put("/f", 2, make([]byte, 1024))
	s.b.attr.putPositive("/f", &io.Attr{Size: 3072})
	h, innerH := s.openCachedHandle("/f")
	s.inner.EXPECT().Allocate(mock.Anything, innerH, uint64(0), uint64(1500), mock.Anything).
		Return(proto.FsError_FS_OK).Once()
	st := s.b.Allocate(context.Background(), h, 0, 1500, 0)
	s.Require().Equal(proto.FsError_FS_OK, st)
	// Chunks 0 and 1 invalidated; chunk 2 untouched.
	s.Assert().Nil(s.b.data.get("/f", 0))
	s.Assert().Nil(s.b.data.get("/f", 1))
	s.Assert().NotNil(s.b.data.get("/f", 2))
	// Attr kept (optimistic update), not evicted; in-file alloc keeps the size.
	a, hit, _ := s.b.attr.get("/f")
	s.Require().True(hit)
	s.Assert().Equal(uint64(3072), a.Size)
}

// --- Pass-through ops (no cache mutations) ---

func (s *CachedBackendTestSuite) TestReleaseDoesNotInvalidate() {
	s.b.attr.putPositive("/f", &io.Attr{Ino: 1})
	s.b.data.put("/f", 0, []byte("DATA"))
	h, innerH := s.openCachedHandle("/f")
	s.inner.EXPECT().Release(mock.Anything, innerH).Return(proto.FsError_FS_OK).Once()
	st := s.b.Release(context.Background(), h)
	s.Require().Equal(proto.FsError_FS_OK, st)
	// Cache untouched.
	_, hit, pos := s.b.attr.get("/f")
	s.Require().True(hit)
	s.Assert().True(pos)
	s.Assert().NotNil(s.b.data.get("/f", 0))
}

func (s *CachedBackendTestSuite) TestStatFsCachedWithinTTL() {
	// SetupTest leaves StatFsTTL unset (cache off); enable it for this test.
	s.b.statfs = newStatfsCache(time.Minute, nil)
	// inner.StatFs is expected exactly once — the second call must be served
	// from cache (a second RPC would be an unexpected mock call and fail).
	s.inner.EXPECT().StatFs(mock.Anything, "/").
		Return(&io.StatFs{Bfree: 42}, proto.FsError_FS_OK).Once()
	v1, st := s.b.StatFs(context.Background(), "/")
	s.Require().Equal(proto.FsError_FS_OK, st)
	s.Assert().Equal(uint64(42), v1.Bfree)
	v2, st := s.b.StatFs(context.Background(), "/")
	s.Require().Equal(proto.FsError_FS_OK, st)
	s.Assert().Equal(uint64(42), v2.Bfree, "second StatFs served from cache, no RPC")
}

func (s *CachedBackendTestSuite) TestFlushDoesNotInvalidate() {
	s.b.attr.putPositive("/f", &io.Attr{Ino: 1})
	s.b.data.put("/f", 0, []byte("DATA"))
	h, innerH := s.openCachedHandle("/f")
	s.inner.EXPECT().Flush(mock.Anything, innerH).Return(proto.FsError_FS_OK).Once()
	st := s.b.Flush(context.Background(), h)
	s.Require().Equal(proto.FsError_FS_OK, st)
	_, hit, _ := s.b.attr.get("/f")
	s.Assert().True(hit)
	s.Assert().NotNil(s.b.data.get("/f", 0))
}

func (s *CachedBackendTestSuite) TestFsyncDoesNotInvalidate() {
	s.b.attr.putPositive("/f", &io.Attr{Ino: 1})
	s.b.data.put("/f", 0, []byte("DATA"))
	h, innerH := s.openCachedHandle("/f")
	s.inner.EXPECT().Fsync(mock.Anything, innerH, int64(0)).Return(proto.FsError_FS_OK).Once()
	st := s.b.Fsync(context.Background(), h, 0)
	s.Require().Equal(proto.FsError_FS_OK, st)
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
		Return(proto.FsError_FS_OK).Once()
	s.inner.EXPECT().SetLk(mock.Anything, innerH, uint64(1), lk, uint32(0)).
		Return(proto.FsError_FS_OK).Once()
	s.inner.EXPECT().SetLkw(mock.Anything, innerH, uint64(1), lk, uint32(0)).
		Return(proto.FsError_FS_OK).Once()
	s.Require().Equal(proto.FsError_FS_OK, s.b.GetLk(context.Background(), h, 1, lk, 0, out))
	s.Require().Equal(proto.FsError_FS_OK, s.b.SetLk(context.Background(), h, 1, lk, 0))
	s.Require().Equal(proto.FsError_FS_OK, s.b.SetLkw(context.Background(), h, 1, lk, 0))
	// Cache untouched.
	_, hit, _ := s.b.attr.get("/f")
	s.Assert().True(hit)
	s.Assert().NotNil(s.b.data.get("/f", 0))
}

// --- Failure-path no-invalidation guards ---

func (s *CachedBackendTestSuite) TestMutationFailureDoesNotInvalidate() {
	// If the inner backend rejects the mutation we must NOT invalidate
	// the cache — invalidation is conditional on inner success. (SetAttr is
	// the deliberate exception: see TestSetAttrFailureStillInvalidates.)
	s.b.attr.putPositive("/f", &io.Attr{Ino: 1})
	s.inner.EXPECT().Unlink(mock.Anything, "/f").Return(proto.FsError_FS_EACCES).Once()
	st := s.b.Unlink(context.Background(), "/f")
	s.Require().Equal(proto.FsError_FS_EACCES, st)
	// Cache still has the old attr.
	_, hit, pos := s.b.attr.get("/f")
	s.Require().True(hit)
	s.Assert().True(pos)
}

// --- Validity-gating tests (Sub-spec D) ---

// newUnverifiedBackend builds a cachedBackend in the default unverified state
// for gating-path tests. SubscribeEnabled=true with a nil client keeps the
// validity tracker in stateUnverified (no subscriber starts, no
// markGlobalVerified). This exercises the gating path where Subscribe is
// the intended freshness mechanism but the stream hasn't delivered its
// first HEARTBEAT yet.
func newUnverifiedBackend(t *testing.T, inner *iomocks.MockFileSystemBackend) *cachedBackend {
	t.Helper()
	cb := NewCachedBackend(inner, Config{
		SubscribeEnabled: true,
		MemoryMaxBytes:   1 << 20,
		ChunkSizeBytes:   1 << 16,
		AttrTTL:          time.Hour,
		DirTTL:           time.Hour,
		NegativeTTL:      time.Minute,
	}, nil, nil, "").(*cachedBackend)
	return cb
}

func (s *CachedBackendTestSuite) TestUnverifiedStateGatesStatViaGetAttrIfChanged() {
	inner := iomocks.NewMockFileSystemBackend(s.T())
	be := newUnverifiedBackend(s.T(), inner)

	// First Stat: cache miss → inner.Stat called, entry populated.
	inner.EXPECT().Stat(mock.Anything, "f").Return(&io.Attr{Ino: 7, Size: 11, Version: 42}, proto.FsError_FS_OK).Once()
	_, st := be.Stat(context.Background(), "f")
	s.Require().Equal(proto.FsError_FS_OK, st)

	// Second Stat: cache hit but unverified → GetAttrIfChanged called.
	inner.EXPECT().GetAttrIfChanged(mock.Anything, "f", uint64(42)).Return(nil, true, proto.FsError_FS_OK).Once()
	a, st := be.Stat(context.Background(), "f")
	s.Require().Equal(proto.FsError_FS_OK, st)
	s.Assert().Equal(uint64(42), a.Version)
}

func (s *CachedBackendTestSuite) TestUnverifiedRevalidationOnVersionChangeInvalidatesAllThree() {
	inner := iomocks.NewMockFileSystemBackend(s.T())
	be := newUnverifiedBackend(s.T(), inner)

	// Seed the cache.
	inner.EXPECT().Stat(mock.Anything, "f").Return(&io.Attr{Version: 42}, proto.FsError_FS_OK).Once()
	_, _ = be.Stat(context.Background(), "f")

	// Version changed: server returns new attrs.
	freshAttr := &io.Attr{Version: 99, Size: 200}
	inner.EXPECT().GetAttrIfChanged(mock.Anything, "f", uint64(42)).Return(freshAttr, false, proto.FsError_FS_OK).Once()
	a, st := be.Stat(context.Background(), "f")
	s.Require().Equal(proto.FsError_FS_OK, st)
	s.Assert().Equal(uint64(99), a.Version)
	s.Assert().Equal(uint64(200), a.Size)
}

func (s *CachedBackendTestSuite) TestUnverifiedRevalidationENOENTReturnsNotFound() {
	inner := iomocks.NewMockFileSystemBackend(s.T())
	be := newUnverifiedBackend(s.T(), inner)

	// Seed the cache.
	inner.EXPECT().Stat(mock.Anything, "f").Return(&io.Attr{Version: 42}, proto.FsError_FS_OK).Once()
	_, _ = be.Stat(context.Background(), "f")

	// Path gone on server.
	inner.EXPECT().GetAttrIfChanged(mock.Anything, "f", uint64(42)).Return(nil, false, proto.FsError_FS_ENOENT).Once()
	_, st := be.Stat(context.Background(), "f")
	s.Require().Equal(proto.FsError_FS_ENOENT, st)
}

// --- CopyFileRange ---

// CopyFileRange must invalidate the destination's cached data range and
// attr entry exactly like a Write of [offOut, offOut+n). A chunk outside
// the copied range stays cached; the source is untouched.
func (s *CachedBackendTestSuite) TestCopyFileRangeInvalidatesDestination() {
	srcH, innerSrc := s.openCachedHandle("/src")
	dstH, innerDst := s.openCachedHandle("/dst")

	// Seed dst attr cache.
	s.inner.EXPECT().Stat(mock.Anything, "/dst").Return(&io.Attr{Ino: 2, Size: 4096}, proto.FsError_FS_OK).Once()
	_, st := s.b.Stat(context.Background(), "/dst")
	s.Require().Equal(proto.FsError_FS_OK, st)

	// Seed dst data cache: chunk 0 ([0,1024)) and chunk 2 ([2048,3072)).
	buf := make([]byte, 1024)
	s.inner.EXPECT().Read(mock.Anything, innerDst, int64(0), mock.Anything).
		RunAndReturn(func(_ context.Context, _ io.FileHandle, _ int64, b []byte) (int, proto.FsError) {
			return 1024, proto.FsError_FS_OK
		}).Once()
	_, st = s.b.Read(context.Background(), dstH, 0, buf)
	s.Require().Equal(proto.FsError_FS_OK, st)
	s.inner.EXPECT().Read(mock.Anything, innerDst, int64(2048), mock.Anything).
		RunAndReturn(func(_ context.Context, _ io.FileHandle, _ int64, b []byte) (int, proto.FsError) {
			return 1024, proto.FsError_FS_OK
		}).Once()
	_, st = s.b.Read(context.Background(), dstH, 2048, buf)
	s.Require().Equal(proto.FsError_FS_OK, st)

	// Copy 100 bytes into dst@0 — overlaps chunk 0 only.
	s.inner.EXPECT().CopyFileRange(mock.Anything, innerSrc, uint64(0), innerDst, uint64(0), uint64(100), uint64(0)).
		Return(uint64(100), proto.FsError_FS_OK).Once()
	n, cst := s.b.CopyFileRange(context.Background(), srcH, 0, dstH, 0, 100, 0)
	s.Require().Equal(proto.FsError_FS_OK, cst)
	s.Require().Equal(uint64(100), n)

	// Chunk 0 must MISS (re-fetch from inner)...
	s.inner.EXPECT().Read(mock.Anything, innerDst, int64(0), mock.Anything).
		RunAndReturn(func(_ context.Context, _ io.FileHandle, _ int64, b []byte) (int, proto.FsError) {
			return 1024, proto.FsError_FS_OK
		}).Once()
	_, st = s.b.Read(context.Background(), dstH, 0, buf)
	s.Require().Equal(proto.FsError_FS_OK, st)
	// ...chunk 2 must still HIT (no new EXPECT: served from cache).
	_, st = s.b.Read(context.Background(), dstH, 2048, buf)
	s.Require().Equal(proto.FsError_FS_OK, st)

	// Attr must MISS after the copy (size/mtime moved).
	s.inner.EXPECT().Stat(mock.Anything, "/dst").Return(&io.Attr{Ino: 2, Size: 4096}, proto.FsError_FS_OK).Once()
	_, st = s.b.Stat(context.Background(), "/dst")
	s.Require().Equal(proto.FsError_FS_OK, st)
}

// Lseek and the xattr trio are pure pass-throughs — one delegation test
// each keeps the interface honest without over-testing.
func (s *CachedBackendTestSuite) TestLseekAndXattrPassThrough() {
	h, innerH := s.openCachedHandle("/f")
	s.inner.EXPECT().Lseek(mock.Anything, innerH, uint64(5), uint32(3)).Return(uint64(9), proto.FsError_FS_OK).Once()
	off, st := s.b.Lseek(context.Background(), h, 5, 3)
	s.Require().Equal(proto.FsError_FS_OK, st)
	s.Equal(uint64(9), off)

	s.inner.EXPECT().SetXAttr(mock.Anything, "/f", "user.k", []byte("v"), uint32(0)).Return(proto.FsError_FS_OK).Once()
	s.Equal(proto.FsError_FS_OK, s.b.SetXAttr(context.Background(), "/f", "user.k", []byte("v"), 0))

	s.inner.EXPECT().RemoveXAttr(mock.Anything, "/f", "user.k").Return(proto.FsError_FS_OK).Once()
	s.Equal(proto.FsError_FS_OK, s.b.RemoveXAttr(context.Background(), "/f", "user.k"))

	s.inner.EXPECT().ListXAttr(mock.Anything, "/f").Return([]string{"user.k"}, proto.FsError_FS_OK).Once()
	names, st := s.b.ListXAttr(context.Background(), "/f")
	s.Require().Equal(proto.FsError_FS_OK, st)
	s.Equal([]string{"user.k"}, names)
}

// --- xattr cache tests ---

// TestListXAttrServesFromCacheAfterFirstCall verifies that the second ListXAttr
// call for the same path is served from the xattr cache without hitting inner
// (the .Once() expectation on inner would fail if inner were called twice).
func (s *CachedBackendTestSuite) TestListXAttrServesFromCacheAfterFirstCall() {
	s.inner.EXPECT().ListXAttr(mock.Anything, "f").Return([]string{"user.a"}, proto.FsError_FS_OK).Once()
	n1, st1 := s.b.ListXAttr(context.Background(), "f")
	s.Require().Equal(proto.FsError_FS_OK, st1)
	s.Equal([]string{"user.a"}, n1)
	// Second call must NOT hit inner (Once() above would fail on a 2nd call).
	n2, st2 := s.b.ListXAttr(context.Background(), "f")
	s.Require().Equal(proto.FsError_FS_OK, st2)
	s.Equal([]string{"user.a"}, n2)
}

// TestListDirPrimesXattrCache verifies that entries with XattrListed=true returned
// by ListDir prime the xattr cache, so a follow-up ListXAttr on a child path is
// served without any inner.ListXAttr call (the cold-pass readdir win).
func (s *CachedBackendTestSuite) TestListDirPrimesXattrCache() {
	s.inner.EXPECT().ListDir(mock.Anything, "d").Return([]io.DirEntryPlus{{
		DirEntry:    io.DirEntry{Name: "child"},
		XattrListed: true,
		XattrNames:  []string{"user.k"},
	}}, proto.FsError_FS_OK).Once()
	_, st := s.b.ListDir(context.Background(), "d")
	s.Require().Equal(proto.FsError_FS_OK, st)
	// ListXAttr on the primed child must be served from cache (no inner call).
	names, xst := s.b.ListXAttr(context.Background(), "d/child")
	s.Require().Equal(proto.FsError_FS_OK, xst)
	s.Equal([]string{"user.k"}, names)
	// no inner.ListXAttr expectation set → mock auto-asserts no unexpected calls
}

// TestSetXAttrInvalidatesXattrAndAttr verifies that a successful SetXAttr drops
// both the xattr name-list cache (forcing re-fetch) and the attr cache (an xattr
// write bumps inode ctime, making the cached attr stale).
func (s *CachedBackendTestSuite) TestSetXAttrInvalidatesXattrAndAttr() {
	// Prime both caches.
	s.inner.EXPECT().ListXAttr(mock.Anything, "f").Return([]string{"user.a"}, proto.FsError_FS_OK).Times(2)
	_, _ = s.b.ListXAttr(context.Background(), "f")
	s.b.attr.putPositive("f", &io.Attr{Ino: 1})
	s.inner.EXPECT().SetXAttr(mock.Anything, "f", "user.b", []byte("v"), uint32(0)).Return(proto.FsError_FS_OK).Once()
	s.Require().Equal(proto.FsError_FS_OK, s.b.SetXAttr(context.Background(), "f", "user.b", []byte("v"), 0))
	// Attr must be invalidated (xattr write bumps ctime).
	_, hit, _ := s.b.attr.get("f")
	s.Assert().False(hit)
	// Next ListXAttr must re-hit inner (Times(2) allows the second call).
	_, _ = s.b.ListXAttr(context.Background(), "f")
}

// TestJoinPathMatchesWirePath pins MN-L2: the cache key MUST equal the raw wire
// path the io layer produces (parent + "/" + name, with empty-parent
// passthrough). If this ever drifts back to path.Join normalization, a cache
// key could stop matching a Subscribe event's path and invalidation would miss.
func (s *CachedBackendTestSuite) TestJoinPathMatchesWirePath() {
	cases := []struct{ parent, name, want string }{
		{"", "child", "child"},        // root: passthrough, no leading slash
		{"a", "b", "a/b"},             // simple
		{"a/b", "c.txt", "a/b/c.txt"}, // nested
		{"/abs", "x", "/abs/x"},       // absolute parent kept verbatim
		{"a/", "b", "a//b"},           // raw form: NO normalization (mirrors io)
		{"dir", "..", "dir/.."},       // raw form: dotdot not collapsed (mirrors io)
	}
	for _, c := range cases {
		s.Assert().Equalf(c.want, joinPath(c.parent, c.name),
			"joinPath(%q,%q) must equal the io-layer wire form", c.parent, c.name)
	}
}

func TestCachedBackendTestSuite(t *testing.T) {
	suite.Run(t, new(CachedBackendTestSuite))
}
