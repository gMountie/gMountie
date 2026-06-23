package cache

import (
	"context"
	"sync"
	"testing"
	"time"

	"go.gmountie.dev/gmountie/pkg/client/backend"
	"go.gmountie.dev/gmountie/pkg/proto"

	"github.com/stretchr/testify/suite"
)

// fakeOracle is a test double for DelegationOracle with a settable delegated set.
type fakeOracle struct {
	mu        sync.Mutex
	delegated map[string]bool
}

func newFakeOracle() *fakeOracle {
	return &fakeOracle{delegated: make(map[string]bool)}
}

func (o *fakeOracle) IsDelegated(path string) bool {
	o.mu.Lock()
	defer o.mu.Unlock()
	return o.delegated[path]
}

// fakeFileHandle is a minimal backend.FileHandle used by fakeCountingInner.Open.
type fakeFileHandle struct{ path string }

func (h *fakeFileHandle) Path() string               { return h.path }
func (h *fakeFileHandle) Unwrap() backend.FileHandle { return h }

// fakeCountingInner is a minimal FileSystemBackend that tracks
// GetAttrIfChanged calls per path and supports setting per-path server
// versions. It embeds backend.PassthroughBackend with a nil Inner — the
// only methods this fake needs to handle are Stat, GetAttrIfChanged,
// Open, and ListDir.
type fakeCountingInner struct {
	backend.PassthroughBackend
	mu                    sync.Mutex
	versions              map[string]uint64 // current server-side version
	getAttrIfChangedCalls map[string]int    // call count per path
}

func newFakeCountingInner() *fakeCountingInner {
	return &fakeCountingInner{
		versions:              make(map[string]uint64),
		getAttrIfChangedCalls: make(map[string]int),
	}
}

// setVersion sets the server-side version for path (simulates a remote write).
func (f *fakeCountingInner) setVersion(path string, v uint64) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.versions[path] = v
}

func (f *fakeCountingInner) Stat(_ context.Context, path string) (*backend.Attr, proto.FsError) {
	f.mu.Lock()
	v := f.versions[path]
	f.mu.Unlock()
	return &backend.Attr{Version: v}, proto.FsError_FS_OK
}

// GetAttrIfChanged counts every call per path. If known == serverVersion
// it returns notModified=true; otherwise it returns the fresh attrs.
func (f *fakeCountingInner) GetAttrIfChanged(_ context.Context, path string, known uint64) (*backend.Attr, bool, proto.FsError) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.getAttrIfChangedCalls[path]++
	sv := f.versions[path]
	if known == sv {
		return nil, true, proto.FsError_FS_OK
	}
	return &backend.Attr{Version: sv}, false, proto.FsError_FS_OK
}

// Open returns a stub FileHandle so the cache layer can wrap it in a
// cachedHandle. No inner call is recorded — Open is not under test here.
func (f *fakeCountingInner) Open(_ context.Context, path string, _ uint32) (backend.FileHandle, proto.FsError) {
	return &fakeFileHandle{path: path}, proto.FsError_FS_OK
}

// ListDir is never reached in the delegation-skip tests (the cache always
// hits), but must satisfy the interface used by the test via PassthroughBackend
// override so a nil-Inner panic doesn't fire on an unexpected fall-through.
func (f *fakeCountingInner) ListDir(_ context.Context, _ string) ([]backend.DirEntryPlus, proto.FsError) {
	return nil, proto.FsError_FS_ENOENT
}

func (f *fakeCountingInner) Close() error { return nil }

// DelegationSkipSuite tests that the delegation oracle fast-path in the cache
// is wired correctly and that removing a delegation restores full revalidation.
type DelegationSkipSuite struct {
	suite.Suite
	inner  *fakeCountingInner
	oracle *fakeOracle
	b      *cachedBackend
	ctx    context.Context
}

func (s *DelegationSkipSuite) SetupTest() {
	s.inner = newFakeCountingInner()
	s.oracle = newFakeOracle()
	cb := NewCachedBackend(s.inner, Config{
		SubscribeEnabled: true, // keeps tracker unverified until first HEARTBEAT
		MemoryMaxBytes:   1 << 20,
		ChunkSizeBytes:   1 << 16,
		AttrTTL:          time.Hour,
		DirTTL:           time.Hour,
		NegativeTTL:      time.Minute,
	}, nil, nil, "", nil, s.oracle).(*cachedBackend)
	// tracker stays unverified (no subscriber runs, no markGlobalVerified)
	s.b = cb
	s.ctx = context.Background()
}

// primeAttr inserts a positive attr entry directly into the attr cache
// WITHOUT routing through Stat (which would call markPathVerified and
// short-circuit the gate in the test). The Version field is used by
// GetAttrIfChanged to detect staleness.
func (s *DelegationSkipSuite) primeAttr(path string, version uint64) {
	s.b.attr.putPositive(path, &backend.Attr{Version: version})
	s.inner.setVersion(path, version) // server matches unless overridden later
}

// TestDelegatedPathSkipsRevalidationWhenUnverified is the PERF proof:
// during an UNVERIFIED window, a delegated path does NOT call
// GetAttrIfChanged (it holds the recall-before-change guarantee), while a
// non-delegated path still revalidates as usual.
func (s *DelegationSkipSuite) TestDelegatedPathSkipsRevalidationWhenUnverified() {
	s.oracle.delegated["proj/a"] = true
	s.b.validity.markGlobalUnverified() // force the revalidation path
	s.primeAttr("proj/a", 7)
	s.primeAttr("other/b", 9)

	_, _ = s.b.Stat(s.ctx, "proj/a")
	_, _ = s.b.Stat(s.ctx, "other/b")

	s.Equal(0, s.inner.getAttrIfChangedCalls["proj/a"],
		"delegated path must skip GetAttrIfChanged (holds recall-before-change guarantee)")
	s.Equal(1, s.inner.getAttrIfChangedCalls["other/b"],
		"non-delegated path must still revalidate when unverified")
}

// TestRecallRestoresRevalidationSoNoStale is the CORRECTNESS proof:
// a delegated path skips revalidation and therefore serves the stale cached
// version — that is SAFE because a recall is guaranteed before any remote
// write. Once the recall fires (oracle flips false), the next Stat must
// revalidate and return the fresh version.
func (s *DelegationSkipSuite) TestRecallRestoresRevalidationSoNoStale() {
	s.oracle.delegated["proj/a"] = true
	s.b.validity.markGlobalUnverified()
	s.primeAttr("proj/a", 7)        // cached version 7
	s.inner.setVersion("proj/a", 8) // server moved on (remote write)

	// While delegated: skip revalidation → cached version 7 is served.
	// This is the expected (correct) behavior — the recall-before-change
	// guarantee ensures no stale exposure without a recall signal.
	attr, st := s.b.Stat(s.ctx, "proj/a")
	s.Require().Equal(proto.FsError_FS_OK, st)
	s.Equal(uint64(7), attr.Version,
		"while delegated, stale cached version is safely served (recall not yet fired)")
	s.Equal(0, s.inner.getAttrIfChangedCalls["proj/a"],
		"while delegated, GetAttrIfChanged must not be called")

	// Simulate the recall: delegation is dropped.
	s.oracle.delegated["proj/a"] = false

	// After recall: no longer delegated → must revalidate → fresh version 8.
	attr2, st2 := s.b.Stat(s.ctx, "proj/a")
	s.Require().Equal(proto.FsError_FS_OK, st2)
	s.Equal(uint64(8), attr2.Version,
		"after recall, revalidation must return fresh server version")
	s.Equal(1, s.inner.getAttrIfChangedCalls["proj/a"],
		"after recall, GetAttrIfChanged must be called exactly once")
}

// primeDir inserts a directory listing directly into the dir cache and also
// primes the attrs for the directory path so the ListDir revalidation path
// (which calls b.attr.get(p) before revalidating) has a cached version to
// compare against GetAttrIfChanged.
func (s *DelegationSkipSuite) primeDir(path string, version uint64) {
	s.primeAttr(path, version)
	s.b.dir.put(path, []backend.DirEntry{{Name: "entry"}})
}

// TestDelegatedPathSkipsListDirRevalidation verifies the ListDir fast-path gate:
// during an UNVERIFIED window a delegated directory is served from cache without
// calling GetAttrIfChanged, while a non-delegated directory still revalidates.
func (s *DelegationSkipSuite) TestDelegatedPathSkipsListDirRevalidation() {
	s.oracle.delegated["dirs/delegated"] = true
	s.b.validity.markGlobalUnverified()
	s.primeDir("dirs/delegated", 3)
	s.primeDir("dirs/other", 5)

	_, _ = s.b.ListDir(s.ctx, "dirs/delegated")
	_, _ = s.b.ListDir(s.ctx, "dirs/other")

	s.Equal(0, s.inner.getAttrIfChangedCalls["dirs/delegated"],
		"delegated dir must skip GetAttrIfChanged (recall-before-change guarantee)")
	s.Equal(1, s.inner.getAttrIfChangedCalls["dirs/other"],
		"non-delegated dir must revalidate when unverified")
}

// TestDelegatedPathSkipsReadRevalidation verifies the (De Morgan-inverted) Read
// fast-path gate: during an UNVERIFIED window a delegated file path skips the
// attr revalidation that normally precedes the data-chunk lookup, while a
// non-delegated path still revalidates. A data chunk is primed for each path so
// the read is served from cache and never reaches the inner backend's Read.
func (s *DelegationSkipSuite) TestDelegatedPathSkipsReadRevalidation() {
	s.oracle.delegated["files/delegated"] = true
	s.b.validity.markGlobalUnverified()

	// Prime attrs (needed by the revalidation path inside Read).
	s.primeAttr("files/delegated", 11)
	s.primeAttr("files/other", 13)

	// Prime chunk 0 for both paths so the read loop is satisfied from the data
	// cache and never calls inner.Read (which would panic via PassthroughBackend).
	chunk := []byte("hello cache")
	s.b.data.put("files/delegated", 0, chunk)
	s.b.data.put("files/other", 0, chunk)

	// Build cachedHandles directly (same package — no Open call needed).
	hDelegated := newCachedHandle(&fakeFileHandle{path: "files/delegated"}, "files/delegated")
	hOther := newCachedHandle(&fakeFileHandle{path: "files/other"}, "files/other")

	buf := make([]byte, len(chunk))
	_, _ = s.b.Read(s.ctx, hDelegated, 0, buf)
	_, _ = s.b.Read(s.ctx, hOther, 0, buf)

	s.Equal(0, s.inner.getAttrIfChangedCalls["files/delegated"],
		"delegated file must skip GetAttrIfChanged during unverified window")
	s.Equal(1, s.inner.getAttrIfChangedCalls["files/other"],
		"non-delegated file must revalidate when unverified")
}

func TestDelegationSkipSuite(t *testing.T) {
	suite.Run(t, new(DelegationSkipSuite))
}
