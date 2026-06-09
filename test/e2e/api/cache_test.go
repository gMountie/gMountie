package api

import (
	"crypto/sha256"
	"fmt"
	"io"
	"math/rand"
	"os"
	"path/filepath"
	"testing"
	"time"

	clientconfig "go.gmountie.dev/gmountie/pkg/client/config"
	"go.gmountie.dev/gmountie/test/e2e/utils"

	"github.com/stretchr/testify/suite"
)

// CacheEnabledFSSuite exercises the new in-memory cache decorator under
// real FUSE syscalls. Each test asserts a correctness invariant that
// silent cache bugs would break: write-then-read returns the new bytes,
// mutations are visible to subsequent ops, ENOENT cache doesn't outlast
// a file's creation, etc.
type CacheEnabledFSSuite struct {
	suite.Suite
	ctx *utils.AppTestingContext
}

func (s *CacheEnabledFSSuite) SetupSuite() {
	ctx, err := utils.NewAppTestingContext(
		utils.WithBasicAuth("test", "test"),
		utils.WithRandomTestVolume(false),
		utils.WithCache(clientconfig.CacheConfig{
			Enabled:        true,
			MemoryMaxBytes: 1 << 30, // 1 GiB — large enough not to evict during tests
			DiskMaxBytes:   1 << 30, // 1 GiB
			ChunkSizeBytes: 1 << 20, // 1 MiB
			AttrTTL:        5 * time.Second,
			DirTTL:         5 * time.Second,
			NegativeTTL:    2 * time.Second,
		}),
	)
	s.Require().NoError(err)
	s.Require().NoError(ctx.Start())
	s.ctx = ctx
	// Safety net: a failed Require below skips TearDownSuite; Close is
	// idempotent, so this coexists with TearDownSuite's Close.
	s.T().Cleanup(func() { _ = ctx.Close() })
	s.Require().NoError(ctx.MountVolumeErr(ctx.GetVolumes()[0]))
}

func (s *CacheEnabledFSSuite) TearDownSuite() {
	if s.ctx != nil {
		_ = s.ctx.Close()
	}
}

func (s *CacheEnabledFSSuite) mountPath() string {
	return s.ctx.GetVolumes()[0].GetMountPath()
}

func (s *CacheEnabledFSSuite) TestWriteThenRead() {
	path := filepath.Join(s.mountPath(), "wr.bin")
	want := []byte("hello cache-on world")
	s.Require().NoError(os.WriteFile(path, want, 0o644))
	got, err := os.ReadFile(path)
	s.Require().NoError(err)
	s.Assert().Equal(want, got)
	// Read again — should hit the data cache; we can't observe hits
	// directly at the e2e layer, but identical bytes is the
	// correctness signal Sub-spec B is required to maintain.
	got2, err := os.ReadFile(path)
	s.Require().NoError(err)
	s.Assert().Equal(want, got2)
}

func (s *CacheEnabledFSSuite) TestWriteInvalidatesPriorRead() {
	path := filepath.Join(s.mountPath(), "inv.bin")
	s.Require().NoError(os.WriteFile(path, []byte("v1"), 0o644))
	v1, err := os.ReadFile(path)
	s.Require().NoError(err)
	s.Require().Equal([]byte("v1"), v1)
	s.Require().NoError(os.WriteFile(path, []byte("v2-different"), 0o644))
	v2, err := os.ReadFile(path)
	s.Require().NoError(err)
	s.Assert().Equal([]byte("v2-different"), v2, "Write must invalidate prior cached Read")
}

func (s *CacheEnabledFSSuite) TestMkdirThenListDirShowsChild() {
	dir := fmt.Sprintf("d-%d", time.Now().UnixNano())
	s.Require().NoError(os.Mkdir(filepath.Join(s.mountPath(), dir), 0o755))
	entries, err := os.ReadDir(s.mountPath())
	s.Require().NoError(err)
	found := false
	for _, e := range entries {
		if e.Name() == dir {
			found = true
			break
		}
	}
	s.Assert().True(found, "Mkdir must invalidate the parent dir cache so the new child appears")
}

func (s *CacheEnabledFSSuite) TestUnlinkInvalidatesNegativeAttr() {
	path := filepath.Join(s.mountPath(), fmt.Sprintf("ephem-%d.bin", time.Now().UnixNano()))
	s.Require().NoError(os.WriteFile(path, []byte("x"), 0o644))
	_, err := os.Stat(path)
	s.Require().NoError(err)
	s.Require().NoError(os.Remove(path))
	_, err = os.Stat(path)
	s.Assert().True(os.IsNotExist(err), "after Unlink, Stat must surface ENOENT not stale OK")
}

func (s *CacheEnabledFSSuite) TestRenameOldPathDisappears() {
	// Two invariants under test:
	//   1. After Rename(a, b), the source name surfaces ENOENT — the
	//      cached attr for `a` must be invalidated, not served stale.
	//   2. The destination name is fully usable: Stat returns the moved
	//      file's attrs and Read returns its bytes. Regression guard for
	//      the relPath-stale-after-MvChild bug fixed alongside this test:
	//      the node adapter used to cache relPath in the gMountieNode
	//      struct, but go-fuse's bridge moves the inode under the new
	//      name via MvChild without notifying our struct, so subsequent
	//      ops on the moved inode hit the OLD path on the server.
	a := filepath.Join(s.mountPath(), fmt.Sprintf("rn-a-%d.bin", time.Now().UnixNano()))
	b := filepath.Join(s.mountPath(), fmt.Sprintf("rn-b-%d.bin", time.Now().UnixNano()))
	body := []byte("rename-body")
	s.Require().NoError(os.WriteFile(a, body, 0o644))
	_, err := os.Stat(a) // populate cache for a
	s.Require().NoError(err)
	s.Require().NoError(os.Rename(a, b))
	_, err = os.Stat(a)
	s.Assert().True(os.IsNotExist(err), "after Rename, source name must surface ENOENT (cached attr invalidated)")
	fi, err := os.Stat(b)
	s.Require().NoError(err, "after Rename, destination name must be statable")
	s.Assert().Equal(int64(len(body)), fi.Size(), "destination Stat must report the moved file's size")
	got, err := os.ReadFile(b)
	s.Require().NoError(err, "after Rename, destination name must be readable")
	s.Assert().Equal(body, got, "destination Read must return the moved file's bytes")
}

func (s *CacheEnabledFSSuite) TestRecreateAfterUnlinkInvalidatesNegative() {
	// Unlink populates a negative-attr cache. Recreating the same name
	// must drop that negative entry so Stat returns OK, not ENOENT.
	path := filepath.Join(s.mountPath(), fmt.Sprintf("recreate-%d.bin", time.Now().UnixNano()))
	s.Require().NoError(os.WriteFile(path, []byte("first"), 0o644))
	s.Require().NoError(os.Remove(path))
	_, err := os.Stat(path)
	s.Require().True(os.IsNotExist(err))
	// Recreate
	s.Require().NoError(os.WriteFile(path, []byte("second"), 0o644))
	got, err := os.ReadFile(path)
	s.Require().NoError(err)
	s.Assert().Equal([]byte("second"), got, "after Create, Stat must see the new file (negative cache dropped)")
}

// TestMultiChunkReadRoundTrip writes a payload larger than ChunkSizeBytes
// (3.5 MiB against a 1 MiB chunk size, so 4 chunks of which the last is
// short) and reads it back through the mount. Regression guard for the
// Read-truncates-at-chunk-boundary bug fixed in af59a86: the bug caused
// any multi-chunk Read to return only the first chunk's worth of bytes,
// which the existing short-payload tests didn't catch.
func (s *CacheEnabledFSSuite) TestMultiChunkReadRoundTrip() {
	const payloadSize = 3*(1<<20) + 512*1024 // 3.5 MiB - spans 4 chunks
	payload := make([]byte, payloadSize)
	if _, err := io.ReadFull(rand.New(rand.NewSource(42)), payload); err != nil {
		s.Require().NoError(err)
	}
	wantSum := sha256.Sum256(payload)

	path := filepath.Join(s.mountPath(), fmt.Sprintf("mc-%d.bin", time.Now().UnixNano()))
	s.Require().NoError(os.WriteFile(path, payload, 0o644))

	// First read: misses on every chunk, populates cache.
	got, err := os.ReadFile(path)
	s.Require().NoError(err)
	s.Assert().Len(got, payloadSize, "first read must return full payload (not truncated to chunk 0)")
	gotSum := sha256.Sum256(got)
	s.Assert().Equal(wantSum, gotSum, "first read SHA-256 must match")

	// Second read: hits every chunk in cache; same multi-chunk traversal
	// must still return the full payload.
	got2, err := os.ReadFile(path)
	s.Require().NoError(err)
	s.Assert().Len(got2, payloadSize, "second read (all chunks cached) must return full payload")
	got2Sum := sha256.Sum256(got2)
	s.Assert().Equal(wantSum, got2Sum, "second read SHA-256 must match")
}

func TestCacheEnabledFSSuite(t *testing.T) {
	suite.Run(t, new(CacheEnabledFSSuite))
}

// CacheDisabledFSSuite is a no-cache control that uses the same fixture
// pattern without WithCache. Confirms the cache-on suite isn't masking a
// base failure shared with cache-off.
type CacheDisabledFSSuite struct {
	suite.Suite
	ctx *utils.AppTestingContext
}

func (s *CacheDisabledFSSuite) SetupSuite() {
	ctx, err := utils.NewAppTestingContext(
		utils.WithBasicAuth("test", "test"),
		utils.WithRandomTestVolume(false),
	)
	s.Require().NoError(err)
	s.Require().NoError(ctx.Start())
	s.ctx = ctx
	// Safety net: a failed Require below skips TearDownSuite; Close is
	// idempotent, so this coexists with TearDownSuite's Close.
	s.T().Cleanup(func() { _ = ctx.Close() })
	s.Require().NoError(ctx.MountVolumeErr(ctx.GetVolumes()[0]))
}

func (s *CacheDisabledFSSuite) TearDownSuite() {
	if s.ctx != nil {
		_ = s.ctx.Close()
	}
}

func (s *CacheDisabledFSSuite) TestSanityWriteReadRename() {
	mp := s.ctx.GetVolumes()[0].GetMountPath()
	path := filepath.Join(mp, fmt.Sprintf("sanity-%d.bin", time.Now().UnixNano()))
	s.Require().NoError(os.WriteFile(path, []byte("x"), 0o644))
	got, err := os.ReadFile(path)
	s.Require().NoError(err)
	s.Require().Equal("x", string(got))

	// Rename round-trip: the underlying node-adapter relPath bug
	// reproduces with the cache disabled, so this guard belongs in the
	// no-cache control suite too. Without it a future regression could
	// be masked by anyone who only runs the cache-enabled suite.
	a := filepath.Join(mp, fmt.Sprintf("rn-a-%d.bin", time.Now().UnixNano()))
	b := filepath.Join(mp, fmt.Sprintf("rn-b-%d.bin", time.Now().UnixNano()))
	body := []byte("rename-body-nocache")
	s.Require().NoError(os.WriteFile(a, body, 0o644))
	s.Require().NoError(os.Rename(a, b))
	gotB, err := os.ReadFile(b)
	s.Require().NoError(err, "read b after rename")
	s.Require().Equal(body, gotB, "rename must preserve content")
}

func TestCacheDisabledFSSuite(t *testing.T) {
	suite.Run(t, new(CacheDisabledFSSuite))
}
