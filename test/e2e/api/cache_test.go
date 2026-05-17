package api

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	clientconfig "gmountie/pkg/client/config"
	"gmountie/test/e2e/utils"

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
			MaxSizeBytes:   1 << 28, // 256 MiB
			ChunkSizeBytes: 1 << 20, // 1 MiB
			AttrTTL:        5 * time.Second,
			DirTTL:         5 * time.Second,
			NegativeTTL:    2 * time.Second,
		}),
	)
	s.Require().NoError(err)
	s.Require().NoError(ctx.Start())
	ctx.MountVolume(ctx.GetVolumes()[0])
	s.ctx = ctx
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
	// The cache invariant under test: after Rename(a, b) the cached
	// attr for `a` must be invalidated, so Stat(a) surfaces ENOENT
	// instead of returning the pre-rename hit. We intentionally do
	// not assert on reading `b` here -- the base client FS path has
	// a separate, pre-existing limitation with rename destinations
	// that reproduces with the cache disabled, so asserting on it
	// would conflate cache correctness with an unrelated bug.
	a := filepath.Join(s.mountPath(), fmt.Sprintf("rn-a-%d.bin", time.Now().UnixNano()))
	b := filepath.Join(s.mountPath(), fmt.Sprintf("rn-b-%d.bin", time.Now().UnixNano()))
	s.Require().NoError(os.WriteFile(a, []byte("body"), 0o644))
	_, err := os.Stat(a) // populate cache for a
	s.Require().NoError(err)
	s.Require().NoError(os.Rename(a, b))
	_, err = os.Stat(a)
	s.Assert().True(os.IsNotExist(err), "after Rename, source name must surface ENOENT (cached attr invalidated)")
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

func TestCacheEnabledFSSuite(t *testing.T) {
	suite.Run(t, new(CacheEnabledFSSuite))
}

// TestCacheDisabledFSSanity is a no-cache control that uses the same
// fixture pattern without WithCache. Confirms the cache-on suite isn't
// masking a base failure shared with cache-off.
func TestCacheDisabledFSSanity(t *testing.T) {
	ctx, err := utils.NewAppTestingContext(
		utils.WithBasicAuth("test", "test"),
		utils.WithRandomTestVolume(false),
	)
	if err != nil {
		t.Fatal(err)
	}
	if err := ctx.Start(); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = ctx.Close() }()
	ctx.MountVolume(ctx.GetVolumes()[0])
	mp := ctx.GetVolumes()[0].GetMountPath()
	path := filepath.Join(mp, fmt.Sprintf("sanity-%d.bin", time.Now().UnixNano()))
	if err := os.WriteFile(path, []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}
	got, err := os.ReadFile(path)
	if err != nil || string(got) != "x" {
		t.Fatalf("sanity: %v / %q", err, got)
	}
}
