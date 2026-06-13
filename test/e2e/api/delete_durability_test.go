package api

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	clientconfig "go.gmountie.dev/gmountie/pkg/client/config"
	"go.gmountie.dev/gmountie/test/e2e/utils"

	"github.com/stretchr/testify/suite"
)

// DeleteDurabilitySuite is the regression guard for issue #113: a `rm -rf` that
// returned success must not reappear after an unmount + remount — specifically
// across a *busy* unmount, where an open fd forces the lazy (MNT_DETACH)
// teardown path the bug report implicated. The hypothesised loss was a stale
// persisted dir/attr cache being re-served on remount before the teardown flush
// landed; the cache layer now syncs invalidations immediately and serialises
// teardown↔remount via the cache LOCK, so this guard should PASS on master.
//
// It needs a real FUSE mount, so it runs in CI / the VM, not the sandbox.
type DeleteDurabilitySuite struct {
	suite.Suite
	cacheDir string // shared cache.Path across the two mounts
}

func (s *DeleteDurabilitySuite) SetupTest() { s.cacheDir = s.T().TempDir() }

func (s *DeleteDurabilitySuite) cacheCfg() clientconfig.CacheConfig {
	return clientconfig.CacheConfig{
		Enabled:        true,
		Path:           s.cacheDir,
		MemoryMaxBytes: 64 << 20,
		DiskMaxBytes:   1 << 30,
		ChunkSizeBytes: 1 << 20,
		AttrTTL:        time.Hour, // long TTLs make a stale-cache resurrection
		DirTTL:         time.Hour, // *more* likely, not less — a strict guard
		NegativeTTL:    2 * time.Second,
	}
}

// TestDeleteSurvivesBusyUnmountRemount creates a subtree, deletes it, tears the
// mount down through the busy/lazy path (an open fd on a sibling forces it),
// then remounts the same volume against the same on-disk cache and asserts the
// deleted subtree is gone — not resurrected from stale persisted cache.
func (s *DeleteDurabilitySuite) TestDeleteSurvivesBusyUnmountRemount() {
	const volName = "delete-durability"
	serverDataDir := s.T().TempDir()

	ctx, err := utils.NewAppTestingContext(
		utils.WithBasicAuth("test", "test"),
		utils.WithExistingVolume(volName, serverDataDir),
		utils.WithCache(s.cacheCfg()),
	)
	s.Require().NoError(err)
	s.Require().NoError(ctx.Start())
	defer ctx.Close() //nolint:errcheck
	s.Require().NoError(ctx.MountVolumeErr(ctx.GetVolumes()[0]))
	mp := ctx.GetVolumes()[0].GetMountPath()

	// Build a subtree with a big file (mirrors the report's probe/big.bin), and
	// a sibling we keep open across the unmount to force the busy/lazy path.
	probe := filepath.Join(mp, "probe")
	s.Require().NoError(os.MkdirAll(filepath.Join(probe, "deep"), 0o755))
	big := make([]byte, 50<<20)
	for i := range big {
		big[i] = byte(i)
	}
	s.Require().NoError(os.WriteFile(filepath.Join(probe, "big.bin"), big, 0o644))
	s.Require().NoError(os.WriteFile(filepath.Join(probe, "deep", "leaf.txt"), []byte("leaf"), 0o644))
	sibling := filepath.Join(mp, "keepopen.txt")
	s.Require().NoError(os.WriteFile(sibling, []byte("hold"), 0o644))

	// Populate the dir/attr cache so a stale entry would have something to
	// resurrect from: list the root and the subtree.
	entries, err := os.ReadDir(mp)
	s.Require().NoError(err)
	s.Require().True(containsName(entries, "probe"), "probe/ must exist before delete")
	_, err = os.ReadDir(probe)
	s.Require().NoError(err)

	// Hold an open fd on the sibling so the unmount can't release immediately
	// and falls back to the lazy (MNT_DETACH) teardown.
	held, err := os.Open(sibling)
	s.Require().NoError(err)

	// The delete that must stick. RemoveAll returning nil is the success the
	// report observed before the tree came back.
	s.Require().NoError(os.RemoveAll(probe))

	// Tear the mount down while the fd is still open (busy → lazy), then release
	// the fd so the teardown can finish and the cache LOCK drops.
	unmounted := make(chan error, 1)
	go func() { unmounted <- ctx.UnmountVolume(ctx.GetVolumes()[0]) }()
	time.Sleep(200 * time.Millisecond) // let the lazy detach engage before the fd closes
	s.Require().NoError(held.Close())
	select {
	case err := <-unmounted:
		s.Require().NoError(err)
	case <-time.After(30 * time.Second):
		s.FailNow("busy unmount did not complete after the fd closed")
	}

	// Remount the same volume against the same server data + same cache dir.
	s.Require().NoError(ctx.MountVolumeErr(ctx.GetVolumes()[0]))
	mp2 := ctx.GetVolumes()[0].GetMountPath()

	// The deleted subtree must stay gone — not resurrected from stale cache.
	_, statErr := os.Stat(filepath.Join(mp2, "probe"))
	s.Require().Truef(os.IsNotExist(statErr), "probe/ must remain deleted after remount, got: %v", statErr)
	entries2, err := os.ReadDir(mp2)
	s.Require().NoError(err)
	s.False(containsName(entries2, "probe"), "the deleted tree must not reappear in the parent listing")
}

func containsName(entries []os.DirEntry, name string) bool {
	for _, e := range entries {
		if e.Name() == name {
			return true
		}
	}
	return false
}

func TestDeleteDurabilitySuite(t *testing.T) {
	suite.Run(t, new(DeleteDurabilitySuite))
}
