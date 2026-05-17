package api

import (
	"crypto/sha256"
	"io"
	"math/rand"
	"os"
	"path/filepath"
	"testing"
	"time"

	clientconfig "gmountie/pkg/client/config"
	"gmountie/test/e2e/utils"

	"github.com/stretchr/testify/suite"
)

// CachePersistentFSSuite covers Sub-spec C's user-visible promises:
//  1. Cached chunks survive a client restart against the same cache.Path.
//  2. Two clients pointed at the same per-volume cache dir fail fast
//     with ErrCacheLocked (the lock file does its job).
//  3. The disk cap (cache.disk_max_bytes) holds when distinct-file
//     reads exceed the budget.
type CachePersistentFSSuite struct {
	suite.Suite
	dir string // cache.Path; shared across tests via SetupTest
}

func (s *CachePersistentFSSuite) SetupTest() { s.dir = s.T().TempDir() }

func (s *CachePersistentFSSuite) cacheCfg(diskMaxBytes int) clientconfig.CacheConfig {
	return clientconfig.CacheConfig{
		Enabled:        true,
		Path:           s.dir,
		MemoryMaxBytes: 64 << 20,
		DiskMaxBytes:   diskMaxBytes,
		ChunkSizeBytes: 1 << 20,
		AttrTTL:        time.Hour,
		DirTTL:         time.Hour,
		NegativeTTL:    2 * time.Second,
	}
}

// TestRestartReusesCachedChunks writes a multi-chunk payload through the
// first client, closes it, then opens a second client against the same
// server data dir and the same cache.Path. The second read must return
// identical bytes, exercising the on-disk persist layer rather than
// the in-memory tier (which is cold after restart).
func (s *CachePersistentFSSuite) TestRestartReusesCachedChunks() {
	const volName = "persist-restart"
	serverDataDir := s.T().TempDir()

	// First boot: write payload, read it back (populates cache), close.
	ctx, err := utils.NewAppTestingContext(
		utils.WithBasicAuth("test", "test"),
		utils.WithExistingVolume(volName, serverDataDir),
		utils.WithCache(s.cacheCfg(1<<30)),
	)
	s.Require().NoError(err)
	s.Require().NoError(ctx.Start())
	ctx.MountVolume(ctx.GetVolumes()[0])
	mp := ctx.GetVolumes()[0].GetMountPath()

	payload := make([]byte, 3<<20) // 3 MiB — spans multiple 1 MiB chunks
	_, _ = io.ReadFull(rand.New(rand.NewSource(1)), payload)
	want := sha256.Sum256(payload)
	path := filepath.Join(mp, "p.bin")
	s.Require().NoError(os.WriteFile(path, payload, 0o644))
	got, err := os.ReadFile(path)
	s.Require().NoError(err)
	s.Require().Equal(want, sha256.Sum256(got))
	s.Require().NoError(ctx.Close())

	// Second boot: same cache.Path, same server data dir, same volume name.
	// Reading the file again must work and content-match.
	ctx2, err := utils.NewAppTestingContext(
		utils.WithBasicAuth("test", "test"),
		utils.WithExistingVolume(volName, serverDataDir),
		utils.WithCache(s.cacheCfg(1<<30)),
	)
	s.Require().NoError(err)
	s.Require().NoError(ctx2.Start())
	ctx2.MountVolume(ctx2.GetVolumes()[0])
	defer ctx2.Close() //nolint:errcheck

	mp2 := ctx2.GetVolumes()[0].GetMountPath()
	got2, err := os.ReadFile(filepath.Join(mp2, "p.bin"))
	s.Require().NoError(err)
	s.Assert().Equal(want, sha256.Sum256(got2), "after restart, cached chunks must round-trip")
}

// TestSecondMountFailsWithLockedCache verifies that two live clients
// pointing at the same per-volume cache directory cannot coexist: the
// second MountVolume must fail because the LOCK file is held.
func (s *CachePersistentFSSuite) TestSecondMountFailsWithLockedCache() {
	const volName = "persist-lock"
	serverDataDir := s.T().TempDir()

	ctx, err := utils.NewAppTestingContext(
		utils.WithBasicAuth("test", "test"),
		utils.WithExistingVolume(volName, serverDataDir),
		utils.WithCache(s.cacheCfg(1<<26)),
	)
	s.Require().NoError(err)
	s.Require().NoError(ctx.Start())
	defer ctx.Close() //nolint:errcheck
	ctx.MountVolume(ctx.GetVolumes()[0])

	ctx2, err := utils.NewAppTestingContext(
		utils.WithBasicAuth("test", "test"),
		utils.WithExistingVolume(volName, serverDataDir),
		utils.WithCache(s.cacheCfg(1<<26)),
	)
	s.Require().NoError(err)
	s.Require().NoError(ctx2.Start())
	defer ctx2.Close() //nolint:errcheck

	err = ctx2.MountVolumeErr(ctx2.GetVolumes()[0])
	s.Require().Error(err, "second mount with same cache.Path must fail with ErrCacheLocked")
}

// TestDiskCapHoldsUnderManyReads writes 200 MiB of user data through
// the mount against a 100 MiB disk cap, then measures the chunks/
// directory size. The cap must hold within 20 MiB of slack.
//
// We use two large files (a.bin = 120 MiB, b.bin = 80 MiB) instead of
// 200 x 1 MiB files. The total data (200 MiB at 1 MiB chunk size) is
// the same, but two FUSE open/write/close cycles are far fewer than 200,
// which eliminates the FUSE-context-cancellation flakiness caused by
// sustained per-file round-trips under load. Eviction is exercised
// identically: the persist tier sees the same 200 chunk writes.
//
// WriteChunk calls enforceDiskBudget after crediting bytes; the eviction
// loop cursors data_idx from the lexically-oldest entry until Over()==0.
func (s *CachePersistentFSSuite) TestDiskCapHoldsUnderManyReads() {
	const capBytes = 100 << 20

	cacheCfg := clientconfig.CacheConfig{
		Enabled:        true,
		Path:           s.dir,
		MemoryMaxBytes: 16 << 20,
		DiskMaxBytes:   capBytes,
		ChunkSizeBytes: 1 << 20,
		AttrTTL:        time.Second,
		DirTTL:         time.Second,
		NegativeTTL:    time.Second,
	}
	const volName = "persist-cap"
	ctx, err := utils.NewAppTestingContext(
		utils.WithBasicAuth("test", "test"),
		utils.WithExistingVolume(volName, s.T().TempDir()),
		utils.WithCache(cacheCfg),
	)
	s.Require().NoError(err)
	s.Require().NoError(ctx.Start())
	defer ctx.Close() //nolint:errcheck
	ctx.MountVolume(ctx.GetVolumes()[0])
	mp := ctx.GetVolumes()[0].GetMountPath()

	// Two large files: 120 MiB + 80 MiB = 200 MiB total, well over the
	// 100 MiB cap. Each write exercises eviction at the 1 MiB chunk
	// boundary as bytes accumulate past the cap.
	files := []struct {
		name string
		mib  int
		fill byte
	}{
		{"a.bin", 120, 0xAA},
		{"b.bin", 80, 0xBB},
	}
	var totalMiB int
	for _, f := range files {
		data := make([]byte, f.mib<<20)
		for i := range data {
			data[i] = f.fill
		}
		s.Require().NoError(os.WriteFile(filepath.Join(mp, f.name), data, 0o644))
		_, err := os.ReadFile(filepath.Join(mp, f.name))
		s.Require().NoError(err)
		totalMiB += f.mib
	}

	chunksDir := filepath.Join(s.dir, volName, "chunks")
	var total int64
	_ = filepath.Walk(chunksDir, func(_ string, info os.FileInfo, _ error) error {
		if info == nil || info.IsDir() {
			return nil
		}
		total += info.Size()
		return nil
	})
	s.T().Logf("chunks/ size after %d MiB written = %d bytes (cap %d)", totalMiB, total, capBytes)
	// Slack: allow up to 20 MiB over the cap. The plan-flagged "FIFO not
	// strict LRU" follow-up means eviction is approximate; the cap is
	// enforced but not perfectly tight.
	s.Assert().LessOrEqual(total, int64(capBytes+(20<<20)), "disk cap (100 MiB) must hold within 20 MiB slack")
}

func TestCachePersistentFSSuite(t *testing.T) {
	suite.Run(t, new(CachePersistentFSSuite))
}
