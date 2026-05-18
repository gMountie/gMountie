package api

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	clientconfig "gmountie/pkg/client/config"
	"gmountie/test/e2e/utils"

	"github.com/stretchr/testify/suite"
)

// CacheSubscribeFSSuite covers Sub-spec D's user-visible promises:
//
//  1. Push invalidation: Client A writes → server bus emits MUTATED →
//     Client B (on the same server) sees new bytes within a few hundred ms,
//     even with AttrTTL=1h (proving Subscribe is doing the work, not TTL).
//
//  2. Restart revalidation with mutation: Client closes, server file is
//     overwritten directly, Client restarts with same cache.Path. The
//     unverified cache state on restart forces GetAttrIfChanged; the
//     version mismatch invalidates stale chunks so new bytes are served.
//
//  3. Deleted-while-offline surfaces ENOENT: same restart shape. File
//     deleted between close and restart; first Stat after restart returns
//     ENOENT via the revalidation path.
//
//  4. Subscribe disabled falls back to TTL: SubscribeEnabled=false marks
//     the cache globally verified at construction. External server-side
//     attr changes are not seen until AttrTTL expires; after expiry a
//     Stat returns fresh metadata.
//
// Two-clients-one-server approach: TestPushInvalidatesAcrossClients uses
// AppTestingContext.NewSiblingClient, which creates a second gRPC
// connection to the same bufconn listener. Both clients share the single
// server process and its event bus, so Subscribe push events emitted by
// Client A's writes flow directly to Client B's subscriber goroutine.
// Two separate AppTestingContext instances cannot achieve this because
// each spawns its own server with an independent event bus.
type CacheSubscribeFSSuite struct {
	suite.Suite
	serverDataDir string
}

func (s *CacheSubscribeFSSuite) SetupTest() { s.serverDataDir = s.T().TempDir() }

// cacheCfg returns a cache config with Subscribe enabled and large TTLs
// so that any invalidation observed by the test is push-driven, not
// TTL-driven.
func (s *CacheSubscribeFSSuite) cacheCfg(cachePath string) clientconfig.CacheConfig {
	return clientconfig.CacheConfig{
		Enabled:          true,
		SubscribeEnabled: true,
		Path:             cachePath,
		MemoryMaxBytes:   64 << 20,
		DiskMaxBytes:     1 << 30,
		ChunkSizeBytes:   1 << 20,
		AttrTTL:          time.Hour,
		DirTTL:           time.Hour,
		NegativeTTL:      time.Second,
	}
}

// TestPushInvalidatesAcrossClients demonstrates that Subscribe push
// events flow from a writing client through the server bus to a second
// independent client on the same server. AttrTTL is set to 1 hour so
// only a push event can produce the invalidation within the test window.
//
// Sequence:
//  1. Start server, mount volume for client A.
//  2. Create a sibling client B sharing the same bufconn (same bus).
//  3. Client B reads the file first → cold miss, populates attr+data cache.
//  4. Client A overwrites the file through FUSE → server emits MUTATED.
//  5. Client B's subscriber goroutine receives MUTATED, invalidates cache.
//  6. Within 5 s, Client B reads the file and sees the new bytes.
func (s *CacheSubscribeFSSuite) TestPushInvalidatesAcrossClients() {
	const volName = "sub-push"
	cacheDir := s.T().TempDir()

	// Client A: the writer. Large TTL ensures it never triggers TTL
	// revalidation during the test window.
	ctx, err := utils.NewAppTestingContext(
		utils.WithBasicAuth("test", "test"),
		utils.WithExistingVolume(volName, s.serverDataDir),
		utils.WithCache(s.cacheCfg(filepath.Join(cacheDir, "a"))),
		// 60 s heartbeat: prevents the heartbeat from racing the first
		// Stat on Client B and flipping stateVerified before we observe
		// push-based invalidation.
		utils.WithHeartbeatInterval(60*time.Second),
	)
	s.Require().NoError(err)
	s.Require().NoError(ctx.Start())
	defer ctx.Close() //nolint:errcheck
	ctx.MountVolume(ctx.GetVolumes()[0])
	mpA := ctx.GetVolumes()[0].GetMountPath()

	// Seed the file through Client A.
	filePathA := filepath.Join(mpA, "push.txt")
	s.Require().NoError(os.WriteFile(filePathA, []byte("v1"), 0o644))

	// Client B: independent sibling that connects to the same server.
	// Separate cache directory so it doesn't conflict with A's lock.
	sibCacheCfg := s.cacheCfg(filepath.Join(cacheDir, "b"))
	sibCtx, err := ctx.NewSiblingClient(&sibCacheCfg)
	s.Require().NoError(err)
	defer sibCtx.Close() //nolint:errcheck

	// Mount a separate TestVolume for B (different mount path, same srcPath).
	volB, err := utils.NewTestVolumeWithExistingSrc(volName, s.serverDataDir)
	s.Require().NoError(err)
	s.Require().NoError(sibCtx.SingleVolumeMounter.Mount(volName, volB.GetMountPath()))
	defer sibCtx.SingleVolumeMounter.Unmount(volName) //nolint:errcheck
	mpB := volB.GetMountPath()

	// Step 3: Client B reads the file → cold miss, caches "v1".
	gotV1, err := os.ReadFile(filepath.Join(mpB, "push.txt"))
	s.Require().NoError(err)
	s.Require().Equal([]byte("v1"), gotV1, "initial read through B must return v1")

	// Step 4: Client A overwrites via FUSE → server emits MUTATED on the bus.
	s.Require().NoError(os.WriteFile(filePathA, []byte("v2-pushed"), 0o644))

	// Step 5+6: Wait up to 5 s for Client B to see v2. The push path is
	// async (server emit → bus → gRPC stream → consumer goroutine →
	// invalidate), so we poll rather than using a fixed sleep.
	s.Require().Eventually(func() bool {
		got, err := os.ReadFile(filepath.Join(mpB, "push.txt"))
		return err == nil && string(got) == "v2-pushed"
	}, 5*time.Second, 50*time.Millisecond,
		"Client B must see v2 via Subscribe push within 5 s (AttrTTL=1h proves TTL is not responsible)")
}

// TestRestartRevalidatesAfterMutation verifies that on restart with an
// unverified cache (stateUnverified until first heartbeat), a file
// mutated externally between close and restart is correctly re-fetched.
//
// This is the discriminating variant of the revalidation path: the cached
// attr version mismatches the server version, so revalidate() invalidates
// the data chunks and the subsequent Read fetches fresh bytes from the
// server. Without revalidation the test would return stale v1 bytes.
//
// Heartbeat is set to 60 s so the subscriber goroutine stays in
// stateUnverified for the duration of the test, guaranteeing that the
// first Stat triggers GetAttrIfChanged rather than the fast verified path.
func (s *CacheSubscribeFSSuite) TestRestartRevalidatesAfterMutation() {
	const volName = "sub-revalidate"
	cacheDir := s.T().TempDir()

	// First boot: write v1, read to populate cache, close.
	ctx, err := utils.NewAppTestingContext(
		utils.WithBasicAuth("test", "test"),
		utils.WithExistingVolume(volName, s.serverDataDir),
		utils.WithCache(s.cacheCfg(cacheDir)),
		utils.WithHeartbeatInterval(60*time.Second),
	)
	s.Require().NoError(err)
	s.Require().NoError(ctx.Start())
	ctx.MountVolume(ctx.GetVolumes()[0])
	mp := ctx.GetVolumes()[0].GetMountPath()

	v1 := []byte("v1-original-bytes")
	s.Require().NoError(os.WriteFile(filepath.Join(mp, "rv.txt"), v1, 0o644))
	got1, err := os.ReadFile(filepath.Join(mp, "rv.txt"))
	s.Require().NoError(err)
	s.Require().Equal(v1, got1, "setup: v1 read-back must match")
	s.Require().NoError(ctx.Close())

	// Between close and restart: overwrite the file directly in srcDir.
	// This bypasses the gRPC path entirely, so the server bus never
	// emitted MUTATED; the cache still has v1 chunks on disk.
	v2 := []byte("v2-mutated-while-offline")
	s.Require().NoError(os.WriteFile(filepath.Join(s.serverDataDir, "rv.txt"), v2, 0o644))

	// Second boot: same cache.Path, same volume name. Memory is cold;
	// persist tier has v1 chunks. Heartbeat is 60 s so we stay unverified.
	// The first Stat on "rv.txt" triggers revalidate → GetAttrIfChanged.
	// The server's attr version advanced when we wrote v2 directly (the
	// loopback filesystem updates mtime/ctime). Version mismatch causes
	// revalidate() to call data.invalidatePath → chunk miss → fresh fetch.
	ctx2, err := utils.NewAppTestingContext(
		utils.WithBasicAuth("test", "test"),
		utils.WithExistingVolume(volName, s.serverDataDir),
		utils.WithCache(s.cacheCfg(cacheDir)),
		utils.WithHeartbeatInterval(60*time.Second),
	)
	s.Require().NoError(err)
	s.Require().NoError(ctx2.Start())
	ctx2.MountVolume(ctx2.GetVolumes()[0])
	defer ctx2.Close() //nolint:errcheck

	mp2 := ctx2.GetVolumes()[0].GetMountPath()
	got2, err := os.ReadFile(filepath.Join(mp2, "rv.txt"))
	s.Require().NoError(err)
	s.Assert().Equal(v2, got2,
		"after restart with mutated file, cached stale chunks must be replaced by fresh bytes via GetAttrIfChanged revalidation")
}

// TestDeletedWhileOfflineSurfacesENOENT exercises the deletion branch of
// revalidation: a file cached positively before shutdown is deleted from
// the server's backing directory while the client is down. On restart the
// client starts unverified; the first Stat triggers revalidate →
// GetAttrIfChanged → ENOENT path → putNegative → surfaces ENOENT to the
// caller, not the stale cached OK.
//
// Heartbeat is set to 60 s to prevent stateVerified from flipping before
// the first Stat. If the heartbeat arrived first, Stat would fast-path
// through the cached positive attr and return OK — a false PASS that
// hides the bug. 60 s gives the test ample time without risking a race.
func (s *CacheSubscribeFSSuite) TestDeletedWhileOfflineSurfacesENOENT() {
	const volName = "sub-enoent"
	cacheDir := s.T().TempDir()

	// First boot: write and cache the file, then close.
	ctx, err := utils.NewAppTestingContext(
		utils.WithBasicAuth("test", "test"),
		utils.WithExistingVolume(volName, s.serverDataDir),
		utils.WithCache(s.cacheCfg(cacheDir)),
		utils.WithHeartbeatInterval(60*time.Second),
	)
	s.Require().NoError(err)
	s.Require().NoError(ctx.Start())
	ctx.MountVolume(ctx.GetVolumes()[0])
	mp := ctx.GetVolumes()[0].GetMountPath()

	s.Require().NoError(os.WriteFile(filepath.Join(mp, "gone.txt"), []byte("present"), 0o644))
	_, err = os.Stat(filepath.Join(mp, "gone.txt"))
	s.Require().NoError(err, "setup: file must be stat-able before close")
	s.Require().NoError(ctx.Close())

	// Between close and restart: delete the file directly in srcDir.
	s.Require().NoError(os.Remove(filepath.Join(s.serverDataDir, "gone.txt")))

	// Second boot: same cache.Path, same volume name. Persist tier has a
	// positive attr entry for "gone.txt". Start unverified (heartbeat=60s).
	ctx2, err := utils.NewAppTestingContext(
		utils.WithBasicAuth("test", "test"),
		utils.WithExistingVolume(volName, s.serverDataDir),
		utils.WithCache(s.cacheCfg(cacheDir)),
		utils.WithHeartbeatInterval(60*time.Second),
	)
	s.Require().NoError(err)
	s.Require().NoError(ctx2.Start())
	ctx2.MountVolume(ctx2.GetVolumes()[0])
	defer ctx2.Close() //nolint:errcheck

	mp2 := ctx2.GetVolumes()[0].GetMountPath()
	_, err = os.Stat(filepath.Join(mp2, "gone.txt"))
	s.Assert().True(os.IsNotExist(err),
		"after restart, a file deleted while offline must surface ENOENT (not stale cached OK)")
}

// TestSubscribeDisabledFallsBackToTTL verifies that with SubscribeEnabled=false
// the cache is marked globally verified at construction (no subscriber
// goroutine is started). Within AttrTTL, a Stat on a known file returns
// cached attrs without going to the server. After AttrTTL expires, a Stat
// fetches fresh metadata from the server.
//
// This tests attr-level TTL fallback. Note: data chunks have no TTL in
// any mode; sub-spec D does not guarantee that Read returns new file
// content after an external server-side mutation without Subscribe — only
// that Stat returns fresh metadata once AttrTTL expires. A subsequent
// Open+Read will then fetch new chunks because the attr version change
// invalidates data in the revalidation path (stateUnverified mode) or
// the user can force a fresh read by re-opening the file.
func (s *CacheSubscribeFSSuite) TestSubscribeDisabledFallsBackToTTL() {
	const volName = "sub-disabled"
	const attrTTL = 2 * time.Second
	cacheDir := s.T().TempDir()

	disabledCfg := clientconfig.CacheConfig{
		Enabled:          true,
		SubscribeEnabled: false, // TTL-only freshness
		Path:             cacheDir,
		MemoryMaxBytes:   64 << 20,
		DiskMaxBytes:     1 << 30,
		ChunkSizeBytes:   1 << 20,
		AttrTTL:          attrTTL,
		DirTTL:           time.Hour,
		NegativeTTL:      time.Second,
	}

	ctx, err := utils.NewAppTestingContext(
		utils.WithBasicAuth("test", "test"),
		utils.WithExistingVolume(volName, s.serverDataDir),
		utils.WithCache(disabledCfg),
	)
	s.Require().NoError(err)
	s.Require().NoError(ctx.Start())
	defer ctx.Close() //nolint:errcheck
	ctx.MountVolume(ctx.GetVolumes()[0])
	mp := ctx.GetVolumes()[0].GetMountPath()

	// Write a file through the mount so it's registered in the cache.
	s.Require().NoError(os.WriteFile(filepath.Join(mp, "ttl.txt"), []byte("v1"), 0o644))

	// Read it back; this populates the attr cache with a TTL=2s entry.
	fi1, err := os.Stat(filepath.Join(mp, "ttl.txt"))
	s.Require().NoError(err)
	origSize := fi1.Size()
	s.Require().EqualValues(len("v1"), origSize)

	// Overwrite the file directly in srcDir — this is NOT routed through
	// the gRPC backend, so the cache is not invalidated via Subscribe or
	// the backend's own mutate path.
	v2 := []byte("v2-longer-content")
	s.Require().NoError(os.WriteFile(filepath.Join(s.serverDataDir, "ttl.txt"), v2, 0o644))

	// Within TTL: Stat should still return the cached (old) size.
	fi2, err := os.Stat(filepath.Join(mp, "ttl.txt"))
	s.Require().NoError(err)
	s.Assert().Equal(origSize, fi2.Size(),
		"within AttrTTL, Stat must return cached attrs (subscribe disabled, TTL not yet expired)")

	// Wait for AttrTTL to expire.
	time.Sleep(attrTTL + 500*time.Millisecond)

	// After TTL expiry: the attr cache misses and Stat fetches fresh attrs.
	fi3, err := os.Stat(filepath.Join(mp, "ttl.txt"))
	s.Require().NoError(err)
	s.Assert().Equal(int64(len(v2)), fi3.Size(),
		"after AttrTTL expiry, Stat must return fresh metadata from the server")
}

func TestCacheSubscribeFSSuite(t *testing.T) {
	suite.Run(t, new(CacheSubscribeFSSuite))
}
