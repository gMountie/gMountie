package fs

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"

	clientConfig "go.gmountie.dev/gmountie/pkg/client/config"
	"go.gmountie.dev/gmountie/pkg/proto"
	"go.gmountie.dev/gmountie/test/e2e/utils"

	"github.com/stretchr/testify/suite"
	"golang.org/x/sys/unix"
	"google.golang.org/grpc"
)

// XAttrCacheE2ESuite proves the xattr-name caching contract end-to-end
// against a real FUSE mount:
//
//  1. A directory listing (ReadDir) primes the xattr-name cache for every
//     entry that has xattrs listed, so subsequent per-file listxattr calls
//     issue ZERO ListXAttr RPCs to the server.
//
//  2. A local unix.Setxattr invalidates the cached names list for that
//     file, so the next listxattr reflects the new attribute.
//
// The suite runs in CI (which has /dev/fuse) and on the dedicated VM.
// It CANNOT run in a sandbox that has no /dev/fuse — the mount silently
// fails and the test is correctly skipped by the harness's mount-failure
// guard.
type XAttrCacheE2ESuite struct {
	suite.Suite
	testAppCtx     *utils.AppTestingContext
	volume         *utils.TestVolume
	listXattrCalls atomic.Int64 // incremented by the client unary interceptor
}

func TestXAttrCacheE2ESuite(t *testing.T) { suite.Run(t, new(XAttrCacheE2ESuite)) }

func (s *XAttrCacheE2ESuite) SetupSuite() {
	// Build the unary client interceptor BEFORE NewAppTestingContext so it is
	// passed in via WithClientUnaryInterceptors. The closure captures &s, which
	// testify keeps as the same pointer for all test methods.
	countingInterceptor := func(
		ctx context.Context,
		method string,
		req, reply any,
		cc *grpc.ClientConn,
		invoker grpc.UnaryInvoker,
		opts ...grpc.CallOption,
	) error {
		if method == proto.RpcFs_ListXAttr_FullMethodName {
			s.listXattrCalls.Add(1)
		}
		return invoker(ctx, method, req, reply, cc, opts...)
	}

	ctx, err := utils.NewAppTestingContext(
		utils.WithBasicAuth("test", "test"),
		utils.WithRandomTestVolume(false),
		utils.WithCache(clientConfig.CacheConfig{
			Enabled:          true,
			SubscribeEnabled: false, // TTL-only — deterministic; no Subscribe race
			MemoryMaxBytes:   clientConfig.DefaultCacheMemoryMaxBytes,
			DiskMaxBytes:     clientConfig.DefaultCacheDiskMaxBytes,
			ChunkSizeBytes:   clientConfig.DefaultCacheChunkSizeBytes,
			AttrTTL:          clientConfig.DefaultCacheAttrTTL,
			DirTTL:           clientConfig.DefaultCacheDirTTL,
			NegativeTTL:      clientConfig.DefaultCacheNegativeTTL,
			XAttrTTL:         clientConfig.DefaultCacheXAttrTTL,
			Path:             s.T().TempDir(), // per-suite cache root
		}),
		utils.WithClientUnaryInterceptors(countingInterceptor),
	)
	s.Require().NoError(err)
	utils.Must0(s.T(), ctx.Start())
	s.testAppCtx = ctx
	// Safety net: a failed Require below skips TearDownSuite; Close is
	// idempotent, so this coexists with TearDownSuite's Close.
	s.T().Cleanup(func() { _ = ctx.Close() })
	s.volume = ctx.GetVolumes()[0]
	s.Require().NotNil(s.volume)
	s.Require().NoError(ctx.MountVolumeErr(s.volume))
}

func (s *XAttrCacheE2ESuite) TearDownSuite() {
	if s.testAppCtx == nil {
		return
	}
	s.Require().NoError(s.testAppCtx.Close())
}

// TestReadDirPrimesXattr_NoPerFileListXAttrRPC creates a fresh subdirectory
// with 8 files (one tagged with a user.* xattr), issues os.ReadDir to prime
// the xattr-name cache, then calls unix.Listxattr on every entry and asserts
// that ZERO ListXAttr RPCs were sent to the server.
//
// The contract: readdirplus-with-xattr primes the client xattr cache for
// every entry, so the per-file listxattr is served locally. Any delta > 0
// means the prime is missing and the feature regressed.
func (s *XAttrCacheE2ESuite) TestReadDirPrimesXattr_NoPerFileListXAttrRPC() {
	mount := s.volume.GetMountPath()
	subdir := filepath.Join(mount, "xattr-prime-readdir")
	s.Require().NoError(os.Mkdir(subdir, 0o755))

	// Create 8 files. One of them gets a user.* xattr so it has a non-empty
	// names list — this is the load-bearing entry whose listxattr triggers the
	// sized second call and exercises the prime most directly.
	const nFiles = 8
	for i := 0; i < nFiles; i++ {
		p := filepath.Join(subdir, fmt.Sprintf("f%d", i))
		s.Require().NoError(os.WriteFile(p, []byte("x"), 0o644))
	}
	taggedPath := filepath.Join(subdir, "f0")
	if err := unix.Setxattr(taggedPath, "user.tag", []byte("v"), 0); err == unix.ENOTSUP {
		s.T().Skip("backing filesystem has no xattr support")
	} else {
		s.Require().NoError(err)
	}

	// Snapshot the counter AFTER the Setxattr above (which fires a SetXAttr
	// RPC, not a ListXAttr RPC) so the baseline is clean.
	before := s.listXattrCalls.Load()

	// Simulate `ls -la`: ReadDir primes the xattr cache; per-file Listxattr
	// must then be served entirely from that cache.
	ents, err := os.ReadDir(subdir)
	s.Require().NoError(err)

	for _, e := range ents {
		p := filepath.Join(subdir, e.Name())
		sz, _ := unix.Listxattr(p, nil)
		if sz > 0 {
			buf := make([]byte, sz)
			_, _ = unix.Listxattr(p, buf)
		}
	}

	after := s.listXattrCalls.Load()
	s.Equal(int64(0), after-before,
		"readdir must prime the xattr cache: no per-file ListXAttr RPC expected, got %d", after-before)
}

// TestSetXAttrInvalidatesCache creates a file, primes its (empty) xattr cache
// via listxattr, then sets a new attribute and asserts the next listxattr
// returns the new name.
//
// The contract: SetXAttr invalidates the cached names list so the re-list
// reflects the mutation. A stale cache would serve the old (empty) list and
// the new name would be absent — a clear test failure.
func (s *XAttrCacheE2ESuite) TestSetXAttrInvalidatesCache() {
	mount := s.volume.GetMountPath()
	p := filepath.Join(mount, "xattr-inval.txt")
	s.Require().NoError(os.WriteFile(p, []byte("x"), 0o644))

	// Prime the cache with the current (empty) list.
	_, _ = unix.Listxattr(p, nil)

	// Set a new attribute — should invalidate the cached names list.
	if err := unix.Setxattr(p, "user.new", []byte("v"), 0); err == unix.ENOTSUP {
		s.T().Skip("backing filesystem has no xattr support")
	} else {
		s.Require().NoError(err)
	}

	// After invalidation the re-list must reach the server (or the updated
	// local cache) and reflect the new name.
	sz, err := unix.Listxattr(p, nil)
	s.Require().NoError(err)
	s.Require().Greater(sz, 0, "listxattr after setxattr must return at least one name")

	buf := make([]byte, sz)
	n, err := unix.Listxattr(p, buf)
	s.Require().NoError(err)
	s.Contains(string(buf[:n]), "user.new",
		"new xattr name must appear in listxattr result after cache invalidation")
}
