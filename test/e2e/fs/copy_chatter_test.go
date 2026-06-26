package fs

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"testing"

	clientConfig "go.gmountie.dev/gmountie/pkg/client/config"
	"go.gmountie.dev/gmountie/pkg/proto"
	"go.gmountie.dev/gmountie/test/e2e/utils"

	"github.com/stretchr/testify/suite"
	"golang.org/x/sys/unix"
	"google.golang.org/grpc"
)

// CopyChatterSuite measures the GetAttr / GetXAttr RPC chatter a real `cp -a`
// copy-IN generates through a cached FUSE mount. It is a MEASUREMENT harness,
// not a pass/fail assertion: it runs a real copy of a node_modules-shaped tree
// into the mount and prints the post-cache RPCs bucketed by method, by xattr
// attribute name (GetXAttr), and by path (GetAttr — distinct vs re-queried).
//
// The buckets answer the investigation directly:
//   - GetXAttr by name → is the flood security.capability (killpriv regression),
//     system.posix_acl_*, security.selinux (never primed), or com.apple.*?
//   - GetAttr by path → same path re-queried (kernel attr-timeout too short, a
//     coherence tradeoff) vs distinct paths each missing once (a priming gap).
//
// Gated behind GMOUNTIE_CHATTER_MEASURE so it does not slow the normal fs suite
// (it copies thousands of files). Needs /dev/fuse — runs on the dedicated VM.
type CopyChatterSuite struct {
	suite.Suite
	appCtx *utils.AppTestingContext
	volume *utils.TestVolume
	rpc    *rpcCounter
}

func TestCopyChatterSuite(t *testing.T) {
	if os.Getenv("GMOUNTIE_CHATTER_MEASURE") == "" {
		t.Skip("set GMOUNTIE_CHATTER_MEASURE=1 to run the copy-chatter measurement")
	}
	suite.Run(t, new(CopyChatterSuite))
}

func (s *CopyChatterSuite) SetupSuite() {
	s.rpc = newRPCCounter()
	ctx, err := utils.NewAppTestingContext(
		utils.WithBasicAuth("test", "test"),
		utils.WithRandomTestVolume(false), // empty volume; we copy INTO it
		utils.WithCache(clientConfig.CacheConfig{
			Enabled:          true,
			SubscribeEnabled: true, // production-faithful: invalidation stream on
			MemoryMaxBytes:   clientConfig.DefaultCacheMemoryMaxBytes,
			DiskMaxBytes:     clientConfig.DefaultCacheDiskMaxBytes,
			ChunkSizeBytes:   clientConfig.DefaultCacheChunkSizeBytes,
			AttrTTL:          clientConfig.DefaultCacheAttrTTL,
			DirTTL:           clientConfig.DefaultCacheDirTTL,
			NegativeTTL:      clientConfig.DefaultCacheNegativeTTL,
			XAttrTTL:         clientConfig.DefaultCacheXAttrTTL,
			Path:             s.T().TempDir(),
		}),
		utils.WithClientUnaryInterceptors(s.rpc.unary),
		utils.WithClientStreamInterceptors(s.rpc.stream),
	)
	s.Require().NoError(err)
	utils.Must0(s.T(), ctx.Start())
	s.appCtx = ctx
	s.T().Cleanup(func() { _ = ctx.Close() })
	s.volume = ctx.GetVolumes()[0]
	s.Require().NotNil(s.volume)
	s.Require().NoError(ctx.MountVolumeErr(s.volume))
}

func (s *CopyChatterSuite) TearDownSuite() {
	if s.appCtx != nil {
		s.Require().NoError(s.appCtx.Close())
	}
}

// TestCopyInChatter generates a node_modules-shaped source tree on local disk,
// resets the RPC counter, runs a real `cp -a` of it into the mount, then prints
// the chatter buckets.
func (s *CopyChatterSuite) TestCopyInChatter() {
	mount := s.volume.GetMountPath()

	src := s.T().TempDir()
	nFiles, nDirs := generateNodeModulesTree(s.T(), src)
	s.T().Logf("source tree: %d files across %d dirs", nFiles, nDirs)

	dest := filepath.Join(mount, "studioennea")

	// Reset right before the copy so the buckets reflect ONLY the cp.
	s.rpc.reset()

	out, err := exec.Command("cp", "-a", src, dest).CombinedOutput()
	s.Require().NoError(err, "cp -a failed: %s", string(out))

	s.rpc.report(s.T(), nFiles, nDirs)
	// Load-bearing invariant: the kernel's per-file security.capability getxattr
	// probe must be fully absorbed by the Create-time negative prime + killpriv,
	// so a bulk copy issues ZERO GetXAttr RPCs. A regression here re-introduces a
	// per-file WAN round-trip.
	s.Zero(s.rpc.count("GetXAttr"), "bulk cp must issue zero GetXAttr RPCs (killpriv probe absorbed)")
}

// TestNpmInstallChatter runs a real `npm install` of a representative
// dependency set INSIDE the mount and reports the chatter. This is the phase
// the user actually saw the GetAttr/GetXAttr flood in — npm's tree-build does
// far more stat/lstat/readdir/getxattr than a plain cp. Requires `npm` on PATH
// and network access to the registry; skips cleanly if either is missing.
func (s *CopyChatterSuite) TestNpmInstallChatter() {
	if _, err := exec.LookPath("npm"); err != nil {
		s.T().Skip("npm not on PATH; skipping npm-install chatter measurement")
	}
	mount := s.volume.GetMountPath()
	proj := filepath.Join(mount, "npmproj")
	s.Require().NoError(os.Mkdir(proj, 0o755))
	pkgJSON := `{"name":"chatter","version":"1.0.0","private":true,` +
		`"dependencies":{"express":"^4.21.0","lodash":"^4.17.21"}}`
	s.Require().NoError(os.WriteFile(filepath.Join(proj, "package.json"), []byte(pkgJSON), 0o644))

	// Reset right before the install so buckets reflect only npm's work.
	s.rpc.reset()

	cmd := exec.Command("npm", "install", "--no-audit", "--no-fund", "--loglevel=error")
	cmd.Dir = proj
	out, err := cmd.CombinedOutput()
	s.Require().NoError(err, "npm install failed: %s", string(out))

	// Count installed entries for per-entry ratios.
	files, dirs := countTree(s.T(), filepath.Join(proj, "node_modules"))
	s.T().Logf("npm install produced %d files / %d dirs under node_modules", files, dirs)
	s.rpc.report(s.T(), files, dirs)
	// Same invariant as the cp path: npm install's per-file killpriv getxattr
	// probe must be fully absorbed — zero GetXAttr RPCs.
	s.Zero(s.rpc.count("GetXAttr"), "npm install must issue zero GetXAttr RPCs (killpriv probe absorbed)")
}

// countTree walks root and returns (files, dirs). Used only for reporting
// per-entry ratios; a walk failure is non-fatal (best-effort denominator).
func countTree(t *testing.T, root string) (files, dirs int) {
	t.Helper()
	_ = filepath.WalkDir(root, func(_ string, d os.DirEntry, err error) error {
		if err != nil || d == nil {
			return nil //nolint:nilerr // best-effort denominator: skip unreadable entries, keep walking
		}
		if d.IsDir() {
			dirs++
		} else {
			files++
		}
		return nil
	})
	return files, dirs
}

// generateNodeModulesTree writes a tree shaped like an npm node_modules dir:
// many packages, each with nested dirs and small files, a package.json carrying
// a user.* xattr (so `cp -a` exercises the xattr-copy path), and a couple of
// dot-files. Returns (files, dirs) created. Sized to surface per-file chatter
// without filling a small VM disk (~a few MiB).
func generateNodeModulesTree(t *testing.T, root string) (files, dirs int) {
	t.Helper()
	const (
		packages    = 60
		subdirsEach = 4
		filesPerDir = 6
	)
	small := []byte(strings.Repeat("x", 512))
	mkdir := func(p string) {
		if err := os.MkdirAll(p, 0o755); err != nil {
			t.Fatalf("mkdir %s: %v", p, err)
		}
		dirs++
	}
	write := func(p string, data []byte) {
		if err := os.WriteFile(p, data, 0o644); err != nil {
			t.Fatalf("write %s: %v", p, err)
		}
		files++
	}
	nm := filepath.Join(root, "node_modules")
	mkdir(nm)
	for p := 0; p < packages; p++ {
		pkg := filepath.Join(nm, fmt.Sprintf("pkg%02d", p))
		mkdir(pkg)
		// package.json with a user xattr — cp -a will copy the xattr.
		pj := filepath.Join(pkg, "package.json")
		write(pj, []byte(fmt.Sprintf(`{"name":"pkg%02d","version":"1.0.0"}`, p)))
		if err := unix.Setxattr(pj, "user.npm", []byte("meta"), 0); err != nil && err != unix.ENOTSUP {
			t.Fatalf("setxattr %s: %v", pj, err)
		}
		write(filepath.Join(pkg, "index.js"), small)
		write(filepath.Join(pkg, ".npmignore"), []byte("test\n"))
		for d := 0; d < subdirsEach; d++ {
			sub := filepath.Join(pkg, fmt.Sprintf("lib%d", d))
			mkdir(sub)
			for f := 0; f < filesPerDir; f++ {
				write(filepath.Join(sub, fmt.Sprintf("m%d.js", f)), small)
			}
		}
	}
	return files, dirs
}

// --- RPC counter (client unary interceptor) ---

// rpcCounter tallies the unary RPCs leaving the client (post-cache), bucketed
// by method, by GetXAttr attribute name, and by GetAttr/GetAttrIfChanged path.
type rpcCounter struct {
	mu            sync.Mutex
	byMethod      map[string]int64
	getxattrName  map[string]int64
	getattrPath   map[string]int64 // GetAttr RPC == client Stat + Lookup
	ifChangedPath map[string]int64 // GetAttrIfChanged RPC (revalidation)
	getattrENOENT int64            // GetAttr replies that returned ENOENT (negative lookups)
}

func newRPCCounter() *rpcCounter {
	c := &rpcCounter{}
	c.resetLocked()
	return c
}

func (c *rpcCounter) resetLocked() {
	c.byMethod = map[string]int64{}
	c.getxattrName = map[string]int64{}
	c.getattrPath = map[string]int64{}
	c.ifChangedPath = map[string]int64{}
	c.getattrENOENT = 0
}

func (c *rpcCounter) reset() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.resetLocked()
}

// count returns the recorded RPC count for a short method name (e.g. "GetXAttr").
func (c *rpcCounter) count(method string) int64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.byMethod[method]
}

func shortMethod(full string) string {
	if i := strings.LastIndex(full, "/"); i >= 0 {
		return full[i+1:]
	}
	return full
}

func (c *rpcCounter) unary(
	ctx context.Context,
	method string,
	req, reply any,
	cc *grpc.ClientConn,
	invoker grpc.UnaryInvoker,
	opts ...grpc.CallOption,
) error {
	// Invoke first so the reply is populated, then bucket (the reply status
	// distinguishes negative lookups from positive stats).
	err := invoker(ctx, method, req, reply, cc, opts...)
	c.mu.Lock()
	c.byMethod[shortMethod(method)]++
	switch r := req.(type) {
	case *proto.GetXAttrRequest:
		c.getxattrName[r.GetAttribute()]++
	case *proto.GetAttrRequest:
		c.getattrPath[r.GetPath()]++
		if rep, ok := reply.(*proto.GetAttrReply); ok && rep.GetStatus() == proto.FsError_FS_ENOENT {
			c.getattrENOENT++
		}
	case *proto.GetAttrIfChangedRequest:
		c.ifChangedPath[r.GetPath()]++
	}
	c.mu.Unlock()
	return err
}

// stream counts streaming RPC initiations (ReadDir, Subscribe) by method. npm
// scans node_modules heavily, so ReadDir — a stream the unary interceptor never
// sees — can be a large share of the chatter; counting it keeps the report honest.
func (c *rpcCounter) stream(
	ctx context.Context,
	desc *grpc.StreamDesc,
	cc *grpc.ClientConn,
	method string,
	streamer grpc.Streamer,
	opts ...grpc.CallOption,
) (grpc.ClientStream, error) {
	c.mu.Lock()
	c.byMethod[shortMethod(method)]++
	c.mu.Unlock()
	return streamer(ctx, desc, cc, method, opts...)
}

// report prints the buckets. n files / d dirs let the reader compute per-file
// ratios (the headline: GetXAttr or GetAttr RPCs per copied file).
func (c *rpcCounter) report(t *testing.T, files, dirs int) {
	t.Helper()
	c.mu.Lock()
	defer c.mu.Unlock()

	t.Logf("=== copy-chatter: %d files, %d dirs copied ===", files, dirs)

	t.Logf("--- RPCs by method ---")
	for _, kv := range sortDesc(c.byMethod) {
		t.Logf("  %-24s %6d  (%.2f/file)", kv.k, kv.v, ratio(kv.v, files))
	}

	t.Logf("--- GetXAttr by attribute name ---")
	if len(c.getxattrName) == 0 {
		t.Logf("  (none)")
	}
	for _, kv := range sortDesc(c.getxattrName) {
		t.Logf("  %-28s %6d  (%.2f/file)", kv.k, kv.v, ratio(kv.v, files))
	}

	reportPaths(t, "GetAttr", c.getattrPath, files)
	t.Logf("    of which %d returned ENOENT (negative/pre-create lookups)", c.getattrENOENT)
	reportPaths(t, "GetAttrIfChanged", c.ifChangedPath, files)
}

func reportPaths(t *testing.T, label string, m map[string]int64, files int) {
	t.Helper()
	var total, distinct, requeried int64
	for _, v := range m {
		total += v
		distinct++
		if v > 1 {
			requeried += v - 1
		}
	}
	t.Logf("--- %s by path: %d total, %d distinct paths, %d re-queries (%.2f/file) ---",
		label, total, distinct, requeried, ratio(total, files))
	// Top re-queried paths (same path hit more than once) name a kernel
	// attr-timeout problem; many distinct single-hit paths name a priming gap.
	top := sortDesc(m)
	shown := 0
	for _, kv := range top {
		if kv.v <= 1 || shown >= 10 {
			break
		}
		t.Logf("    re-queried %3d×  %s", kv.v, kv.k)
		shown++
	}
}

type kv struct {
	k string
	v int64
}

func sortDesc(m map[string]int64) []kv {
	out := make([]kv, 0, len(m))
	for k, v := range m {
		out = append(out, kv{k, v})
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].v != out[j].v {
			return out[i].v > out[j].v
		}
		return out[i].k < out[j].k
	})
	return out
}

func ratio(v int64, files int) float64 {
	if files == 0 {
		return 0
	}
	return float64(v) / float64(files)
}
