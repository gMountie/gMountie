package contract_test

import (
	"os"
	"testing"
	"time"

	"go.gmountie.dev/gmountie/pkg/client/backend"
	"go.gmountie.dev/gmountie/pkg/client/backend/cache"
	"go.gmountie.dev/gmountie/pkg/client/backend/cache/persist"
	"go.gmountie.dev/gmountie/pkg/client/backend/contract"
	"go.gmountie.dev/gmountie/pkg/client/backend/identity"
	"go.gmountie.dev/gmountie/pkg/client/backend/memfs"
	"go.gmountie.dev/gmountie/pkg/client/backend/observer"
	"go.gmountie.dev/gmountie/pkg/client/metrics"

	"github.com/stretchr/testify/suite"
)

// ContractSuite drives RunBackendContract against each FileSystemBackend
// layering so a single run exercises the seam for every decorator. Each method
// is a distinct backend construction; RunBackendContract itself fans out the
// behavioral subtests.
type ContractSuite struct{ suite.Suite }

func TestContractSuite(t *testing.T) { suite.Run(t, new(ContractSuite)) }

// TestMemfs proves the in-memory reference backend (and, transitively, the
// harness itself) honors the FileSystemBackend behavioral contract.
func (s *ContractSuite) TestMemfs() {
	contract.RunBackendContract(s.T(), "memfs", func() backend.FileSystemBackend {
		return memfs.New()
	})
}

// TestObserverOverMemfs proves the metrics observer is transparent: wrapping
// memfs in NewMetricsLayer must not change any contract-observable behavior.
func (s *ContractSuite) TestObserverOverMemfs() {
	contract.RunBackendContract(s.T(), "observer-over-memfs", func() backend.FileSystemBackend {
		return observer.NewMetricsLayer(memfs.New(), metrics.NopRecorder{})
	})
}

// TestCacheOverMemfs proves the cache decorator honors the contract over a real
// backend. Subscribe is disabled (no client, no goroutine), so freshness is
// TTL-driven and the validity tracker is globally verified at construction;
// persistence uses a per-test temp dir.
//
// The gRPC transport leaf (BackendClient) is deliberately EXCLUDED from this
// suite: exercising it requires full gRPC mocks for every op (it streams
// Read/Write, threads session_id/request_id, owns the Subscribe stream and the
// retry loop), which its own unit tests in pkg/client/backend already cover. The
// contract here is about the layering seam, not the wire.
func (s *ContractSuite) TestCacheOverMemfs() {
	contract.RunBackendContract(s.T(), "cache-over-memfs", func() backend.FileSystemBackend {
		p, err := persist.Open(persist.Options{Root: s.T().TempDir()})
		s.Require().NoError(err)
		cfg := cache.Config{
			SubscribeEnabled: false,
			MemoryMaxBytes:   0, // disable byte-cap eviction: entries live until invalidated
			ChunkSizeBytes:   1 << 20,
			AttrTTL:          time.Minute,
			DirTTL:           time.Minute,
			NegativeTTL:      time.Minute,
			XAttrTTL:         time.Minute,
			StatFsTTL:        time.Minute,
		}
		// client=nil + SubscribeEnabled=false => no subscriber goroutine.
		return cache.NewCachedBackend(memfs.New(), cfg, p, nil, "", metrics.NopRecorder{})
	})
}

// TestIdentityOverMemfs proves the identity layer is FS-contract-faithful: it
// rewrites uid/gid but must not perturb any other FS semantics. A NON-identity
// rewriter (server identity = caller's real uid/gid; a foreign localUID so the
// nobody fallback is live) is used so the rewrite is actually exercised. The
// contract asserts FS behavior (sizes, ENOENT, rename, xattr roundtrips), not
// specific uid/gid values, so it passes regardless of the mapping.
func (s *ContractSuite) TestIdentityOverMemfs() {
	contract.RunBackendContract(s.T(), "identity-over-memfs", func() backend.FileSystemBackend {
		serverUID := uint32(os.Getuid())
		serverGID := uint32(os.Getgid())
		// localUID/localGID differ from the server identity so Inbound/Outbound
		// are non-trivial transforms, not the identity map.
		rw := backend.NewIDRewriter(
			&backend.Identity{Uid: serverUID, Gid: serverGID, Gids: []uint32{serverGID}},
			serverUID+1000, serverGID+1000,
		)
		return identity.NewLayer(memfs.New(), rw)
	})
}
