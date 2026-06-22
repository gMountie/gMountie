package contract_test

import (
	"testing"
	"time"

	cio "go.gmountie.dev/gmountie/pkg/client/io"
	"go.gmountie.dev/gmountie/pkg/client/io/cache"
	"go.gmountie.dev/gmountie/pkg/client/io/cache/persist"
	"go.gmountie.dev/gmountie/pkg/client/io/contract"
	"go.gmountie.dev/gmountie/pkg/client/io/memfs"
	"go.gmountie.dev/gmountie/pkg/client/io/observer"
	"go.gmountie.dev/gmountie/pkg/client/metrics"

	"github.com/stretchr/testify/require"
)

// TestMemfsContract proves the in-memory reference backend (and, transitively,
// the harness itself) honors the FileSystemBackend behavioral contract.
func TestMemfsContract(t *testing.T) {
	contract.RunBackendContract(t, "memfs", func() cio.FileSystemBackend {
		return memfs.New()
	})
}

// TestObserverOverMemfsContract proves the metrics observer is transparent:
// wrapping memfs in NewMetricsLayer must not change any contract-observable
// behavior.
func TestObserverOverMemfsContract(t *testing.T) {
	contract.RunBackendContract(t, "observer-over-memfs", func() cio.FileSystemBackend {
		return observer.NewMetricsLayer(memfs.New(), metrics.NopRecorder{})
	})
}

// TestCacheOverMemfsContract proves the cache decorator honors the contract
// over a real backend. Subscribe is disabled (no client, no goroutine), so
// freshness is TTL-driven and the validity tracker is globally verified at
// construction; persistence uses a per-test temp dir.
//
// The gRPC transport leaf (BackendClient) is deliberately EXCLUDED from this
// suite: exercising it requires full gRPC mocks for every op (it streams
// Read/Write, threads session_id/request_id, owns the Subscribe stream and the
// retry loop), which its own unit tests in pkg/client/io already cover. The
// contract here is about the layering seam, not the wire.
func TestCacheOverMemfsContract(t *testing.T) {
	contract.RunBackendContract(t, "cache-over-memfs", func() cio.FileSystemBackend {
		p, err := persist.Open(persist.Options{Root: t.TempDir()})
		require.NoError(t, err)
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
