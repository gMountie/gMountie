package mount

import (
	"context"
	"os"
	"path/filepath"

	"go.gmountie.dev/gmountie/pkg/client/cache"
	"go.gmountie.dev/gmountie/pkg/client/cache/persist"
	"go.gmountie.dev/gmountie/pkg/client/config"
	"go.gmountie.dev/gmountie/pkg/client/grpc"
	"go.gmountie.dev/gmountie/pkg/client/io"
	"go.gmountie.dev/gmountie/pkg/client/metrics"
	"go.gmountie.dev/gmountie/pkg/proto"
	"go.gmountie.dev/gmountie/pkg/utils/log"

	"github.com/pkg/errors"
	"github.com/puzpuzpuz/xsync/v3"
	"go.uber.org/zap"
)

// SingleVolumeMounter is the interface for the mounter that supports a single volume
type SingleVolumeMounter interface {
	Mount(volumeName, path string) error
	// Wait blocks until the named volume's FUSE server exits — whether from
	// our own Unmount or an out-of-band detach (a direct `fusermount -u`) —
	// then releases that volume's client-side state. It lets a foreground
	// mount notice an external unmount and exit instead of blocking forever.
	Wait(volume string)
	Mounter
}

// SingleVolumeMounterImpl is a service that mounts volumes
type SingleVolumeMounterImpl struct {
	client grpc.Client
	fuse   *config.FUSEConfig
	cache  config.CacheConfig
	// rawIDs disables WhoAmI-based UID/GID rewriting when true.
	rawIDs bool
	mounts *xsync.MapOf[string, mountHandle]
	// mountPaths tracks the local mountpoint per volume so Unmount
	// can request a lazy fusermount3 -uz fallback if the regular
	// unmount keeps failing with EBUSY.
	mountPaths *xsync.MapOf[string, string]
	persists   *xsync.MapOf[string, *persist.Persist]
	backends   *xsync.MapOf[string, io.FileSystemBackend]
}

// NewSingleVolumeMounter creates a new SingleVolumeMounterImpl. fuseCfg
// must be non-nil; the client config layer guarantees this by treating
// FUSE as a required block with defaults. cacheCfg is consumed by value
// and only applied when cacheCfg.Enabled is true. rawIDs disables
// WhoAmI-based UID/GID rewriting (pass true for backup/admin use-cases
// that need to preserve server-side ownership as-is).
func NewSingleVolumeMounter(client grpc.Client, fuseCfg *config.FUSEConfig, cacheCfg config.CacheConfig, rawIDs bool) SingleVolumeMounter {
	return &SingleVolumeMounterImpl{
		client:     client,
		fuse:       fuseCfg,
		cache:      cacheCfg,
		rawIDs:     rawIDs,
		mounts:     xsync.NewMapOf[string, mountHandle](),
		mountPaths: xsync.NewMapOf[string, string](),
		persists:   xsync.NewMapOf[string, *persist.Persist](),
		backends:   xsync.NewMapOf[string, io.FileSystemBackend](),
	}
}

// identityFromProto converts a proto.Identity wire message to the
// io.Identity type used by IDRewriter. Returns nil when p is nil, which
// makes NewIDRewriter produce a nil (no-op) rewriter.
func identityFromProto(p *proto.Identity) *io.Identity {
	if p == nil {
		return nil
	}
	return &io.Identity{Uid: p.Uid, Gid: p.PrimaryGid, Gids: p.Gids}
}

// Mount mounts a volume
func (m *SingleVolumeMounterImpl) Mount(volume, mountPath string) (err error) {
	// Check if the volume is already mounted
	if m.IsVolumeMounted(volume) {
		return errors.Errorf("volume %s is already mounted", volume)
	}

	// Roll back partial state on any failure below. The persist and backend
	// are stored into the maps BEFORE gofs.Mount runs; without this rollback a
	// failed mount would leak them for the process lifetime — including the
	// cache flock, which makes every retry of the same volume in-process fail
	// with ErrCacheLocked. Matters for the importable pkg/... library
	// use-case, where the process outlives a failed mount.
	defer func() {
		if err != nil {
			m.releaseVolume(volume)
		}
	}()

	maxWrite := negotiateMaxWriteBytes(m.client, m.fuse)

	// Plus listings (per-entry attrs on ReadDir) only pay off when the cache
	// decorator below can prime its attr cache from them — without a cache
	// the extra attrs are wasted bytes, so the knob tracks cache.Enabled.
	// With the cache on, also disable the io-layer readahead: the cache does its
	// own sequential prefetch (span over-read), and two prefetchers on one gRPC
	// connection halve WAN read throughput (measured). Without the cache, the
	// io-layer readahead stays on — it's the prefetcher in that path.
	backendOpts := []io.BackendOption{io.WithPlusListings(m.cache.Enabled)}
	if m.cache.Enabled {
		backendOpts = append(backendOpts, io.WithoutReadahead())
	}
	var backend io.FileSystemBackend = io.NewBackendClient(m.client, volume, backendOpts...)
	if m.cache.Enabled {
		root := filepath.Join(m.cache.Path, volume)
		// Wire the persist GC/accounting observability sink to the client
		// metrics. Guard nil: a client built without metrics leaves the sink
		// nil, which persist.Open turns into its no-op (never wrap a nil
		// *metrics.Metrics — the adapter would panic).
		var gcMetrics persist.GCMetrics
		if cm := m.client.Metrics(); cm != nil {
			gcMetrics = persistGCMetrics{cm}
		}
		p, err := persist.Open(persist.Options{Root: root, DiskMaxBytes: int64(m.cache.DiskMaxBytes), Metrics: gcMetrics})
		if err != nil {
			return errors.Wrap(err, "open cache persist")
		}
		m.persists.Store(volume, p)
		backend = cache.NewCachedBackend(backend, cache.ConfigFromClient(m.cache), p, m.client.Fs(), volume)
	}
	m.backends.Store(volume, backend)

	// Fetch the server identity so we can rewrite UIDs/GIDs to local values.
	// raw_ids=true skips this and leaves the kernel seeing the raw server IDs.
	var rewriter *io.IDRewriter
	if !m.rawIDs {
		ctx, cancel := context.WithTimeout(context.Background(), m.client.MetaTimeout())
		defer cancel()
		idResp, err := m.client.WhoAmI(ctx, volume)
		if err != nil {
			log.Log.Warn("WhoAmI failed, mounting with raw IDs", zap.String("volume", volume), zap.Error(err))
		} else {
			rewriter = io.NewIDRewriter(identityFromProto(idResp), uint32(os.Getuid()), uint32(os.Getgid()))
		}
	}

	handle, err := establishMount(mountPath, volume, m.client.GetEndpoint(), backend, rewriter, m.fuse, maxWrite, m.client.MetaTimeout())
	if err != nil {
		return err
	}
	m.mounts.Store(volume, handle)
	m.mountPaths.Store(volume, mountPath)
	return nil
}

// IsVolumeMounted checks if a volume is mounted
func (m *SingleVolumeMounterImpl) IsVolumeMounted(volume string) bool {
	_, ok := m.mounts.Load(volume)
	return ok
}

// GetMounts returns the mounts
func (m *SingleVolumeMounterImpl) GetMounts() []string {
	mounts := make([]string, 0)
	m.mounts.Range(func(volume string, _ mountHandle) bool {
		mounts = append(mounts, volume)
		return true
	})
	return mounts
}

// Wait blocks until the named volume's FUSE server exits, then releases the
// volume's client-side state. server.Wait() returns when the Serve loop ends —
// for our own Unmount or for an out-of-band detach (a direct `fusermount -u`) —
// so a foreground mount can use this to exit on an external unmount rather than
// blocking on its signal wait forever. No-op if the volume isn't mounted.
func (m *SingleVolumeMounterImpl) Wait(volume string) {
	handle, ok := m.mounts.Load(volume)
	if !ok {
		return
	}
	handle.Wait()
	// The handle is gone; drop bookkeeping so a later Unmount/Close is a clean
	// no-op (and we don't redundantly fusermount an already-detached path).
	m.releaseVolume(volume)
}

// Unmount unmounts a volume
func (m *SingleVolumeMounterImpl) Unmount(volume string) error {
	handle, ok := m.mounts.Load(volume)
	if !ok {
		return errors.Errorf("volume %s is not mounted", volume)
	}
	mountPath, _ := m.mountPaths.Load(volume)
	if err := handle.Unmount(mountPath); err != nil {
		return err
	}
	m.releaseVolume(volume)
	log.Log.Info("unmounted volume", zap.String("volume", volume))
	return nil
}

// releaseVolume drops a volume's bookkeeping and closes its backend/cache.
// It is safe to call concurrently and more than once for the same volume: the
// atomic LoadAndDelete guarantees each backend/persist is closed exactly once,
// which matters because both Unmount (signal path) and Wait (server-exit path)
// can race to release the same volume after a clean unmount.
func (m *SingleVolumeMounterImpl) releaseVolume(volume string) {
	m.mounts.Delete(volume)
	m.mountPaths.Delete(volume)
	if be, ok := m.backends.LoadAndDelete(volume); ok {
		_ = be.Close()
	}
	if p, ok := m.persists.LoadAndDelete(volume); ok {
		_ = p.Close()
	}
}

// UnmountAll unmounts all volumes
func (m *SingleVolumeMounterImpl) UnmountAll() error {
	for _, volume := range m.GetMounts() {
		err := m.Unmount(volume)
		if err != nil {
			return err
		}
	}
	return nil
}

// Close closes the mounter
func (m *SingleVolumeMounterImpl) Close() error {
	return m.UnmountAll()
}

// persistGCMetrics adapts *metrics.Metrics to persist.GCMetrics. The adapter
// lives here (not in persist, which must not import metrics, and not in
// metrics, which would reverse the dependency). The embedded *Metrics must be
// non-nil — the caller guards that before wrapping.
type persistGCMetrics struct{ m *metrics.Metrics }

func (a persistGCMetrics) ChunkUnlinked(reason string) { a.m.ChunkUnlinkedInc(reason) }
func (a persistGCMetrics) GhostEntryDeleted()          { a.m.GhostEntryDeletedInc() }
func (a persistGCMetrics) RefcountUnderflow()          { a.m.RefcountUnderflowInc() }
func (a persistGCMetrics) OrphanReclaimed()            { a.m.OrphanReclaimedInc() }
func (a persistGCMetrics) TmpReclaimed()               { a.m.TmpReclaimedInc() }
func (a persistGCMetrics) BudgetEviction()             { a.m.BudgetEvictionInc() }
func (a persistGCMetrics) DiskBytesUsed(n int64)       { a.m.DiskBytesUsedSet(n) }
