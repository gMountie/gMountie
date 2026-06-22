package mount

import (
	"path/filepath"

	"go.gmountie.dev/gmountie/pkg/client/backend"
	"go.gmountie.dev/gmountie/pkg/client/backend/cache"
	"go.gmountie.dev/gmountie/pkg/client/backend/cache/persist"
	"go.gmountie.dev/gmountie/pkg/client/backend/identity"
	"go.gmountie.dev/gmountie/pkg/client/backend/observer"
	"go.gmountie.dev/gmountie/pkg/client/backend/transport"
	"go.gmountie.dev/gmountie/pkg/client/config"
	"go.gmountie.dev/gmountie/pkg/client/grpc"
	"go.gmountie.dev/gmountie/pkg/client/metrics"
	"go.gmountie.dev/gmountie/pkg/proto"
	"go.gmountie.dev/gmountie/pkg/utils/log"

	"github.com/pkg/errors"
	"github.com/puzpuzpuz/xsync/v3"
	"go.uber.org/zap"
)

// mappingModeSquash is the WhoAmI mapping_mode value (matching the server's
// config.MappingModeSquash) for which the kernel may enforce permissions locally
// via default_permissions instead of forwarding an Access RPC per check.
const mappingModeSquash = "squash"

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
	backends   *xsync.MapOf[string, backend.FileSystemBackend]
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
		backends:   xsync.NewMapOf[string, backend.FileSystemBackend](),
	}
}

// identityFromProto converts a proto.Identity wire message to the
// identity.Identity type used by IDRewriter. Returns nil when p is nil, which
// makes NewIDRewriter produce a nil (no-op) rewriter.
func identityFromProto(p *proto.Identity) *identity.Identity {
	if p == nil {
		return nil
	}
	return &identity.Identity{Uid: p.Uid, Gid: p.PrimaryGid, Gids: p.Gids}
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

	params, rewriter := negotiateMountParams(m.client, m.fuse, m.rawIDs, volume)

	backendOpts := []transport.BackendOption{
		transport.WithPlusListings(m.cache.Enabled),
		transport.WithXattrListings(m.cache.Enabled),
	}
	if m.cache.Enabled {
		backendOpts = append(backendOpts, transport.WithoutReadahead())
	}
	transportBackend := transport.NewBackendClient(m.client, volume, backendOpts...)

	var layers []backendLayer
	if m.cache.Enabled {
		root := filepath.Join(m.cache.Path, volume)
		var gcMetrics persist.GCMetrics
		if cm := m.client.Metrics(); cm != nil {
			gcMetrics = persistGCMetrics{cm}
		}
		p, err := persist.Open(persist.Options{Root: root, DiskMaxBytes: int64(m.cache.DiskMaxBytes), Metrics: gcMetrics})
		if err != nil {
			return errors.Wrap(err, "open cache persist")
		}
		m.persists.Store(volume, p)
		client := m.client // capture for the closure
		cacheCfg := cache.ConfigFromClient(m.cache)
		// *metrics.Metrics satisfies metrics.Recorder. client.Metrics() may return
		// a nil *Metrics (no metrics wired); pass a true-nil Recorder in that case
		// so NewCachedBackend substitutes a NopRecorder rather than receiving a
		// non-nil interface wrapping a nil pointer (which would panic on emit).
		var rec metrics.Recorder
		if cm := client.Metrics(); cm != nil {
			rec = cm
		}
		layers = append(layers, backendLayer{pos: posCache, build: func(inner backend.FileSystemBackend) backend.FileSystemBackend {
			return cache.NewCachedBackend(inner, cacheCfg, p, client.Fs(), volume, rec)
		}})
	}

	if rec := m.client.Metrics(); rec != nil {
		layers = append(layers, backendLayer{pos: posObserver, build: func(inner backend.FileSystemBackend) backend.FileSystemBackend {
			return observer.NewMetricsLayer(inner, rec)
		}})
	}

	// The identity layer is OUTERMOST: it rewrites server↔local uid/gid so the
	// FUSE adapters see local display ids while the cache (and its Subscribe
	// invalidation stream) keeps storing server ids. A nil rewriter (raw_ids /
	// no WhoAmI identity) means NewLayer returns inner unchanged.
	if rewriter != nil {
		layers = append(layers, backendLayer{pos: posIdentity, build: func(inner backend.FileSystemBackend) backend.FileSystemBackend {
			return identity.NewLayer(inner, rewriter)
		}})
	}

	backend := composeBackend(transportBackend, layers)
	m.backends.Store(volume, backend)

	handle, err := establishMount(mountPath, volume, m.client.GetEndpoint(), backend, m.fuse, params.MaxWriteBytes, m.client.MetaTimeout(), params.DefaultPermissions)
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
