package mount

import (
	"context"
	"path/filepath"
	"sync"
	"time"

	"go.gmountie.dev/gmountie/pkg/client/backend"
	"go.gmountie.dev/gmountie/pkg/client/backend/cache"
	"go.gmountie.dev/gmountie/pkg/client/backend/cache/persist"
	"go.gmountie.dev/gmountie/pkg/client/backend/delegation"
	"go.gmountie.dev/gmountie/pkg/client/backend/identity"
	"go.gmountie.dev/gmountie/pkg/client/backend/observer"
	"go.gmountie.dev/gmountie/pkg/client/backend/transport"
	"go.gmountie.dev/gmountie/pkg/client/config"
	grpcclient "go.gmountie.dev/gmountie/pkg/client/grpc"
	"go.gmountie.dev/gmountie/pkg/client/metrics"
	"go.gmountie.dev/gmountie/pkg/proto"
	"go.gmountie.dev/gmountie/pkg/utils/log"

	"github.com/pkg/errors"
	"github.com/puzpuzpuz/xsync/v3"
	"go.uber.org/zap"
	"google.golang.org/grpc"
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
	client grpcclient.Client
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
func NewSingleVolumeMounter(client grpcclient.Client, fuseCfg *config.FUSEConfig, cacheCfg config.CacheConfig, rawIDs bool) SingleVolumeMounter {
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

// lazyInvalidator is a forward-reference adapter that breaks the
// Manager↔cache construction cycle. The Manager is constructed first
// (with a lazyInvalidator as its CacheInvalidator), the cache is built
// next (using the Manager as the oracle), and then the lazyInvalidator's
// target is set to the concrete *cachedBackend. After set() is called,
// all OnRecall-driven InvalidateSubtree calls reach the real cache.
type lazyInvalidator struct {
	mu     sync.RWMutex
	target delegation.CacheInvalidator
}

func (l *lazyInvalidator) InvalidateSubtree(p string) {
	l.mu.RLock()
	t := l.target
	l.mu.RUnlock()
	if t != nil {
		t.InvalidateSubtree(p)
	}
}

func (l *lazyInvalidator) set(t delegation.CacheInvalidator) {
	l.mu.Lock()
	l.target = t
	l.mu.Unlock()
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

	var layers []backendLayer

	// --- Delegation wiring (only when cache is enabled) ---
	// Delegation rides on the cache: the Manager's IsDelegated oracle is
	// threaded into the cache so delegated paths skip revalidation, and
	// the transport hook piggybacks requests/grants on mutating RPCs.
	// With cache disabled the nil-oracle / no-hook / no-goroutine path is
	// byte-for-byte unchanged.
	var delMgr *delegation.Manager
	if m.cache.Enabled {
		inv := &lazyInvalidator{}
		delMgr = delegation.NewManager(inv)

		// Wire the transport hook BEFORE constructing the transport backend.
		backendOpts = append(backendOpts, transport.WithDelegationHook(delMgr))

		// Build the cache layer. NewCachedBackend returns backend.FileSystemBackend
		// but the concrete type is *cachedBackend which satisfies
		// delegation.CacheInvalidator via the InvalidateSubtree method added in
		// this PR. We type-assert inside the closure (the only place the concrete
		// value exists) and wire it into the forward-ref adapter immediately, so
		// OnRecall can always reach the real invalidator.
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
		// Capture inv for use in the closure below (can't close over local err).
		inv_ := inv
		delMgr_ := delMgr
		layers = append(layers, backendLayer{pos: posCache, build: func(inner backend.FileSystemBackend) backend.FileSystemBackend {
			cb := cache.NewCachedBackend(inner, cacheCfg, p, client.Fs(), volume, rec, delMgr_)
			// Wire the forward-ref adapter: after this point OnRecall can reach the real cache.
			if ci, ok := cb.(delegation.CacheInvalidator); ok {
				inv_.set(ci)
			} else {
				log.Log.Error("cache backend does not implement CacheInvalidator; delegation recalls will not invalidate the cache")
			}
			return cb
		}})

		// posWritePath: records every mutating op path into the Manager's write-set
		// so the Manager can compute an LCA delegation root to piggyback on RPCs.
		layers = append(layers, backendLayer{pos: posWritePath, build: func(inner backend.FileSystemBackend) backend.FileSystemBackend {
			return delegation.NewLayer(inner, delMgr_)
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

	transportBackend := transport.NewBackendClient(m.client, volume, backendOpts...)
	composed := composeBackend(transportBackend, layers)
	m.backends.Store(volume, composed)

	handle, err := establishMount(mountPath, volume, m.client.GetEndpoint(), composed, m.fuse, params.MaxWriteBytes, m.client.MetaTimeout(), params.DefaultPermissions)
	if err != nil {
		return err
	}
	m.mounts.Store(volume, handle)
	m.mountPaths.Store(volume, mountPath)

	// Start the recall goroutine only AFTER the FUSE mount succeeds. The
	// goroutine requires no FUSE state of its own, but delaying until here
	// ensures we never leak a running goroutine on mount failure (the
	// failure-rollback defer calls releaseVolume → be.Close() → mgr.Close(),
	// but if the goroutine was never started there is nothing to cancel). The
	// composeBackend closures have all run by now, so inv.set has been called
	// and OnRecall can always reach the real cache invalidator.
	if m.cache.Enabled && delMgr != nil {
		m.startRecallGoroutine(delMgr, volume)
	}

	return nil
}

// startRecallGoroutine starts the background goroutine that drains the
// server-pushed Recall stream. It creates a context whose cancellation is
// wired into delMgr.Close() via SetCancel, so the goroutine exits cleanly
// on unmount. The goroutine models the reconnect loop of the Subscribe
// consumer in pkg/client/backend/cache/subscriber.go.
func (m *SingleVolumeMounterImpl) startRecallGoroutine(mgr *delegation.Manager, volume string) {
	ctx, cancel := context.WithCancel(context.Background())
	mgr.SetCancel(cancel)
	go m.runRecallLoop(ctx, mgr, volume)
}

// runRecallLoop is the blocking body of the recall goroutine. It is separated
// from startRecallGoroutine so tests can drive it directly (same package,
// no FUSE required). It reconnects with exponential backoff (1s → 30s) and
// exits when ctx is cancelled.
func (m *SingleVolumeMounterImpl) runRecallLoop(ctx context.Context, mgr *delegation.Manager, volume string) {
	backoff := time.Second
	for ctx.Err() == nil {
		stream, err := m.client.Fs().Recall(ctx, waitForReady())
		if err == nil {
			// Reset backoff on a successful stream open so that after a
			// server restart the goroutine doesn't stay pinned at the max.
			backoff = time.Second
			for {
				msg, rerr := stream.Recv()
				if rerr != nil {
					break
				}
				if err := mgr.OnRecall(ctx, msg.GetRoot()); err != nil {
					// WAL flush failed: the handoff is aborted. Do NOT send a
					// RecallAck — letting the server-side RecallRegistry time out
					// is the least-bad option given the current wire protocol
					// (RecallAck has no error field). The server will treat the
					// timeout as a failed recall and may revoke the grant itself.
					// The inner stream loop continues so later recalls on
					// unaffected roots can still be processed.
					log.Log.Error("recall OnRecall failed; skipping RecallAck",
						zap.String("volume", volume),
						zap.String("root", msg.GetRoot()),
						zap.Error(err),
					)
					continue
				}
				_ = stream.Send(&proto.RecallAck{RecallId: msg.GetRecallId(), Done: true})
			}
		}
		select {
		case <-ctx.Done():
			log.Log.Debug("recall goroutine exited", zap.String("volume", volume))
			return
		case <-time.After(backoff):
		}
		if backoff < 30*time.Second {
			backoff *= 2
			if backoff > 30*time.Second {
				backoff = 30 * time.Second
			}
		}
	}
	log.Log.Debug("recall goroutine exited", zap.String("volume", volume))
}

// waitForReady returns the grpc.WaitForReady(true) call option. Factored
// out so the recall goroutine opener matches the Subscribe consumer pattern.
func waitForReady() grpc.CallOption {
	return grpc.WaitForReady(true)
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
