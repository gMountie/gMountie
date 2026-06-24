package mount

import (
	"context"
	"os"
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
	"go.gmountie.dev/gmountie/pkg/client/backend/wal"
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

// walFlushInterval is the period for the background WAL interval flusher.
// It controls how often pending (delegated) writes are streamed to the server
// when no explicit Fsync or recall-flush has triggered an earlier flush.
const walFlushInterval = 30 * time.Second

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
	var coord *wal.Coordinator
	if m.cache.Enabled {
		inv := &lazyInvalidator{}
		delMgr = delegation.NewManager(inv)

		// ── WAL construction ────────────────────────────────────────────────
		// The WAL log lives under the same per-volume root as the cache so
		// it is co-located with the cache's meta.db and chunks/.
		walRoot := filepath.Join(m.cache.Path, volume)
		// persist.Open (below) calls os.MkdirAll on this root, but the WAL
		// bolt.Open runs first — create the directory proactively so it
		// doesn't fail with ENOENT on the first mount of a new volume.
		if err := os.MkdirAll(walRoot, 0o700); err != nil {
			return errors.Wrap(err, "create wal root")
		}
		walLog, werr := wal.Open(filepath.Join(walRoot, "wal.db"))
		if werr != nil {
			return errors.Wrap(werr, "open wal log")
		}
		// Hand the wal.db's stable epoch to the delegation Manager so every
		// piggybacked DelegationRequest carries it — the server keys the
		// delegation gen + dedup watermark per (identity, volume, wal-epoch),
		// matching the epoch stamped on each WalOp at flush time. A fresh wal.db
		// (cache wipe / reinstall) thus gets its own server-side seq namespace
		// and is never dedup-skipped against a prior epoch's watermark.
		delMgr.SetWalEpoch(walLog.Epoch())
		overlay := wal.NewOverlay()
		// Wire the three flush options so Apply streams actually reach the server.
		// applyFactory opens a fresh Apply stream per flush using the mounted
		// volume's gRPC client.
		// Per-op caller fidelity (passthrough/system mode) is a follow-up;
		// squash (default) squashes all callers to one principal so a mount-level
		// caller yields the correct watermark key.
		mountCaller := &proto.Caller{
			Owner: &proto.Owner{Uid: uint32(os.Getuid()), Gid: uint32(os.Getgid())},
			Pid:   uint32(os.Getpid()),
		}
		coord = wal.NewCoordinator(delMgr, walLog, overlay,
			wal.WithApplyFactory(func(ctx context.Context) (proto.RpcFs_ApplyClient, error) {
				return m.client.Fs().Apply(ctx)
			}),
			wal.WithVolume(volume),
			wal.WithCaller(mountCaller),
		)

		// SetMetrics BEFORE the coordinator can Flush or Replay (the global
		// walMetrics is written here; subsequent flushes will read it).
		if cm := m.client.Metrics(); cm != nil {
			wal.SetMetrics(cm)
		}

		// Dead-process recovery (CRIT-2): if a previous mount of this volume left
		// un-acked ops in wal.db, apply them to the in-memory overlay now so RYOW
		// is correct for any delegation grants re-acquired during this mount.
		// This runs synchronously before FUSE starts serving so no reads can
		// arrive before the overlay is populated.
		//
		// A persistent-replay of the ops to the server happens asynchronously once
		// FUSE is live; see the startup replay goroutine started after establishMount.
		if err := coord.RebuildOverlay(); err != nil {
			// RebuildOverlay already fired onLoss("wal-unreadable"); surface the
			// error to the caller so the mount is rejected rather than silently
			// operating with a broken overlay.
			return errors.Wrap(err, "wal: startup overlay rebuild")
		}

		// Wire the WAL drain into the transport BEFORE constructing the transport
		// backend (backendOpts are consumed by NewBackendClient below).
		backendOpts = append(backendOpts, transport.WithWriteDrain(coord))

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

		// posWAL: WAL read-your-own-writes seam (outer of the cache). Serves
		// merged reads (base ⊕ pending overlay) for delegated paths and records
		// metadata mutations (create/unlink/rename/…) in the log without
		// forwarding to inner. coord.Close() → stopFlusher + log.Close() is
		// driven by the Layer.Close() override, which is reached via
		// releaseVolume → be.Close() cascade.
		coord_ := coord
		layers = append(layers, backendLayer{pos: posWAL, build: func(inner backend.FileSystemBackend) backend.FileSystemBackend {
			return wal.NewLayer(inner, delMgr_, coord_)
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

	// Start the recall goroutine and WAL interval flusher only AFTER the FUSE
	// mount succeeds. Goroutines started before this point would leak on mount
	// failure (the failure-rollback defer calls releaseVolume → be.Close() →
	// Layer.Close() → coord.Close() → stopFlusher, but if the goroutine was
	// never started there is nothing to cancel). The composeBackend closures
	// have all run by now, so inv.set has been called and OnRecall can reach
	// the real cache invalidator.
	if m.cache.Enabled && delMgr != nil {
		// Wire the WAL coordinator as the recall flusher BEFORE starting the
		// recall goroutine (which may immediately receive a recall and trigger
		// a flush via mgr.OnRecall).
		if coord != nil {
			delMgr.SetRecallFlusher(coord)
			coord.StartIntervalFlusher(walFlushInterval)

			// Startup replay: if wal.db had leftover un-acked ops from a previous
			// crashed process (CRIT-2), replay them to the server now that a live
			// connection is available.  The overlay was already rebuilt above
			// (RebuildOverlay), so RYOW is correct immediately; this goroutine
			// sends the ops to the server so the data is durably committed.
			//
			// StartupReplay tracks the goroutine in flusherWg so coord.Close()
			// waits for it to exit before log.Close() — preventing a data race
			// between the log.Replay call and log.Close().  It also stores a cancel
			// func that stopFlusher invokes so a fast unmount does not hang.
			coord.StartupReplay(context.Background())
		}
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
