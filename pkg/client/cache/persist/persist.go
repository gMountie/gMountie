// Package persist is the on-disk backing store for the client-side
// cache. It owns a bbolt database (meta.db) under a per-volume root
// directory and a content-addressable chunks/ tree. A LOCK file
// enforces single-process ownership. Higher layers in pkg/client/cache
// compose a Persist with their in-memory tiers.
package persist

import (
	"encoding/hex"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pkg/errors"
	bolt "go.etcd.io/bbolt"

	"go.gmountie.dev/gmountie/pkg/utils/log"
	"go.uber.org/zap"
)

// Chunk-unlink reasons threaded through unlinkChunk into ChunkUnlinked.
// Named to avoid label typos across the call sites.
const (
	unlinkReasonRefcountZero = "refcount_zero" // Invalidate*/PutChunkRef-overwrite/DecChunkRef
	unlinkReasonGhost        = "ghost"         // ghost sweep reclaimed a dangling chunk
	unlinkReasonOrphan       = "orphan"        // orphan sweep reclaimed an unreferenced chunk
	unlinkReasonBudget       = "budget"        // disk-budget eviction
)

// GCMetrics is the optional, nil-safe metrics sink for the persist GC and
// disk-accounting paths. *metrics.Metrics satisfies it via a client-side
// adapter (see pkg/client/mount). Defined here (not imported) so the persist
// package stays decoupled from the metrics package and the sink is trivially
// fakeable in tests. A nil Metrics on Options becomes noopGCMetrics, so every
// internal call site stays nil-check-free.
type GCMetrics interface {
	// ChunkUnlinked is called once per successful on-disk chunk removal,
	// labelled by reason (one of the unlinkReason* consts).
	ChunkUnlinked(reason string)
	// GhostEntryDeleted is called once per data_idx entry reclaimed by the
	// ghost sweep (an index entry over a missing chunk file).
	GhostEntryDeleted()
	// RefcountUnderflow is called whenever a decrement lands on an absent
	// refcount key (a corruption signal).
	RefcountUnderflow()
	// OrphanReclaimed is called once per orphan chunk file reclaimed.
	OrphanReclaimed()
	// TmpReclaimed is called once per stale crash-leftover .tmp- file reclaimed.
	TmpReclaimed()
	// BudgetEviction is called once per data_idx entry evicted under disk
	// budget pressure.
	BudgetEviction()
	// DiskBytesUsed sets the gauge of currently-accounted bytes in chunks/.
	DiskBytesUsed(n int64)
}

// noopGCMetrics is the zero-cost sink used when Options.Metrics is nil.
type noopGCMetrics struct{}

func (noopGCMetrics) ChunkUnlinked(string) {}
func (noopGCMetrics) GhostEntryDeleted()   {}
func (noopGCMetrics) RefcountUnderflow()   {}
func (noopGCMetrics) OrphanReclaimed()     {}
func (noopGCMetrics) TmpReclaimed()        {}
func (noopGCMetrics) BudgetEviction()      {}
func (noopGCMetrics) DiskBytesUsed(int64)  {}

// ErrCacheLocked is returned by Open when another process already
// holds the LOCK file in Root after LockAcquireTimeout has elapsed.
// Wrap-checked with errors.Is.
var ErrCacheLocked = errors.New("cache directory is locked by another process")

// DefaultLockAcquireTimeout is the wait budget for a brand-new Open
// call to inherit the LOCK from a previous owner that's still in the
// middle of releasing it. Sized to cover the unmount-then-remount race:
// the prior process gets a few seconds of regular fusermount3 retries
// plus a lazy-unmount fallback (see pkg/client/mount/common.go) before
// it can close bbolt and drop the flock. A user re-mounting immediately
// after a Ctrl-C lands inside this window and would otherwise eat
// ErrCacheLocked.
const DefaultLockAcquireTimeout = 5 * time.Second

// lockRetryInterval is the poll cadence between flock attempts. Short
// enough that the typical 1-2s release latency only costs 10-20 wakeups.
const lockRetryInterval = 100 * time.Millisecond

// metaSyncInterval bounds how stale the on-disk meta.db can be after an
// unclean crash. meta.db opens NoSync (no per-commit fsync) because the
// cache index is reconstructable — a lost entry costs a cache miss, never
// data. This ticker flushes it durably in the background.
const metaSyncInterval = time.Second

// Options governs Open behaviour.
type Options struct {
	// Root is the per-volume cache directory. Created if missing.
	Root string
	// DiskMaxBytes is the advisory byte budget for chunks/. 0 = unbounded.
	DiskMaxBytes int64
	// LockAcquireTimeout caps how long Open will retry on ErrCacheLocked
	// before giving up. Zero uses DefaultLockAcquireTimeout. Negative
	// disables the retry entirely (single attempt — useful in tests that
	// assert fast-fail semantics).
	LockAcquireTimeout time.Duration
	// DisableBackgroundSweeps skips starting the async orphan/ghost sweeps
	// on Open. Production leaves this false. Index-layer unit tests set it
	// true so the ghost sweep (which deletes data_idx entries whose chunk
	// file is missing) can't race with refcount-only fixtures and make
	// assertions flaky. The meta syncer still runs (it mutates no logical
	// state). Sweep behaviour itself is covered by calling the sweep
	// functions synchronously (see sweep_test.go).
	DisableBackgroundSweeps bool
	// Metrics is the optional GC/accounting observability sink. Nil becomes
	// noopGCMetrics so all internal call sites stay nil-safe.
	Metrics GCMetrics
}

// Persist owns the bbolt handle, chunks/ tree, and LOCK file for one
// cache directory. Safe for concurrent use; bbolt is single-writer
// but Persist serializes writes internally.
type Persist struct {
	root      string
	db        *bolt.DB
	lock      *lockHandle
	disk      *diskAccountant
	metrics   GCMetrics      // GC/accounting sink; never nil (noopGCMetrics if unset)
	stopCh    chan struct{}  // closed by Close to signal all background goroutines
	bgWG      sync.WaitGroup // syncer + sweeps; Close waits on it before db.Close
	closeOnce sync.Once
	closeErr  error
	syncCount atomic.Int64 // cumulative db.Sync() calls; observability for tests

	accMu         sync.Mutex   // serializes diskAccountant mutations with file ops
	refUnderflows atomic.Int64 // decRef on an absent key (corruption signal)
}

// sync fsyncs meta.db and records the call. meta.db opens NoSync (commits
// don't fsync), so durability of a specific mutation comes only from an
// explicit Sync — notably kvDelete, which must make an invalidation durable
// before a crash (issue #113). Routing every db.Sync through here makes that
// fsync observable to tests.
func (p *Persist) sync() error {
	p.syncCount.Add(1)
	return p.db.Sync()
}

// Open acquires the LOCK file, opens (or creates) meta.db, ensures
// buckets exist, and validates format_version (wipes on mismatch).
// Returns ErrCacheLocked when another process holds the lock.
func Open(opts Options) (*Persist, error) {
	if opts.Root == "" {
		return nil, errors.New("persist.Open: Root is required")
	}
	if err := os.MkdirAll(opts.Root, 0o755); err != nil {
		return nil, errors.Wrap(err, "create cache root")
	}
	if err := os.MkdirAll(filepath.Join(opts.Root, "chunks"), 0o755); err != nil {
		return nil, errors.Wrap(err, "create chunks dir")
	}
	lock, err := acquireLockWithRetry(filepath.Join(opts.Root, "LOCK"), opts.LockAcquireTimeout)
	if err != nil {
		return nil, err
	}
	db, err := bolt.Open(filepath.Join(opts.Root, "meta.db"), 0o600, &bolt.Options{Timeout: time.Second, NoSync: true})
	if err != nil {
		_ = lock.release()
		return nil, errors.Wrap(err, "open meta.db")
	}
	wiped, err := ensureSchema(db)
	if err != nil {
		_ = db.Close()
		_ = lock.release()
		return nil, err
	}
	if wiped {
		if err := wipeChunksFor(opts.Root); err != nil {
			_ = db.Close()
			_ = lock.release()
			return nil, err
		}
	}
	m := opts.Metrics
	if m == nil {
		m = noopGCMetrics{}
	}
	p := &Persist{root: opts.Root, db: db, lock: lock, disk: newDiskAccountant(opts.DiskMaxBytes), metrics: m}
	if err := p.seedDiskBytes(); err != nil {
		_ = db.Close()
		_ = lock.release()
		return nil, err
	}
	if err := p.enforceDiskBudget(); err != nil {
		_ = db.Close()
		_ = lock.release()
		return nil, err
	}
	// One stop channel + wait group govern all background goroutines
	// (sweeps and the meta syncer). Created before any of them start so
	// Close can cancel and join them deterministically.
	p.stopCh = make(chan struct{})
	if !opts.DisableBackgroundSweeps {
		p.startBackgroundSweeps()
	}
	p.startMetaSyncer()
	return p, nil
}

// startMetaSyncer launches the background goroutine that periodically
// fsyncs the NoSync meta.db. Stopped by Close before db.Close().
func (p *Persist) startMetaSyncer() {
	p.bgWG.Add(1)
	go func() {
		defer p.bgWG.Done()
		t := time.NewTicker(metaSyncInterval)
		defer t.Stop()
		for {
			select {
			case <-p.stopCh:
				return
			case <-t.C:
				_ = p.sync()
			}
		}
	}()
}

// Close stops the background syncer, flushes bbolt durably, releases the
// lock file, and frees OS resources. Idempotent and safe under concurrent
// calls: the work runs once and every caller gets the same result.
func (p *Persist) Close() error {
	p.closeOnce.Do(func() {
		if p.stopCh != nil {
			close(p.stopCh)
			p.bgWG.Wait()
		}
		// Final durable flush: NoSync means db.Close() won't fsync pending
		// commits for us.
		_ = p.sync()
		if err := p.db.Close(); err != nil {
			_ = p.lock.release()
			p.closeErr = errors.Wrap(err, "close meta.db")
			return
		}
		p.closeErr = p.lock.release()
	})
	return p.closeErr
}

// Root returns the cache directory passed to Open.
func (p *Persist) Root() string { return p.root }

// recordRefUnderflow is the single sink for refcount underflows: a decrement
// that landed on an absent key (double-decrement / corruption). It bumps the
// atomic counter (kept for the test helper), emits the observability counter,
// and warns — underflow is rare and a real corruption signal, so a plain Warn
// (no throttling) is appropriate. Called from inside bbolt txns, which is fine.
func (p *Persist) recordRefUnderflow(hash [16]byte) {
	p.refUnderflows.Add(1)
	p.metrics.RefcountUnderflow()
	log.Log.Warn("chunk refcount underflow",
		zap.String("hash", hex.EncodeToString(hash[:])))
}

// diskAdd is the single update point for the disk accountant: it mutates the
// (atomic) byte total and republishes the gauge. The add is atomic, so this is
// safe regardless of whether accMu is held.
func (p *Persist) diskAdd(n int64) {
	p.disk.add(n)
	p.metrics.DiskBytesUsed(p.disk.Used())
}
