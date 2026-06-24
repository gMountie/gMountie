// Package wal — wal.go: WAL Coordinator (Task 10a).
//
// The Coordinator is the central object that ties the WAL log, the in-memory
// overlay, and the delegation Manager together into the single WriteDrain
// implementation that the transport layer calls on every walHandle.flush().
//
// # Delegation routing
//
// On Drain:
//   - If mgr.IsDelegated(path) → the write is DEFERRED: append an OpWrite to
//     the BboltLog (durable) then apply it to the Overlay (in-memory), and
//     return FS_OK without calling wireFlush. Zero-byte flushes (empty pending)
//     are skipped — they carry no data and would pollute the log.
//   - Otherwise → wireFlush is called immediately (unchanged behavior for
//     non-delegated paths). The log and overlay are not touched.
//
// # Import direction
//
// wal imports transport (for the WriteDrain interface assertion); transport
// must NOT import wal — the dependency arrow is wal → transport, not the
// reverse. This is enforced by the compile-time assertion below.
package wal

import (
	"context"
	stderrors "errors"
	"sync"
	"sync/atomic"

	"github.com/pkg/errors"
	"go.gmountie.dev/gmountie/pkg/client/backend"
	"go.gmountie.dev/gmountie/pkg/client/backend/delegation"
	"go.gmountie.dev/gmountie/pkg/client/backend/transport"
	"go.gmountie.dev/gmountie/pkg/proto"
	"go.gmountie.dev/gmountie/pkg/utils/log"
	"go.uber.org/zap"
)

// compile-time assertion: Coordinator must satisfy transport.WriteDrain.
var _ transport.WriteDrain = (*Coordinator)(nil)

// Coordinator ties the WAL log, the pending overlay, and the delegation
// Manager together. It implements transport.WriteDrain so the transport layer
// can route Flush calls to the WAL (delegated paths) or the wire (all others).
//
// Concurrency: all methods are safe for concurrent use. BboltLog and Overlay
// each manage their own concurrency; delegation.Manager is also concurrent-safe.
// flushMu serialises Apply streams so concurrent Fsync/interval/size triggers
// never double-send. capMu + capCond guard the size-cap backpressure path.
type Coordinator struct {
	mgr     *delegation.Manager
	log     *BboltLog
	overlay *Overlay

	// cfg holds the optional flush configuration set via functional options.
	cfg flushConfig

	// onLoss is the hook called on an ordered halt (FailedSeq > 0) or gen-fence.
	// Default is nil (no-op). Set via WithOnLoss.
	// reason identifies the loss path: "apply-failure", "gen-fenced",
	// "recall-flush-failure", "wal-unreadable".
	onLoss func(reason string, lostOps []Op, fe proto.FsError)

	// watermark is the highest seq durably acked by the server. Written only
	// inside Flush/Replay (which hold flushMu); read concurrently by
	// pendingCount (under capMu). Use atomic load/store to avoid a race.
	watermark atomic.Uint64

	// flushMu serialises all Apply streams (Flush, Replay, Fsync, interval, size).
	flushMu flushMuType

	// capMu and capCond guard the size-cap backpressure path.
	capMu   sync.Mutex
	capCond *sync.Cond
	// flushing is set to true while a background drain flush is in flight,
	// ensuring at most one such goroutine runs at a time (see waitForCap).
	flushing atomic.Bool

	// flushStop and flushStopOnce control the interval goroutine lifecycle.
	flushStop     chan struct{}
	flushStopOnce sync.Once

	// flusherWg tracks the running interval-flush goroutine AND the startup
	// replay goroutine so Close can wait for both to exit before closing the log.
	flusherWg sync.WaitGroup

	// startupReplayCancel cancels the context passed to the startup replay
	// goroutine (launched via StartupReplay). stopFlusher calls this alongside
	// closing flushStop so Close blocks only until the replay goroutine exits.
	// Nil if StartupReplay was never called.
	startupReplayCancel context.CancelFunc
}

// NewCoordinator returns a Coordinator. mgr is the delegation oracle; log and
// overlay hold the durable + in-memory pending state respectively. opts are
// applied in order and enable flush behaviour (WithApplyFactory, WithVolume, etc.).
//
// If no WithOnLoss option is supplied the Coordinator installs a default hook
// that calls logDataLost (loud ERROR log + WalDataLost metric) using the
// (identity, volume) derived from the final cfg (after all opts are applied).
func NewCoordinator(mgr *delegation.Manager, log *BboltLog, overlay *Overlay, opts ...Option) *Coordinator {
	c := &Coordinator{
		mgr:       mgr,
		log:       log,
		overlay:   overlay,
		flushStop: make(chan struct{}),
	}
	c.capCond = sync.NewCond(&c.capMu)
	for _, opt := range opts {
		opt(c)
	}
	// Install the default loud-loss hook when the caller hasn't provided one.
	if c.onLoss == nil {
		c.onLoss = c.defaultOnLoss()
	}
	return c
}

// Drain implements transport.WriteDrain.
//
// Delegated path: appends an OpWrite to the log (durable) and applies it to
// the overlay (in-memory), then returns FS_OK without touching the wire.
// Zero-byte pending data on a delegated path is a no-op (fast-path: skip the
// log entry for a clean Flush that had nothing coalesced).
//
// Non-delegated path: calls wireFlush directly — byte-identical to the
// pre-Task-10 wire-drain behavior.
func (c *Coordinator) Drain(
	ctx context.Context,
	path string,
	pendingData []byte,
	pendingOff int64,
	requestID string,
	wireFlush func(ctx context.Context, data []byte, off int64, reqID string) proto.FsError,
) proto.FsError {
	if !c.mgr.IsDelegated(path) {
		return wireFlush(ctx, pendingData, pendingOff, requestID)
	}

	// Delegated: skip zero-byte flushes (no data to persist).
	if len(pendingData) == 0 {
		return proto.FsError_FS_OK
	}

	op := Op{
		Kind:   OpWrite,
		Path:   path,
		Offset: pendingOff,
		Data:   pendingData,
	}
	if err := c.RecordOp(op); err != nil {
		return proto.FsError_FS_EIO
	}
	return proto.FsError_FS_OK
}

// RecordOp appends op to the durable log and then applies it to the in-memory
// overlay. Durability-first: if Append fails, Apply is skipped and the error
// is returned. Used by the 10b backend layer for create/mkdir/rename/unlink/
// setattr/xattr ops (non-write mutations that bypass the WriteDrain seam).
//
// When a size cap is configured (WithCapOps), RecordOp blocks until the
// pending count drops below the cap, then appends. This provides backpressure:
// the caller degrades toward synchronous writes rather than OOM-ing.
func (c *Coordinator) RecordOp(op Op) error {
	// Backpressure: block if pending WAL count >= cap.
	c.waitForCap()

	if _, err := c.log.Append(op); err != nil {
		return errors.Wrap(err, "wal RecordOp")
	}
	c.overlay.Apply(op)
	return nil
}

// ── Read accessors (thin pass-throughs to Overlay) ────────────────────────────
//
// These keep the Overlay encapsulated behind the Coordinator so Task 10b's
// layer calls the Coordinator rather than the Overlay directly.

// Stat returns the pending overlay state for path.
// See Overlay.Stat for the full contract.
func (c *Coordinator) Stat(path string) (attr *backend.Attr, ok bool, tombstoned bool, baseDelta bool, valid uint32) {
	return c.overlay.Stat(path)
}

// Has returns true if path has any pending state in the overlay (including a
// tombstone). Used by Task 10b to decide whether to consult the overlay at all.
func (c *Coordinator) Has(path string) bool {
	return c.overlay.Has(path)
}

// Epoch returns the durable WAL-epoch (minted in wal.db at creation) that
// stamps this coordinator's WalOps and DelegationRequests, so the server keys
// its dedup watermark + gen-fence by (identity, volume, epoch).
func (c *Coordinator) Epoch() string {
	return c.log.Epoch()
}

// ListMerge produces a merged directory listing for dirPath, overlaying pending
// overlay state over base entries. See Overlay.ListMerge for the full contract.
func (c *Coordinator) ListMerge(dirPath string, base []backend.DirEntryPlus) []backend.DirEntryPlus {
	return c.overlay.ListMerge(dirPath, base)
}

// ReadMerge overlays pending bytes for path over the base byte slice.
// See Overlay.ReadMerge for the full contract.
func (c *Coordinator) ReadMerge(path string, off int64, base []byte) []byte {
	return c.overlay.ReadMerge(path, off, base)
}

// Xattr returns the pending xattr state for the named attribute on path.
// See Overlay.Xattr for the full contract.
func (c *Coordinator) Xattr(path, name string) (val []byte, set bool, removed bool) {
	return c.overlay.Xattr(path, name)
}

// RebuildOverlay replays all ops from the durable log (fromSeq=0) and applies
// each to the in-memory overlay. It is called at mount time when an existing
// wal.db is found after a dead-process crash, so RYOW is correct immediately
// for any delegation grants re-acquired during the current mount — before the
// async startup Replay has flushed the ops to the server.
//
// Returns nil when the log is empty (no-op, common case on first mount).
// Returns an error if the log cannot be read; in that case onLoss is called
// with reason "wal-unreadable" so the event is surfaced to ops.
func (c *Coordinator) RebuildOverlay() error {
	ops, err := c.log.Replay(0)
	if err != nil {
		if c.onLoss != nil {
			c.onLoss("wal-unreadable", nil, proto.FsError_FS_EIO)
		}
		return errors.Wrap(err, "wal: RebuildOverlay read log")
	}
	for _, op := range ops {
		c.overlay.Apply(op)
	}
	return nil
}

// StartupReplay runs the dead-process recovery flush in the background.  It
// reads all durable ops from wal.db (fromSeq=0) and sends them to the server
// via the Apply RPC.  It is called at mount time, after establishMount, when
// wal.db may contain un-acked ops from a previous crashed process (CRIT-2).
//
// The goroutine is tracked by flusherWg so Close() waits for it before
// closing the log — preventing a data race between the goroutine's
// log.Replay call and log.Close().  ctx is stored as startupReplayCancel; the
// stopFlusher path (called by Close) cancels it so a network-stuck Apply
// stream does not make Close block indefinitely.
//
// When ctx is cancelled mid-replay (i.e. a fast unmount), the goroutine logs
// at DEBUG level and exits cleanly — not an ERROR, since there is no data loss
// (ops are still in wal.db for the next mount).
//
// StartupReplay must be called at most once per Coordinator.
func (c *Coordinator) StartupReplay(ctx context.Context) {
	replayCtx, cancel := context.WithCancel(ctx)
	c.startupReplayCancel = cancel

	c.flusherWg.Add(1)
	go func() {
		defer c.flusherWg.Done()
		defer cancel()
		if err := c.Replay(replayCtx, 0); err != nil {
			// Teardown cancellation is not a data-loss event: the ops remain
			// in wal.db and will be replayed on the next mount.
			if replayCtx.Err() != nil {
				log.Log.Debug("startup WAL replay cancelled during unmount; ops remain in wal.db",
					zap.String("volume", c.cfg.volume))
				return
			}
			log.Log.Error("startup WAL replay failed; un-acked ops remain in wal.db for next mount",
				zap.String("volume", c.cfg.volume),
				zap.Error(err))
		}
	}()
}

// Close stops the interval flusher goroutine (if running), performs a final
// synchronous flush of all pending WAL ops, then closes the underlying WAL log.
//
// Ordering: stopFlusher → flusherWg.Wait → final Flush (checked) → log.Close.
// Stopping the interval goroutine first prevents a race where both the goroutine
// and the final flush attempt concurrent Apply streams.
//
// Flush failure modes have different semantics:
//   - Ordered halt (server ENOENT/EPERM etc.): processAck fires onLoss (loud
//     ERROR log + WalDataLost metric) before returning the error. The log tail
//     is truncated — ops are permanently gone. The joined error signals an
//     unclean unmount.
//   - Transport failure (server unreachable): onLoss does NOT fire and the log
//     is NOT truncated — ops remain durably in wal.db for next-mount replay
//     (CRIT-2). The joined error still signals an unclean unmount so callers
//     can log or surface it.
//
// If applyFactory is nil (coordinator built without flush options — should not
// happen after the single.go wiring is correct, but guarded defensively), the
// final flush is skipped to avoid a "no Apply factory" error that would mask the
// real empty-factory bug; the log is still closed cleanly.
//
// The Overlay and delegation.Manager have their own lifecycles and are NOT closed
// here.
func (c *Coordinator) Close() error {
	c.stopFlusher()
	c.flusherWg.Wait()

	var flushErr error
	if c.cfg.applyFactory != nil {
		ops, err := c.log.Replay(0)
		if err == nil && len(ops) > 0 {
			maxSeq := ops[len(ops)-1].Seq
			flushErr = c.Flush(context.Background(), maxSeq)
		}
	}

	closeErr := c.log.Close()
	return stderrors.Join(flushErr, closeErr)
}
