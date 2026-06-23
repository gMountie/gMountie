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
	"sync"
	"sync/atomic"

	"github.com/pkg/errors"
	"go.gmountie.dev/gmountie/pkg/client/backend"
	"go.gmountie.dev/gmountie/pkg/client/backend/delegation"
	"go.gmountie.dev/gmountie/pkg/client/backend/transport"
	"go.gmountie.dev/gmountie/pkg/proto"
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
	capMu    sync.Mutex
	capCond  *sync.Cond
	// flushing is set to true while a background drain flush is in flight,
	// ensuring at most one such goroutine runs at a time (see waitForCap).
	flushing atomic.Bool

	// flushStop and flushStopOnce control the interval goroutine lifecycle.
	flushStop     chan struct{}
	flushStopOnce sync.Once
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

// Close closes the underlying WAL log. The Overlay and delegation.Manager have
// their own lifecycles and are NOT closed here.
func (c *Coordinator) Close() error {
	return c.log.Close()
}
