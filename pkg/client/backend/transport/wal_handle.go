// wal_handle.go — composable per-fd write-coalescing handle.
//
// walHandle wraps a *grpcFileHandle leaf and owns the three pieces of per-fd
// write state that the WAL layer (Task 10) needs to intercept:
//
//   - WriteCoalescer (small-write accumulation)
//   - dirty flag    (Flush fast-path gate)
//   - lastWriteErr  (sticky write-back error propagated on Flush/Release)
//
// These fields are lifted OUT of grpcFileHandle so the WAL backend can insert
// itself at the write/flush/release seam without touching the leaf's fd-level
// RPC primitives.
//
// # Drain seam (WriteDrain, Task 10 extension point)
//
// walHandle.flush delegates the fused WriteAndFlush call to a WriteDrain
// interface. The default implementation (writeDrainToWire) drives
// BackendClient.writeAndFlushImpl and preserves the one-RTT fusion. Task 10
// replaces the drain target with a WAL drain that appends the op to the WAL
// log before forwarding to the leaf; the walHandle itself is unmodified.
//
// # Obstacle resolution (from Task 9b spec)
//
// (a) No import cycle: walHandle lives in the transport package. The wal
// package (pkg/client/backend/wal) has no dependency on transport; Task 10
// will pass its drain target as a WriteDrain to newWalHandle — the wal package
// imports transport, not the other way around.
//
// (b) Characterization tests exercise BackendClient (the transport layer).
// Open/Create and the test newHandle helper wrap the leaf in walHandle when
// WriteCoalesceBytes > 0, so coalescing is still visible through BackendClient.
package transport

import (
	"context"
	"sync/atomic"

	"go.gmountie.dev/gmountie/pkg/client/backend"
	"go.gmountie.dev/gmountie/pkg/proto"
	"go.gmountie.dev/gmountie/pkg/utils/log"

	"github.com/google/uuid"
	"go.uber.org/zap"
)

// WriteDrain is the exported seam the WAL layer (Task 10) plugs into.
// walHandle calls it on Flush to perform the fused drain+flush in one RTT.
//
// The default implementation (writeDrainToWire) writes straight to the wire
// via BackendClient.writeAndFlushImpl. Task 10 injects a wal.DrainTarget that
// appends an Op to BboltLog before forwarding to the same leaf primitive.
//
// Contract:
//   - pendingData/pendingOff are the snapshot from the coalescer; both are
//     zero/nil when the coalescer was empty (pure Flush with no buffered bytes).
//   - requestID is allocated once by walHandle before any retry so the server's
//     idempotency cache can short-circuit a replay of the write half.
//   - leafFlush is the primitive that commits to the wire (WriteAndFlush RPC).
//     Drain implementations must call it exactly once when they are ready to
//     commit (after any WAL append). Returning its error.
type WriteDrain interface {
	Drain(
		ctx context.Context,
		pendingData []byte,
		pendingOff int64,
		requestID string,
		leafFlush func(ctx context.Context, data []byte, off int64, reqID string) proto.FsError,
	) proto.FsError
}

// writeDrainToWire is the default WriteDrain: calls the leaf's WriteAndFlush
// RPC immediately with no WAL recording. This is byte-identical to the
// pre-refactor BackendClient.Flush body.
type writeDrainToWire struct{}

func (writeDrainToWire) Drain(
	ctx context.Context,
	pendingData []byte,
	pendingOff int64,
	requestID string,
	leafFlush func(ctx context.Context, data []byte, off int64, reqID string) proto.FsError,
) proto.FsError {
	return leafFlush(ctx, pendingData, pendingOff, requestID)
}

// walHandle is a composable per-fd handle that owns write-coalescing state and
// wraps a *grpcFileHandle leaf for all non-coalescing fd ops.
//
// It satisfies backend.FileHandle via Path() + Unwrap(). BackendClient detects
// *walHandle via resolveWalHandle and delegates Write/Flush/Fsync/Release to
// the handle's own methods; all other ops (Read, Allocate, Lseek, locks, …)
// reach the leaf through resolveHandle's Unwrap chain.
type walHandle struct {
	leaf      *grpcFileHandle
	b         *BackendClient
	coalescer *WriteCoalescer
	threshold int
	drain     WriteDrain // nil-safe default: writeDrainToWire{}

	dirty        atomic.Bool
	lastWriteErr atomic.Int32
}

// compile-time check: walHandle must satisfy backend.FileHandle.
var _ backend.FileHandle = (*walHandle)(nil)

// newWalHandle wraps leaf in a walHandle. b is retained so the handle can call
// b.streamingWrite and b.writeAndFlushImpl. drain may be nil, in which case
// writeDrainToWire is used (wire-straight, one-RTT WriteAndFlush).
//
// When threshold == 0, no WriteCoalescer is created (coalescer stays nil) and
// write() passes straight to streamingWrite. This allows newWalHandle to be
// used uniformly regardless of whether coalescing is enabled.
func newWalHandle(b *BackendClient, leaf *grpcFileHandle, threshold int, drain WriteDrain) *walHandle {
	if drain == nil {
		drain = writeDrainToWire{}
	}
	wh := &walHandle{
		leaf:      leaf,
		b:         b,
		threshold: threshold,
		drain:     drain,
	}
	if threshold > 0 {
		wh.coalescer = NewWriteCoalescer(threshold)
	}
	return wh
}

// Path returns the file path this handle was opened against. Satisfies backend.FileHandle.
func (wh *walHandle) Path() string { return wh.leaf.path }

// Unwrap returns the leaf so resolveHandle still reaches *grpcFileHandle for
// non-coalescing ops. Satisfies backend.FileHandle.Unwrap().
func (wh *walHandle) Unwrap() backend.FileHandle { return wh.leaf }

// resolveWalHandle walks the FileHandle chain looking for a *walHandle. Returns
// nil if none is present. Callers that need coalescing state (Write/Flush/Fsync/
// Release) use this; resolveHandle is used for leaf-only access.
func resolveWalHandle(fh backend.FileHandle) *walHandle {
	cur := fh
	for cur != nil {
		if wh, ok := cur.(*walHandle); ok {
			return wh
		}
		next := cur.Unwrap()
		if next == cur {
			return nil
		}
		cur = next
	}
	return nil
}

// ── write-back error bookkeeping ─────────────────────────────────────────────

// recordWriteErr stickily records a non-OK status from a failed coalesced
// flush whose bytes were already snapshot-and-cleared from the coalescer.
// First-wins CAS; FS_OK is a no-op.
func (wh *walHandle) recordWriteErr(st proto.FsError) {
	if st == proto.FsError_FS_OK {
		return
	}
	wh.lastWriteErr.CompareAndSwap(int32(proto.FsError_FS_OK), int32(st))
}

// takeWriteErr atomically reads and clears the sticky write-back error.
// Returns FS_OK when none is pending.
func (wh *walHandle) takeWriteErr() proto.FsError {
	return proto.FsError(wh.lastWriteErr.Swap(int32(proto.FsError_FS_OK)))
}

// ── coalescing write path ─────────────────────────────────────────────────────

// write implements the coalescing write logic. Called by BackendClient.Write
// when it resolves a *walHandle. Mirrors the pre-refactor grpcFileHandle write
// path exactly:
//
//   - marks dirty before buffering (preserves the dirty⇒bytes-in-flight invariant)
//   - big writes (≥threshold) drain pending bytes first, then send the big write
//   - small writes append to the coalescer; a full batch is flushed immediately
//   - failed batch flushes are stickily recorded so the next Flush surfaces them
func (wh *walHandle) write(data []byte, off int64) (uint32, proto.FsError) {
	wh.dirty.Store(true)
	if h := wh.coalescer; h == nil {
		// Should not happen (walHandle is only constructed with threshold>0),
		// but guard defensively.
		return wh.b.streamingWrite(wh.leaf, data, off, uuid.NewString())
	}
	// Big write: drain pending first, then send big directly.
	if len(data) >= wh.threshold {
		if pending := wh.coalescer.Drain(); pending != nil {
			if _, st := wh.b.streamingWrite(wh.leaf, pending.Data, pending.Offset, uuid.NewString()); st != proto.FsError_FS_OK {
				wh.recordWriteErr(st)
				return 0, st
			}
		}
		return wh.b.streamingWrite(wh.leaf, data, off, uuid.NewString())
	}
	// Small write: append to coalescer; flush batch if returned.
	if batch := wh.coalescer.Append(data, off); batch != nil {
		if _, st := wh.b.streamingWrite(wh.leaf, batch.Data, batch.Offset, uuid.NewString()); st != proto.FsError_FS_OK {
			wh.recordWriteErr(st)
			return 0, st
		}
	}
	return uint32(len(data)), proto.FsError_FS_OK
}

// drainCoalescer flushes any pending coalesced bytes to the wire via a
// streaming Write RPC. Returns FS_OK on empty or nil coalescer.
func (wh *walHandle) drainCoalescer() proto.FsError {
	if wh.coalescer == nil {
		return proto.FsError_FS_OK
	}
	pending := wh.coalescer.Drain()
	if pending == nil {
		return proto.FsError_FS_OK
	}
	_, st := wh.b.streamingWrite(wh.leaf, pending.Data, pending.Offset, uuid.NewString())
	return st
}

// ── Flush ─────────────────────────────────────────────────────────────────────

// flush implements the Flush durability boundary for walHandle. Mirrors the
// pre-refactor BackendClient.Flush body exactly, preserving the one-RTT
// WriteAndFlush fusion via the WriteDrain seam:
//
//  1. Surface sticky write-back error (takeWriteErr); skip RPC if set.
//  2. Clean-handle fast path: skip RPC if !dirty.
//  3. Drain the coalescer in memory (pending may be nil).
//  4. Generate a requestID once (outside any retry) for idempotency.
//  5. Call wh.drain.Drain → leafFlush (WriteAndFlush RPC) — one RTT.
//  6. Clear dirty on success.
func (wh *walHandle) flush(ctx context.Context) proto.FsError {
	if st := wh.takeWriteErr(); st != proto.FsError_FS_OK {
		return st
	}
	if !wh.dirty.Load() {
		return proto.FsError_FS_OK
	}
	var pendingOff int64
	var pendingData []byte
	if wh.coalescer != nil {
		if pending := wh.coalescer.Drain(); pending != nil {
			pendingOff = pending.Offset
			pendingData = pending.Data
		}
	}
	requestID := uuid.NewString()
	leafFlush := func(ctx context.Context, data []byte, off int64, reqID string) proto.FsError {
		return wh.b.writeAndFlushImpl(ctx, wh.leaf, data, off, reqID)
	}
	st := wh.drain.Drain(ctx, pendingData, pendingOff, requestID, leafFlush)
	if st == proto.FsError_FS_OK {
		wh.dirty.Store(false)
	}
	return st
}

// ── Fsync ─────────────────────────────────────────────────────────────────────

// fsyncPrep surfaces the sticky write-back error and drains the coalescer
// before the Fsync RPC is issued. Returns non-OK to short-circuit the RPC.
// On success the caller issues the Fsync RPC and calls clearDirty().
func (wh *walHandle) fsyncPrep() proto.FsError {
	if st := wh.takeWriteErr(); st != proto.FsError_FS_OK {
		return st
	}
	return wh.drainCoalescer()
}

// clearDirty clears the dirty flag. Called by BackendClient.Fsync on success.
func (wh *walHandle) clearDirty() { wh.dirty.Store(false) }

// ── Release ───────────────────────────────────────────────────────────────────

// release implements the coalescing-state cleanup for Release. Logs the sticky
// write-back error (if any) and drains the coalescer best-effort. The leaf
// Release RPC is issued by BackendClient.Release after this call.
//
// Ordering mirrors the pre-refactor BackendClient.Release body:
//
//	takeWriteErr → log sticky error → drainCoalescer → log drain error
func (wh *walHandle) release() {
	if st := wh.takeWriteErr(); st != proto.FsError_FS_OK {
		log.Log.Error("sticky write-back error on Release: acked bytes were lost in an earlier coalesced flush",
			zap.String("path", wh.leaf.path), zap.Stringer("status", st))
	}
	if st := wh.drainCoalescer(); st != proto.FsError_FS_OK {
		log.Log.Error("error draining coalescer on Release",
			zap.String("path", wh.leaf.path), zap.Stringer("status", st))
	}
}

// ── CopyFileRange helper ──────────────────────────────────────────────────────

// coalescerDrainFor drains the coalescer for fh if it is a *walHandle, or
// falls back to the BackendClient's legacy drainCoalescer for a raw
// *grpcFileHandle. Used by CopyFileRange and Lseek which drain pending bytes
// before server-side operations that must observe current file state.
//
// Deprecated path (grpcFileHandle with coalescer) is unreachable after Task 9b:
// all coalesced handles are walHandles. The fallback guard remains for safety.
func (b *BackendClient) coalescerDrainFor(fh backend.FileHandle) proto.FsError {
	if wh := resolveWalHandle(fh); wh != nil {
		return wh.drainCoalescer()
	}
	// Legacy: grpcFileHandle still has a coalescer (only reached pre-Task-9b or
	// when coalescing is disabled — in the latter case Drain is a no-op anyway).
	if h := resolveHandle(fh); h != nil {
		return b.drainCoalescer(h)
	}
	return proto.FsError_FS_EBADF
}
