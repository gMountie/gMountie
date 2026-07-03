package wal

// flush.go — WAL flush triggers and Apply client-streaming (Task 11).
//
// # Flush contract
//
// Flush(ctx, throughSeq) streams the contiguous WAL prefix [watermark+1..throughSeq]
// to the server via the Apply client-streaming RPC and processes the single
// ApplyAck:
//
//   - Success (FailedSeq == 0): Truncate log [..ack.Watermark], clear overlay,
//     advance local watermark to ack.Watermark.
//   - Ordered halt (FailedSeq > 0): Truncate the committed prefix [..ack.Watermark],
//     clear overlay for those seqs, call onLoss for ops at/after FailedSeq, then
//     also truncate and clear the lost tail so the poisoned ops are not re-sent.
//     Returns a non-nil error wrapping the loss.
//
// # Replay
//
// Replay(ctx, resumeWatermark) fetches all ops with seq > resumeWatermark from the
// durable log and streams them via Apply. Used on reconnect: the caller passes the
// watermark from SessionResumeReply. On gen-fence or permanent failure the onLoss
// hook fires.
//
// # Thread safety
//
// A single flushMu guards all Apply streams so concurrent Fsync/interval/size
// triggers never double-send or race the watermark update. All other exported
// methods (RecordOp, Drain, etc.) remain safe; they do not hold flushMu.

import (
	"context"
	"io"
	"sync"
	"time"

	"github.com/pkg/errors"
	"go.gmountie.dev/gmountie/pkg/client/backend"
	"go.gmountie.dev/gmountie/pkg/common/fsconv"
	"go.gmountie.dev/gmountie/pkg/proto"
	"go.gmountie.dev/gmountie/pkg/utils/log"

	"go.uber.org/zap"
)

// flushConfig holds the options that enable flush behaviour. All fields are
// optional; a zero-value flushConfig means flushing is disabled (no-op).
type flushConfig struct {
	// volume is the volume name embedded in every WalOp inner request.
	// The server's Apply handler derives identity from the first op's Volume.
	volume string

	// caller is embedded in every WalOp inner request. nil is valid (server
	// falls back to session identity for squash mapping).
	caller *proto.Caller

	// applyFactory returns a fresh Apply stream. Called once per flush.
	applyFactory func(ctx context.Context) (proto.RpcFs_ApplyClient, error)

	// capOps is the maximum number of pending WAL entries before backpressure
	// kicks in. 0 means no cap.
	capOps int

	// maxApplyOps / maxApplyBytes bound a SINGLE Apply stream. A flush of a large
	// backlog (e.g. an offline/backlogged client reconnecting, or a recall flush
	// of a big delegated subtree) is split into multiple bounded Apply streams
	// instead of one unbounded stream. The server applies a stream op-by-op and
	// frees as it goes, but one continuous multi-thousand-op stream lets the Go
	// heap outrun GC under the memory limit → OOMKilled. Chunking lets the
	// handler return between batches so GC reclaims the burst. 0 ⇒ defaults below.
	maxApplyOps   int
	maxApplyBytes int
}

const (
	// defaultMaxApplyOps caps ops per Apply stream. A normal interval flush is
	// well under this; only large backlogs/replays chunk.
	defaultMaxApplyOps = 2000
	// defaultMaxApplyBytes caps total OpWrite payload bytes per Apply stream so a
	// few large writes can't blow the batch up even under the op cap.
	defaultMaxApplyBytes = 16 << 20 // 16 MiB
)

// applyLimits returns the effective per-Apply-stream bounds (defaults applied).
func (c *Coordinator) applyLimits() (maxOps, maxBytes int) {
	maxOps, maxBytes = c.cfg.maxApplyOps, c.cfg.maxApplyBytes
	if maxOps <= 0 {
		maxOps = defaultMaxApplyOps
	}
	if maxBytes <= 0 {
		maxBytes = defaultMaxApplyBytes
	}
	return maxOps, maxBytes
}

// Option is a functional option for NewCoordinator.
type Option func(*Coordinator)

// WithVolume sets the volume name embedded in flushed WalOp inner requests.
func WithVolume(volume string) Option {
	return func(c *Coordinator) { c.cfg.volume = volume }
}

// WithCaller sets the caller embedded in flushed WalOp inner requests.
func WithCaller(caller *proto.Caller) Option {
	return func(c *Coordinator) { c.cfg.caller = caller }
}

// WithApplyFactory sets the factory that returns a fresh Apply client stream.
func WithApplyFactory(f func(ctx context.Context) (proto.RpcFs_ApplyClient, error)) Option {
	return func(c *Coordinator) { c.cfg.applyFactory = f }
}

// WithOnLoss sets the hook called when ops are permanently lost after an ordered
// halt. Task 13 wires this to loud logging.
func WithOnLoss(fn func(reason string, lostOps []Op, fe proto.FsError)) Option {
	return func(c *Coordinator) {
		c.onLoss = fn
	}
}

// WithCapOps sets the backpressure cap: RecordOp blocks when the WAL has more
// than capOps pending entries, until a flush drains it below the cap.
// 0 means no cap (default).
func WithCapOps(cap int) Option {
	return func(c *Coordinator) { c.cfg.capOps = cap }
}

// WithApplyBatchLimits bounds a single Apply stream to maxOps ops and maxBytes
// of OpWrite payload; a larger flush/replay is split across multiple streams.
// Non-positive values keep the defaults. Guards the server against an OOM from
// one unbounded multi-thousand-op Apply stream (e.g. a big offline-client replay).
func WithApplyBatchLimits(maxOps, maxBytes int) Option {
	return func(c *Coordinator) {
		c.cfg.maxApplyOps = maxOps
		c.cfg.maxApplyBytes = maxBytes
	}
}

// ── applyOps ──────────────────────────────────────────────────────────────────

// applyOps opens a single Apply stream, sends every op as a WalOp, half-closes,
// and returns the server's ApplyAck. It is the single shared primitive for Flush
// and Replay. The caller holds flushMu.
//
// On stream.Send error the function stops sending and calls CloseAndRecv to get
// any partial ack the server may have buffered. On CloseAndRecv error it returns
// a nil ack and the transport error.
func (c *Coordinator) applyOps(ctx context.Context, ops []Op) (*proto.ApplyAck, error) {
	if c.cfg.applyFactory == nil {
		return nil, errors.New("wal: no Apply factory configured")
	}

	stream, err := c.cfg.applyFactory(ctx)
	if err != nil {
		log.Log.Warn("wal applyOps: failed to open Apply stream", zap.Error(err))
		return nil, errors.Wrap(err, "wal: open Apply stream")
	}

	for _, op := range ops {
		walOp := opToWalOp(op, c.cfg.volume, c.cfg.caller, c.log.Epoch())
		if serr := stream.Send(walOp); serr != nil {
			if errors.Is(serr, io.EOF) {
				// Server closed the stream early — CloseAndRecv carries the real ack.
				break
			}
			// Transport failure: try to get whatever the server recorded.
			ack, _ := stream.CloseAndRecv()
			return ack, errors.Wrap(serr, "wal: Apply Send")
		}
	}

	ack, err := stream.CloseAndRecv()
	if err != nil {
		log.Log.Warn("wal applyOps: Apply CloseAndRecv failed", zap.Error(err))
		return nil, errors.Wrap(err, "wal: Apply CloseAndRecv")
	}
	log.Log.Debug("wal applyOps: ApplyAck received",
		zap.Uint64("watermark", ack.GetWatermark()), zap.Uint64("failed_seq", ack.GetFailedSeq()),
		zap.String("fserr", ack.GetFserr().String()))
	return ack, nil
}

// ── Flush ─────────────────────────────────────────────────────────────────────

// Flush streams the contiguous WAL prefix (watermark+1..throughSeq) via Apply
// and processes the single ApplyAck. On success the log prefix is truncated and
// the overlay is cleared. On ordered halt the committed prefix is truncated and
// onLoss is called for the remaining ops.
//
// Flush is safe for concurrent callers: only one Apply stream runs at a time
// (flushMu). If no ops are pending in [watermark+1..throughSeq] it is a no-op.
func (c *Coordinator) Flush(ctx context.Context, throughSeq uint64) error {
	c.flushMu.Lock()
	defer c.flushMu.Unlock()

	ops := c.log.Prefix(throughSeq)
	// Filter to the contiguous window above the current watermark.
	wm := c.watermark.Load()
	var pending []Op
	for _, op := range ops {
		if op.Seq > wm {
			pending = append(pending, op)
		}
	}
	if len(pending) == 0 {
		return nil
	}

	maxOps, maxBytes := c.applyLimits()
	log.Log.Debug("wal Flush: streaming pending ops via Apply (chunked)",
		zap.Uint64("through_seq", throughSeq), zap.Uint64("watermark", wm),
		zap.Int("pending", len(pending)), zap.Int("max_apply_ops", maxOps))

	// Chunk the pending window into bounded Apply streams. The server applies a
	// stream op-by-op and frees as it goes, but one continuous multi-thousand-op
	// stream lets the Go heap outrun GC under the memory limit → OOMKilled.
	// Returning between batches gives the handler a GC boundary. Each batch's
	// processAck advances the watermark and truncates the flushed prefix; on
	// ordered halt or transport error processAck returns non-nil and we stop
	// (those ops were discarded or retained per that path — not re-sent here).
	for start := 0; start < len(pending); {
		end, batchBytes := start, 0
		for end < len(pending) && end-start < maxOps && batchBytes < maxBytes {
			batchBytes += len(pending[end].Data)
			end++
		}
		batch := pending[start:end]
		ack, err := c.applyOps(ctx, batch)
		if perr := c.processAck("apply-failure", ack, batch, err); perr != nil {
			return perr
		}
		start = end
	}
	return nil
}

// processAck handles the ApplyAck and transport error from applyOps.
// reason is the loss reason label passed to onLoss: "apply-failure" from Flush
// — including a recall-window flush, since FlushForRecall routes through the
// same Flush path and does not distinguish the caller (design doc §7.4) — or
// "gen-fenced" from Replay. It must be called while holding flushMu.
func (c *Coordinator) processAck(reason string, ack *proto.ApplyAck, sent []Op, transportErr error) error {
	if transportErr != nil {
		// Transport-level failure: the ops are NOT discarded (they stay in the
		// WAL for the next flush/retry), so this is not a data-loss event — but
		// it was previously silent. Log it so a persistently-failing flush is
		// visible instead of looking like nothing is happening.
		log.Log.Warn("wal flush: Apply failed at transport level; ops retained in WAL for retry",
			zap.String("reason", reason), zap.Int("ops", len(sent)), zap.Error(transportErr))
		return transportErr
	}
	if ack == nil {
		return errors.New("wal: nil ApplyAck")
	}

	committed := ack.GetWatermark()
	failedSeq := ack.GetFailedSeq()

	// Invalidate the inner cache for every flushed path BEFORE clearing the
	// overlay below. A delegated-subtree mutation went to the overlay and
	// bypassed the cache's own Create/Unlink invalidation; once commitFlushed
	// moves it to the server and clears the overlay, a read must fall through to
	// the (now authoritative) server, not a STALE cache HIT from before the
	// write — the just-created entry missing from its parent's listing (rm sees
	// an "empty" dir → rmdir → ENOTEMPTY cascade) or a stale negative attr
	// (readback ENOENT). Per-FLUSH (not per-op): the cache stays warm between
	// flushes, only just-flushed paths re-fetch once — avoiding the per-op
	// invalidation RTT storm that starved npm install. Invalidating before the
	// clear keeps reads correct from the overlay until it is cleared (no
	// overlay-empty + cache-stale window).
	if c.onFlushed != nil {
		c.onFlushed(sent)
	}

	if failedSeq != 0 && ack.GetFserr() == proto.FsError_FS_EAGAIN {
		// Transient halt: the server refused the tail (a recall was in flight
		// or a foreign delegation contended). Commit the applied prefix only;
		// ops ≥ failedSeq stay durably in the log and the overlay for the next
		// flush attempt (interval / fsync / recall). NOT a data-loss event —
		// no onLoss, no tail truncation. Implements design §7.3's
		// "transient (EAGAIN) → retry from committed_watermark + 1".
		c.commitFlushed(committed)
		c.watermark.Store(committed)
		c.capCond.Broadcast()
		return errors.Errorf("wal: Apply transient halt at seq %d (EAGAIN); tail retained for retry", failedSeq)
	}

	if failedSeq == 0 {
		// Full success: drop the flushed prefix (seq ≤ committed) from the log
		// and rebuild the overlay from the SURVIVORS (seq > committed) — the ops
		// recorded during the in-flight Apply. Clearing the whole overlay here
		// (the old DropSubtree("")) wiped those in-flight writes before they were
		// on the server → read-your-own-writes ENOENT (the npm-install failure).
		c.commitFlushed(committed)
		c.watermark.Store(committed)
		// Signal backpressure waiters.
		c.capCond.Broadcast()
		return nil
	}

	// Ordered halt: ops at/after failedSeq are lost (discarded, not re-sent).
	// committed ops were successfully applied.
	var lostOps []Op
	for _, op := range sent {
		if op.Seq >= failedSeq {
			lostOps = append(lostOps, op)
		}
	}

	// Drop the entire sent prefix (≤ lastSeq: committed ops + the poisoned lost
	// tail) and rebuild the overlay from the survivors (seq > lastSeq) so
	// concurrent in-flight writes are preserved; never re-send poisoned ops.
	if len(sent) > 0 {
		lastSeq := sent[len(sent)-1].Seq
		c.commitFlushed(lastSeq)
	}
	c.watermark.Store(committed)
	c.capCond.Broadcast()

	// Invoke the loss hook before returning the error.
	if c.onLoss != nil {
		c.onLoss(reason, lostOps, ack.GetFserr())
	}

	return errors.Errorf("wal: Apply ordered halt at seq %d: %v", failedSeq, ack.GetFserr())
}

// ── Fsync ─────────────────────────────────────────────────────────────────────

// Fsync flushes all pending WAL entries synchronously. It is called by the
// FUSE Fsync handler for delegated files. Returns FS_OK on success; otherwise
// the FsError that best describes the failure.
func (c *Coordinator) Fsync(ctx context.Context) proto.FsError {
	// Determine the current highest seq.
	ops, err := c.log.Replay(0)
	if err != nil || len(ops) == 0 {
		return proto.FsError_FS_OK
	}
	maxSeq := ops[len(ops)-1].Seq

	if err := c.Flush(ctx, maxSeq); err != nil {
		return proto.FsError_FS_EIO
	}
	return proto.FsError_FS_OK
}

// FlushForRecall implements delegation.RecallFlusher. It performs a SINGLE
// bounded flush of a barrier snapshot of the WAL — not a loop until the log
// is empty.
//
// # Why one snapshot is sufficient
//
// The Manager marks the recalled region "draining" BEFORE calling
// FlushForRecall, and RecordOp's admission check + append are atomic under
// recordMu (the same mutex this function takes for its snapshot). That
// makes taking recordMu once a correct barrier for the recalled region:
//
//   - Every op already admitted for the recalled region completed its
//     append before this snapshot could acquire recordMu, so it is in
//     ops.
//   - Every op that ARRIVES for the recalled region after the snapshot is
//     refused admission (ErrNotDelegated) — draining was set before we were
//     ever called, so there is no window in which a new op for this region
//     can land outside the snapshot.
//
// The result is a bounded, contiguous prefix through ops[len(ops)-1].Seq — a
// correct superset of everything the recalled root will ever need flushed
// (design doc §7.4: recall flushes the contiguous prefix up to the last op
// touching the recalled delegation, not the whole future of the log).
//
// # Why NOT loop until empty
//
// Ops admitted after the snapshot for OTHER, non-draining regions are
// deliberately left deferred — flushing them is not this recall's job. A
// loop-until-empty variant would keep re-snapshotting and re-flushing as long
// as ANY region keeps writing, so sustained concurrent writes to unrelated
// delegated subtrees could keep the log perpetually non-empty and stall the
// recall handoff indefinitely. A single bounded flush cannot be starved by
// unrelated write traffic.
func (c *Coordinator) FlushForRecall(ctx context.Context, root string) error {
	c.recordMu.Lock()
	ops, err := c.log.Replay(0)
	c.recordMu.Unlock()
	if err != nil {
		return errors.Wrap(err, "wal: FlushForRecall read log")
	}
	if len(ops) == 0 {
		return nil
	}
	return c.Flush(ctx, ops[len(ops)-1].Seq)
}

// ── Replay ────────────────────────────────────────────────────────────────────

// Replay is called on reconnect: it fetches all ops with seq > resumeWatermark
// from the durable log and streams them to the server via Apply. On ordered
// halt or gen-fence the onLoss hook fires.
func (c *Coordinator) Replay(ctx context.Context, resumeWatermark uint64) error {
	ops, err := c.log.Replay(resumeWatermark)
	if err != nil {
		return errors.Wrap(err, "wal: Replay read log")
	}
	if len(ops) == 0 {
		return nil
	}

	c.flushMu.Lock()
	defer c.flushMu.Unlock()

	ack, applyErr := c.applyOps(ctx, ops)
	return c.processAck("gen-fenced", ack, ops, applyErr)
}

// ── Op → WalOp conversion ─────────────────────────────────────────────────────

// opToWalOp converts a durable Op to the proto WalOp sent on the Apply stream.
// volume and caller are embedded in the inner request so the server can resolve
// the (identity, volume) key from the first op.
//
// The Seq and Gen fields on WalOp carry the durable sequence and delegation gen.
// RequestId is derived deterministically from Seq (hex string) so re-sent ops
// carry the same id and pass server-side idempotency guards.
func opToWalOp(op Op, volume string, caller *proto.Caller, walEpoch string) *proto.WalOp {
	w := &proto.WalOp{Seq: op.Seq, Gen: op.Gen, WalEpoch: walEpoch}

	switch op.Kind {
	case OpWrite:
		w.Op = &proto.WalOp_Write{Write: &proto.WriteOp{
			Volume: volume,
			Caller: caller,
			Path:   op.Path,
			Offset: op.Offset,
			Data:   op.Data,
		}}

	case OpCreate:
		w.Op = &proto.WalOp_Create{Create: &proto.CreateRequest{
			Volume: volume,
			Caller: caller,
			Path:   op.Path,
			Flags:  op.Flags,
			Mode:   op.Mode,
		}}

	case OpMkdir:
		w.Op = &proto.WalOp_Mkdir{Mkdir: &proto.MkdirRequest{
			Volume: volume,
			Caller: caller,
			Path:   op.Path,
			Mode:   op.Mode,
		}}

	case OpUnlink:
		w.Op = &proto.WalOp_Unlink{Unlink: &proto.UnlinkRequest{
			Volume: volume,
			Caller: caller,
			Path:   op.Path,
		}}

	case OpRmdir:
		w.Op = &proto.WalOp_Rmdir{Rmdir: &proto.RmdirRequest{
			Volume: volume,
			Caller: caller,
			Path:   op.Path,
		}}

	case OpRename:
		w.Op = &proto.WalOp_Rename{Rename: &proto.RenameRequest{
			Volume:  volume,
			Caller:  caller,
			OldName: op.Path,
			NewName: op.NewPath,
		}}

	case OpSymlink:
		w.Op = &proto.WalOp_Symlink{Symlink: &proto.SymlinkRequest{
			Volume:   volume,
			Caller:   caller,
			Target:   string(op.Data),
			LinkPath: op.Path,
		}}

	case OpSetAttr:
		sa := &proto.SetAttrRequest{
			Volume: volume,
			Caller: caller,
			Path:   op.Path,
			Valid:  op.Valid,
		}
		if op.Valid&backend.FATTR_MODE != 0 {
			sa.Mode = op.Mode
		}
		if op.Valid&backend.FATTR_UID != 0 {
			sa.Uid = op.UID
		}
		if op.Valid&backend.FATTR_GID != 0 {
			sa.Gid = op.GID
		}
		if op.Valid&backend.FATTR_SIZE != 0 {
			sa.Size = op.Size
		}
		if op.Valid&backend.FATTR_ATIME != 0 {
			sa.Atime = &proto.FileTime{Sec: uint64(op.AtimeSec), Nsec: op.AtimeNsec}
		}
		if op.Valid&backend.FATTR_MTIME != 0 {
			sa.Mtime = &proto.FileTime{Sec: uint64(op.MtimeSec), Nsec: op.MtimeNsec}
		}
		w.Op = &proto.WalOp_SetAttr{SetAttr: sa}

	case OpSetXAttr:
		w.Op = &proto.WalOp_SetXattr{SetXattr: &proto.SetXAttrRequest{
			Volume:    volume,
			Caller:    caller,
			Path:      op.Path,
			Attribute: op.XattrName,
			Data:      op.XattrValue,
			Flags:     fsconv.XAttrModeToProto(int(op.XattrFlags)),
		}}

	case OpRemoveXAttr:
		w.Op = &proto.WalOp_RemoveXattr{RemoveXattr: &proto.RemoveXAttrRequest{
			Volume:    volume,
			Caller:    caller,
			Path:      op.Path,
			Attribute: op.XattrName,
		}}

	default:
		// Unknown kind: emit a Release no-op marker so the stream can continue.
		// The server advances the watermark for ReleaseOp without a fs call.
		w.Op = &proto.WalOp_Release{Release: &proto.ReleaseOp{
			Volume: volume,
			Caller: caller,
			Path:   op.Path,
		}}
	}

	return w
}

// ── interval goroutine ────────────────────────────────────────────────────────

// StartIntervalFlusher starts a single background goroutine that flushes the
// WAL on every tick of a time.Ticker with period d. The goroutine is tracked
// by flusherWg so Coordinator.Close() can wait for it to exit (ensuring no
// in-flight log access after log.Close).
//
// StartIntervalFlusher must be called at most once per Coordinator; calling it
// after Close is a no-op (flushStop is already closed, the goroutine exits
// immediately on the first select). It is the caller's responsibility to call
// Close to stop the goroutine and avoid a leak.
func (c *Coordinator) StartIntervalFlusher(d time.Duration) {
	t := time.NewTicker(d)
	c.flusherWg.Add(1)
	go func() {
		defer c.flusherWg.Done()
		defer t.Stop()
		for {
			select {
			case <-c.flushStop:
				return
			case <-t.C:
				ops, err := c.log.Replay(0)
				if err != nil {
					log.Log.Warn("wal interval flusher: log.Replay failed", zap.Error(err))
					continue
				}
				if len(ops) == 0 {
					continue
				}
				log.Log.Debug("wal interval flusher: tick", zap.Int("ops_in_log", len(ops)), zap.Uint64("max_seq", ops[len(ops)-1].Seq))
				// Use a background context — the interval flush is best-effort.
				_ = c.Flush(context.Background(), ops[len(ops)-1].Seq)
			}
		}
	}()
}

// stopFlusher is called by Close to stop the interval goroutine and, if
// StartupReplay was called, to cancel the startup replay context so a
// network-stuck Apply stream does not block flusherWg.Wait indefinitely.
func (c *Coordinator) stopFlusher() {
	c.flushStopOnce.Do(func() {
		if c.flushStop != nil {
			close(c.flushStop)
		}
		if c.startupReplayCancel != nil {
			c.startupReplayCancel()
		}
	})
}

// ── size-cap helpers ──────────────────────────────────────────────────────────

// pendingCount returns the number of ops in the log above the current watermark.
// This is an approximate count used for backpressure — exact consistency is not
// required (the log Replay is the authoritative sequence).
func (c *Coordinator) pendingCount() int {
	ops, err := c.log.Replay(c.watermark.Load())
	if err != nil {
		return 0
	}
	return len(ops)
}

// waitForCap blocks until the pending WAL count drops below cfg.capOps.
// Must be called WITHOUT flushMu held (it needs to release capMu for the
// flusher to signal capCond). capMu is the mutex on capCond.
//
// At most ONE drain flush runs at a time: the flushing flag is set before
// the goroutine is spawned and cleared (under capMu) with a Broadcast after
// Flush returns. This prevents unbounded goroutine accumulation under
// sustained over-cap pressure.
func (c *Coordinator) waitForCap() {
	if c.cfg.capOps == 0 {
		return
	}
	c.capMu.Lock()
	defer c.capMu.Unlock()
	for c.pendingCount() >= c.cfg.capOps {
		// Spawn a drain flush only when no flush is already in flight.
		if c.flushing.CompareAndSwap(false, true) {
			go func() {
				ops, err := c.log.Replay(c.watermark.Load())
				if err == nil && len(ops) > 0 {
					maxSeq := ops[len(ops)-1].Seq
					_ = c.Flush(context.Background(), maxSeq)
				}
				// Signal under capMu so no wakeup can be missed.
				c.capMu.Lock()
				c.flushing.Store(false)
				c.capCond.Broadcast()
				c.capMu.Unlock()
			}()
		}
		c.capCond.Wait()
	}
}

// flushMuType is a named alias so wal.go's struct field is explicit in godoc.
type flushMuType = sync.Mutex
