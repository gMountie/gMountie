package wal

// loss.go — loud, file-enumerating data-loss logging + metric (Task 13).
//
// logDataLost is the onLoss implementation: on ANY discard of un-flushed WAL
// ops it emits a SINGLE ERROR log that enumerates EVERY lost file path (not a
// count), plus reason, seq range, (identity, volume), and the FsError. It also
// increments the WalDataLost metric (events +1, files +len(distinct paths)).
//
// Wire: logDataLost is called from the ordered-halt path in processAck (Flush
// / Replay both route through it) and from any startup WAL-unreadable path
// (Task 14). The Coordinator carries logDataLostHook (its Key) as the default
// onLoss; callers may override via WithOnLoss.
//
// Metrics seam: a package-level *metrics.Metrics handle (walMetrics, nil-safe)
// lets tests inject a fresh registry without importing the global prometheus
// default. Set it once on mount via SetMetrics. Production builds call
// SetMetrics(m) when wiring the client; tests supply a private instance.

import (
	"go.gmountie.dev/gmountie/pkg/client/metrics"
	"go.gmountie.dev/gmountie/pkg/proto"
	"go.gmountie.dev/gmountie/pkg/utils/log"
	"go.uber.org/zap"
)

// Key identifies the (identity, volume) pair for a WAL Coordinator. Identity
// is the server-side username of the authenticated caller (from
// cfg.caller.Owner.UserName, or "" for squash mode). Volume is the target
// volume name.
type Key struct {
	Identity string
	Volume   string
}

// walMetrics is the package-level metrics handle used by logDataLost.
// Nil-safe: all callers check for nil before use. Set once on mount via
// SetMetrics; never mutated concurrently with log calls.
var walMetrics *metrics.Metrics

// SetMetrics wires the package-level metrics handle used by logDataLost.
// Call once at mount time before any Coordinator is used. Safe to call
// multiple times (last write wins); not safe for concurrent mutation with
// active logDataLost calls (set it before the coordinator starts running).
func SetMetrics(m *metrics.Metrics) { walMetrics = m }

// logDataLost is the default onLoss implementation. It emits a single ERROR
// log entry that enumerates EVERY lost file path (zap.Strings "lost_paths"),
// then increments WalDataLost (events +1, files +distinctPaths).
//
// reason identifies the loss path:
//   - "apply-failure"         — server ordered-halt during Flush
//   - "gen-fenced"            — server rejected Replay (stale generation)
//   - "recall-flush-failure"  — delegation-recall flush failed (Task 12)
//   - "wal-unreadable"        — WAL db could not be read on startup (Task 14)
//
// key carries the (identity, volume) of the Coordinator that suffered the loss.
func logDataLost(reason string, key Key, lostOps []Op, fserr proto.FsError) {
	if len(lostOps) == 0 {
		return
	}

	// Collect all lost paths (preserving order; duplicates are included so the
	// log faithfully reflects the op stream rather than a deduped set).
	paths := make([]string, len(lostOps))
	for i, op := range lostOps {
		paths[i] = op.Path
	}

	// Distinct path count for the metric (dedup here only).
	distinct := distinctPaths(paths)

	minSeq := lostOps[0].Seq
	maxSeq := lostOps[len(lostOps)-1].Seq

	log.Log.Error("WAL data loss: un-flushed ops discarded without reaching the server",
		zap.String("reason", reason),
		zap.String("identity", key.Identity),
		zap.String("volume", key.Volume),
		zap.Uint64("seq_min", minSeq),
		zap.Uint64("seq_max", maxSeq),
		zap.Int("lost_op_count", len(lostOps)),
		zap.Strings("lost_paths", paths),
		zap.String("fserr", fserr.String()),
	)

	if walMetrics != nil {
		walMetrics.WalDataLostEventInc(reason)
		walMetrics.WalDataLostFilesAdd(reason, distinct)
	}
}

// distinctPaths returns the count of distinct non-empty strings in paths.
func distinctPaths(paths []string) int {
	seen := make(map[string]struct{}, len(paths))
	for _, p := range paths {
		seen[p] = struct{}{}
	}
	return len(seen)
}

// defaultOnLoss returns the default onLoss closure for a Coordinator: it
// captures key and calls logDataLost. Wired automatically when no WithOnLoss
// override is provided. The Coordinator constructor calls this after all opts
// are applied so key is derived from the final cfg.
func (c *Coordinator) defaultOnLoss() func(reason string, lostOps []Op, fe proto.FsError) {
	key := Key{
		Identity: c.cfg.caller.GetOwner().GetUserName(),
		Volume:   c.cfg.volume,
	}
	return func(reason string, lostOps []Op, fe proto.FsError) {
		logDataLost(reason, key, lostOps, fe)
	}
}
