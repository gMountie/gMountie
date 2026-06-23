package wal

// loss_test.go — TDD tests for Task 13: loud file-enumerating data-loss
// logging + WalDataLost metric.
//
// Test coverage:
//   1. logDataLost emits exactly ONE ERROR log entry with EVERY lost file path
//      in zap.Strings("lost_paths"), plus reason/seq-range/identity/volume/fserr.
//      A count-only log (no paths) fails the test.
//   2. logDataLost increments WalDataLost: events+1, files+N (distinct paths).
//   3. logDataLost with no ops is a no-op (no log, no metric increment).
//   4. Integration — flush ordered-halt with onLoss=default: the loud log
//      enumerates the lost-tail paths and the metric increments (uses the Task-11
//      flush harness via FlushSuite embed).
//   5. Integration — Replay gen-fence (ordered-halt on Replay): same loud log
//      with reason="gen-fenced" and metric increments.
//
// Observer pattern: zaptest/observer is installed over log.Log (same pattern
// as transport.CoalesceCharacterizationSuite). The package-level walMetrics is
// set to a fresh *metrics.Metrics in each test and restored in Cleanup.

import (
	"context"
	"fmt"
	"testing"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/suite"
	"go.gmountie.dev/gmountie/pkg/client/backend/delegation"
	"go.gmountie.dev/gmountie/pkg/client/metrics"
	"go.gmountie.dev/gmountie/pkg/proto"
	"go.gmountie.dev/gmountie/pkg/utils/log"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"go.uber.org/zap/zaptest/observer"
)

// ── helpers ───────────────────────────────────────────────────────────────────

// ifaceSliceToStrings converts the []interface{} that zapcore.MapObjectEncoder
// produces for zap.Strings fields into a plain []string. Panics (test failure)
// if the conversion cannot be made.
func ifaceSliceToStrings(raw interface{}) []string {
	sl, ok := raw.([]interface{})
	if !ok {
		panic(fmt.Sprintf("expected []interface{} from zap MapObjectEncoder, got %T", raw))
	}
	result := make([]string, len(sl))
	for i, v := range sl {
		str, isStr := v.(string)
		if !isStr {
			panic(fmt.Sprintf("expected string element in lost_paths, got %T", v))
		}
		result[i] = str
	}
	return result
}

// installObserver replaces log.Log with a zaptest observer at ErrorLevel and
// returns the observed logs. Restores log.Log in t.Cleanup.
func installObserver(t *testing.T) *observer.ObservedLogs {
	t.Helper()
	core, observed := observer.New(zapcore.ErrorLevel)
	orig := log.Log
	log.Log = zap.New(core)
	t.Cleanup(func() { log.Log = orig })
	return observed
}

// installTestMetrics replaces walMetrics with a fresh *metrics.Metrics and
// returns it. Restores the original walMetrics in t.Cleanup.
func installTestMetrics(t *testing.T) *metrics.Metrics {
	t.Helper()
	m := metrics.NewMetrics()
	orig := walMetrics
	walMetrics = m
	t.Cleanup(func() { walMetrics = orig })
	return m
}

// ── LossLoggingSuite ──────────────────────────────────────────────────────────

type LossLoggingSuite struct {
	suite.Suite
}

// TestLogDataLost_EmitsOneErrorWithEveryLostPath verifies that logDataLost emits
// exactly ONE ERROR log entry whose zap "lost_paths" field contains EVERY lost
// file path (not just a count).
func (s *LossLoggingSuite) TestLogDataLost_EmitsOneErrorWithEveryLostPath() {
	observed := installObserver(s.T())
	m := installTestMetrics(s.T())

	lostOps := []Op{
		{Seq: 1, Kind: OpCreate, Path: "dir/a.txt"},
		{Seq: 2, Kind: OpWrite, Path: "dir/a.txt"},
		{Seq: 3, Kind: OpCreate, Path: "dir/b.txt"},
		{Seq: 4, Kind: OpMkdir, Path: "dir/sub"},
	}
	key := Key{Identity: "alice", Volume: "vol1"}

	logDataLost("apply-failure", key, lostOps, proto.FsError_FS_EIO)

	// Exactly ONE error log entry.
	s.Require().Equal(1, observed.Len(), "logDataLost must emit exactly one ERROR log entry")

	entry := observed.All()[0]
	s.Assert().Equal(zapcore.ErrorLevel, entry.Level, "log entry must be at ERROR level")

	fields := entry.ContextMap()

	// reason field
	s.Assert().Equal("apply-failure", fields["reason"], "reason field must be present")

	// identity + volume
	s.Assert().Equal("alice", fields["identity"], "identity field must be present")
	s.Assert().Equal("vol1", fields["volume"], "volume field must be present")

	// seq range (zap.Uint64 stores as uint64 in the context map)
	s.Assert().Equal(uint64(1), fields["seq_min"], "seq_min must be the lowest seq")
	s.Assert().Equal(uint64(4), fields["seq_max"], "seq_max must be the highest seq")

	// fserr
	s.Assert().Equal("FS_EIO", fields["fserr"], "fserr field must be present")

	// lost_paths must enumerate EVERY lost path (not a count).
	// zap.Strings is encoded by MapObjectEncoder as []interface{} — convert via helper.
	rawPaths, ok := fields["lost_paths"]
	s.Require().True(ok, "lost_paths field must be present in the log entry")
	paths := ifaceSliceToStrings(rawPaths)
	s.Require().Len(paths, 4, "lost_paths must contain all 4 op paths")
	for _, op := range lostOps {
		s.Assert().Contains(paths, op.Path, "lost_paths must contain path %q", op.Path)
	}

	// Metric: events +1, files = distinct paths (3: dir/a.txt, dir/b.txt, dir/sub).
	s.Assert().Equal(1.0, testutil.ToFloat64(m.WalDataLost.WithLabelValues("apply-failure", "events")),
		"WalDataLost events must be incremented by 1")
	s.Assert().Equal(3.0, testutil.ToFloat64(m.WalDataLost.WithLabelValues("apply-failure", "files")),
		"WalDataLost files must be incremented by the number of DISTINCT lost paths")
}

// TestLogDataLost_NoOpsIsNoOp verifies that logDataLost with an empty op slice
// emits no log and does not increment the metric.
func (s *LossLoggingSuite) TestLogDataLost_NoOpsIsNoOp() {
	observed := installObserver(s.T())
	m := installTestMetrics(s.T())

	logDataLost("apply-failure", Key{Identity: "bob", Volume: "v"}, nil, proto.FsError_FS_OK)

	s.Assert().Equal(0, observed.Len(), "no log must be emitted for an empty lostOps slice")
	s.Assert().Equal(0.0, testutil.ToFloat64(m.WalDataLost.WithLabelValues("apply-failure", "events")),
		"WalDataLost events must not be incremented for empty lostOps")
}

// TestLogDataLost_MetricIncrementsEventsPlusDistinctFiles verifies the exact
// metric values: events +1, files = distinct paths even when ops repeat paths.
func (s *LossLoggingSuite) TestLogDataLost_MetricIncrementsEventsPlusDistinctFiles() {
	installObserver(s.T())
	m := installTestMetrics(s.T())

	// 5 ops, only 2 distinct paths.
	lostOps := []Op{
		{Seq: 10, Path: "x.txt"},
		{Seq: 11, Path: "y.txt"},
		{Seq: 12, Path: "x.txt"},
		{Seq: 13, Path: "x.txt"},
		{Seq: 14, Path: "y.txt"},
	}

	logDataLost("gen-fenced", Key{Volume: "myvol"}, lostOps, proto.FsError_FS_EPERM)

	s.Assert().Equal(1.0, testutil.ToFloat64(m.WalDataLost.WithLabelValues("gen-fenced", "events")))
	s.Assert().Equal(2.0, testutil.ToFloat64(m.WalDataLost.WithLabelValues("gen-fenced", "files")),
		"files metric must count DISTINCT paths, not total op count")
}

// TestLogDataLost_ReasonLabel verifies that different reasons produce
// independent counter series (no cross-contamination).
func (s *LossLoggingSuite) TestLogDataLost_ReasonLabel() {
	installObserver(s.T())
	m := installTestMetrics(s.T())

	ops := []Op{{Seq: 1, Path: "a/b"}}

	logDataLost("apply-failure", Key{Volume: "v"}, ops, proto.FsError_FS_EIO)
	logDataLost("gen-fenced", Key{Volume: "v"}, ops, proto.FsError_FS_EIO)

	s.Assert().Equal(1.0, testutil.ToFloat64(m.WalDataLost.WithLabelValues("apply-failure", "events")))
	s.Assert().Equal(1.0, testutil.ToFloat64(m.WalDataLost.WithLabelValues("gen-fenced", "events")))
	// Cross-contamination check: apply-failure files should not include gen-fenced.
	s.Assert().Equal(1.0, testutil.ToFloat64(m.WalDataLost.WithLabelValues("apply-failure", "files")))
	s.Assert().Equal(1.0, testutil.ToFloat64(m.WalDataLost.WithLabelValues("gen-fenced", "files")))
}

// ── Integration — flush ordered-halt ─────────────────────────────────────────

// LossIntegrationSuite tests that the default Coordinator onLoss wiring
// routes through logDataLost during a real flush ordered-halt or Replay gen-fence.
type LossIntegrationSuite struct {
	suite.Suite
	mgr     *delegation.Manager
	log     *BboltLog
	overlay *Overlay
	stream  *fakeApplyStream
}

func (s *LossIntegrationSuite) SetupTest() {
	s.mgr = delegation.NewManager(noopInvalidator{})
	s.log = openTestLog(s.T())
	s.overlay = NewOverlay()
	s.stream = &fakeApplyStream{ack: &proto.ApplyAck{}}
}

// makeCoordWithDefaultLoss returns a Coordinator that has the default onLoss
// wired (no WithOnLoss override), so logDataLost is the effective hook.
func (s *LossIntegrationSuite) makeCoord(identity, volume string) *Coordinator {
	caller := &proto.Caller{Owner: &proto.Owner{UserName: identity}}
	return NewCoordinator(s.mgr, s.log, s.overlay,
		WithApplyFactory(func(ctx context.Context) (proto.RpcFs_ApplyClient, error) {
			return s.stream, nil
		}),
		WithVolume(volume),
		WithCaller(caller),
	)
}

// TestFlushOrderedHalt_LogsEveryLostPathAndIncrementMetric is the integration
// gate for the ordered-halt path (Flush → processAck → onLoss → logDataLost).
func (s *LossIntegrationSuite) TestFlushOrderedHalt_LogsEveryLostPathAndIncrementMetric() {
	observed := installObserver(s.T())
	m := installTestMetrics(s.T())

	coord := s.makeCoord("carol", "videos")

	// Append 3 ops.
	seq1, _ := s.log.Append(Op{Kind: OpMkdir, Path: "movies/2026"})
	seq2, _ := s.log.Append(Op{Kind: OpCreate, Path: "movies/2026/film.mkv"})
	seq3, _ := s.log.Append(Op{Kind: OpWrite, Path: "movies/2026/film.mkv"})
	s.overlay.Apply(Op{Seq: seq1, Kind: OpMkdir, Path: "movies/2026"})
	s.overlay.Apply(Op{Seq: seq2, Kind: OpCreate, Path: "movies/2026/film.mkv"})
	s.overlay.Apply(Op{Seq: seq3, Kind: OpWrite, Path: "movies/2026/film.mkv"})

	// Server acks seq1, ordered-halt at seq2 (seq2 and seq3 are lost).
	s.stream.ack = &proto.ApplyAck{
		Watermark: seq1,
		FailedSeq: seq2,
		Fserr:     proto.FsError_FS_EPERM,
	}

	err := coord.Flush(context.Background(), seq3)
	s.Require().Error(err, "Flush must return error on ordered halt")

	// Exactly one ERROR log entry.
	s.Require().Equal(1, observed.Len(), "default onLoss must emit exactly one ERROR log")
	entry := observed.All()[0]
	s.Assert().Equal(zapcore.ErrorLevel, entry.Level)

	fields := entry.ContextMap()
	s.Assert().Equal("apply-failure", fields["reason"])
	s.Assert().Equal("carol", fields["identity"])
	s.Assert().Equal("videos", fields["volume"])

	// lost_paths must enumerate both lost paths (zap.Strings → []interface{} via MapObjectEncoder).
	rawPaths, ok := fields["lost_paths"]
	s.Require().True(ok, "lost_paths must appear in the log")
	paths := ifaceSliceToStrings(rawPaths)
	s.Assert().Contains(paths, "movies/2026/film.mkv", "lost path must appear in log")

	// Metric: events +1, files +1 (1 distinct path: movies/2026/film.mkv).
	s.Assert().Equal(1.0, testutil.ToFloat64(m.WalDataLost.WithLabelValues("apply-failure", "events")))
	s.Assert().Equal(1.0, testutil.ToFloat64(m.WalDataLost.WithLabelValues("apply-failure", "files")))
}

// TestReplayGenFence_LogsEveryLostPathAndIncrementMetric is the integration gate
// for the Replay gen-fence path (Replay → processAck → onLoss → logDataLost
// with reason="gen-fenced").
func (s *LossIntegrationSuite) TestReplayGenFence_LogsEveryLostPathAndIncrementMetric() {
	observed := installObserver(s.T())
	m := installTestMetrics(s.T())

	coord := s.makeCoord("dave", "backups")

	// Pre-seed the log with 2 ops.
	_, _ = s.log.Append(Op{Kind: OpCreate, Path: "snap/alpha.tar"})
	seq2, _ := s.log.Append(Op{Kind: OpWrite, Path: "snap/beta.tar"})

	// Server acks nothing (watermark=0), ordered-halt at seq1 — both ops are lost.
	// Use FailedSeq=1 (the first seq assigned) to lose both.
	s.stream.ack = &proto.ApplyAck{
		Watermark: 0,
		FailedSeq: 1,
		Fserr:     proto.FsError_FS_ENOSPC,
	}
	_ = seq2

	err := coord.Replay(context.Background(), 0)
	s.Require().Error(err, "Replay must return error on gen-fence ordered halt")

	s.Require().Equal(1, observed.Len(), "default onLoss must emit exactly one ERROR log")
	entry := observed.All()[0]
	fields := entry.ContextMap()
	s.Assert().Equal("gen-fenced", fields["reason"], "Replay loss must use reason='gen-fenced'")
	s.Assert().Equal("dave", fields["identity"])
	s.Assert().Equal("backups", fields["volume"])

	rawPaths, ok := fields["lost_paths"]
	s.Require().True(ok)
	paths := ifaceSliceToStrings(rawPaths)
	s.Assert().Contains(paths, "snap/alpha.tar")
	s.Assert().Contains(paths, "snap/beta.tar")

	// Metric: events +1, files +2 (2 distinct paths).
	s.Assert().Equal(1.0, testutil.ToFloat64(m.WalDataLost.WithLabelValues("gen-fenced", "events")))
	s.Assert().Equal(2.0, testutil.ToFloat64(m.WalDataLost.WithLabelValues("gen-fenced", "files")))
}

// ── Suite runners ─────────────────────────────────────────────────────────────

func TestLossLoggingSuite(t *testing.T) {
	suite.Run(t, new(LossLoggingSuite))
}

func TestLossIntegrationSuite(t *testing.T) {
	suite.Run(t, new(LossIntegrationSuite))
}
