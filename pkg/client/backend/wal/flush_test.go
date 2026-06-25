package wal

// flush_test.go — TDD tests for WAL flush triggers and Apply streaming (Task 11).
//
// Test coverage:
//   1. applyOps streams WalOps in ascending seq order and returns the ApplyAck.
//   2. Flush(throughSeq) sends ops [watermark+1..throughSeq], truncates the acked
//      prefix [..ack.Watermark], clears overlay, and advances local watermark.
//   3. Ordered halt: ack.FailedSeq > 0 → truncates committed prefix [..ack.Watermark],
//      calls onLoss with the remaining ops and ack.Fserr; watermark advances to ack.Watermark.
//   4. Fsync flushes synchronously (blocks until done) and returns the ApplyAck error.
//   5. Size-cap triggers a background flush and the capped path unblocks after drain.
//   6. Replay: streams all ops with seq > resume watermark via Apply.
//   7. Op→WalOp conversion: all 10 op kinds round-trip through opToWalOp.

import (
	"context"
	"errors"
	"io"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/suite"
	"go.gmountie.dev/gmountie/pkg/client/backend"
	"go.gmountie.dev/gmountie/pkg/client/backend/delegation"
	"go.gmountie.dev/gmountie/pkg/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

// ── fake Apply stream ─────────────────────────────────────────────────────────

// fakeApplyStream implements grpc.ClientStreamingClient[proto.WalOp, proto.ApplyAck].
// It records sent WalOps and returns a scripted ApplyAck.
type fakeApplyStream struct {
	mu      sync.Mutex
	sent    []*proto.WalOp
	ack     *proto.ApplyAck
	sendErr error // if non-nil, Send returns this error
	closed  bool
	gate    chan struct{} // if non-nil, CloseAndRecv blocks on it (in-flight Apply)
}

func (f *fakeApplyStream) Send(op *proto.WalOp) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.sendErr != nil {
		return f.sendErr
	}
	f.sent = append(f.sent, op)
	return nil
}

func (f *fakeApplyStream) CloseAndRecv() (*proto.ApplyAck, error) {
	f.mu.Lock()
	gate := f.gate
	f.mu.Unlock()
	if gate != nil {
		<-gate // simulate a slow in-flight Apply RPC until the test releases it
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.closed = true
	if f.ack == nil {
		return &proto.ApplyAck{}, nil
	}
	return f.ack, nil
}

func (f *fakeApplyStream) Header() (metadata.MD, error) { return nil, nil } //nolint:nilnil // fake gRPC stream: no metadata and no error is the correct test-double contract
func (f *fakeApplyStream) Trailer() metadata.MD         { return nil }
func (f *fakeApplyStream) CloseSend() error             { return nil }
func (f *fakeApplyStream) Context() context.Context     { return context.Background() }
func (f *fakeApplyStream) SendMsg(m any) error          { return nil }
func (f *fakeApplyStream) RecvMsg(m any) error          { return io.EOF }

// ── suite helpers ─────────────────────────────────────────────────────────────

type FlushSuite struct {
	suite.Suite
	mgr     *delegation.Manager
	log     *BboltLog
	overlay *Overlay
	coord   *Coordinator
	stream  *fakeApplyStream
}

func (s *FlushSuite) SetupTest() {
	s.mgr = delegation.NewManager(noopInvalidator{})
	s.log = openTestLog(s.T())
	s.overlay = NewOverlay()
	s.stream = &fakeApplyStream{ack: &proto.ApplyAck{}}
	s.coord = NewCoordinator(s.mgr, s.log, s.overlay,
		WithApplyFactory(func(ctx context.Context) (proto.RpcFs_ApplyClient, error) {
			return s.stream, nil
		}),
		WithVolume("vol1"),
	)
}

// appendOp is a helper that records an op to both log and overlay.
func (s *FlushSuite) appendOp(kind OpKind, path string) uint64 {
	op := Op{Kind: kind, Path: path}
	seq, err := s.log.Append(op)
	s.Require().NoError(err)
	op.Seq = seq
	s.overlay.Apply(op)
	return seq
}

// ── Test 1: applyOps streams ops in seq order ─────────────────────────────────

func (s *FlushSuite) TestApplyOps_StreamsInSeqOrder() {
	s.stream.ack = &proto.ApplyAck{Watermark: 3}

	ops := []Op{
		{Seq: 1, Kind: OpMkdir, Path: "a"},
		{Seq: 2, Kind: OpCreate, Path: "a/b.txt"},
		{Seq: 3, Kind: OpWrite, Path: "a/b.txt", Offset: 0, Data: []byte("hello")},
	}

	ack, err := s.coord.applyOps(context.Background(), ops)
	s.Require().NoError(err)
	s.Require().NotNil(ack)
	s.Equal(uint64(3), ack.Watermark)

	s.stream.mu.Lock()
	sent := s.stream.sent
	s.stream.mu.Unlock()

	s.Require().Len(sent, 3)
	s.Equal(uint64(1), sent[0].Seq)
	s.Equal(uint64(2), sent[1].Seq)
	s.Equal(uint64(3), sent[2].Seq)

	// Volume must be set in the first op's inner request.
	create, ok := sent[1].Op.(*proto.WalOp_Create)
	s.Require().True(ok)
	s.Equal("vol1", create.Create.Volume)
}

// ── Test 2: Flush truncates the acked prefix and clears overlay ───────────────

func (s *FlushSuite) TestFlush_TruncatesAckedPrefixAndClearsOverlay() {
	seq1 := s.appendOp(OpMkdir, "dir1")
	seq2 := s.appendOp(OpCreate, "dir1/file.txt")
	seq3 := s.appendOp(OpWrite, "dir1/file.txt")

	s.stream.ack = &proto.ApplyAck{Watermark: seq3}

	err := s.coord.Flush(context.Background(), seq3)
	s.Require().NoError(err)

	// All ops truncated.
	remaining, rerr := s.log.Replay(0)
	s.Require().NoError(rerr)
	s.Empty(remaining, "log must be empty after full-ack flush")

	// Overlay cleared.
	s.False(s.coord.Has("dir1"), "overlay must be cleared after flush")
	s.False(s.coord.Has("dir1/file.txt"), "overlay must be cleared after flush")

	// Watermark advanced.
	_ = seq1
	_ = seq2
	s.Equal(seq3, s.coord.watermark.Load(), "local watermark must advance to ack.Watermark")
}

// TestFlush_PreservesInFlightWritesAboveThroughSeq proves the flush clears ONLY
// the flushed prefix (seq ≤ throughSeq) and PRESERVES ops recorded during the
// in-flight Apply (seq > throughSeq). Before the fix processAck cleared the
// ENTIRE overlay (DropSubtree("")), wiping these in-flight writes before they
// were on the server → read-your-own-writes ENOENT (the npm-install failure).
func (s *FlushSuite) TestFlush_PreservesInFlightWritesAboveThroughSeq() {
	// Ops 1..3 are the batch this flush sends (throughSeq=through). Ops 4..5
	// stand in for writes recorded DURING the in-flight Apply (seq > through).
	s.appendOp(OpMkdir, "pkg")
	s.appendOp(OpCreate, "pkg/flushed.txt")
	through := s.appendOp(OpWrite, "pkg/flushed.txt")
	s.appendOp(OpCreate, "pkg/inflight1.txt")
	s.appendOp(OpCreate, "pkg/inflight2.txt")

	s.stream.ack = &proto.ApplyAck{Watermark: through}
	s.Require().NoError(s.coord.Flush(context.Background(), through))

	// Flushed prefix dropped from the overlay.
	s.False(s.coord.Has("pkg/flushed.txt"), "flushed op must be cleared from overlay")
	// In-flight writes (seq > throughSeq) MUST survive — not yet on the server,
	// so the overlay is the only place read-your-own-writes can serve them.
	s.True(s.coord.Has("pkg/inflight1.txt"), "in-flight write must NOT be wiped by the flush")
	s.True(s.coord.Has("pkg/inflight2.txt"), "in-flight write must NOT be wiped by the flush")

	// Log keeps only the survivors (seq > throughSeq), in order.
	remaining, rerr := s.log.Replay(0)
	s.Require().NoError(rerr)
	s.Require().Len(remaining, 2)
	s.Equal("pkg/inflight1.txt", remaining[0].Path)
	s.Equal("pkg/inflight2.txt", remaining[1].Path)

	s.Equal(through, s.coord.watermark.Load())
}

// TestFlush_OnFlushedReceivesFlushedPaths: the flush invokes onFlushed with the
// flushed ops so the Layer can invalidate the inner cache for each path. Without
// it, a delegated-subtree mutation (which bypasses the cache's own Create/Unlink
// invalidation) leaves a stale cache hit after the overlay clears — the readback
// ENOENT and the rm-sees-empty-dir → rmdir ENOTEMPTY cascade.
func (s *FlushSuite) TestFlush_OnFlushedReceivesFlushedPaths() {
	s.appendOp(OpMkdir, "pkg")
	s.appendOp(OpCreate, "pkg/a.txt")
	through := s.appendOp(OpCreate, "pkg/b.txt")

	var got []string
	s.coord.onFlushed = func(sent []Op) {
		for i := range sent {
			got = append(got, sent[i].Path)
		}
	}
	s.stream.ack = &proto.ApplyAck{Watermark: through}
	s.Require().NoError(s.coord.Flush(context.Background(), through))

	s.Require().Equal([]string{"pkg", "pkg/a.txt", "pkg/b.txt"}, got,
		"flush must hand the flushed paths to onFlushed for cache invalidation")
}

// TestFlush_ConcurrentInFlightWriteSurvives is the faithful repro: an op is
// recorded WHILE the flush's Apply RPC is in flight (blocked on the gate), then
// the flush completes. The concurrent write (seq > throughSeq) must survive the
// flush's overlay rebuild. Run under -race to exercise recordMu vs commitFlushed.
func (s *FlushSuite) TestFlush_ConcurrentInFlightWriteSurvives() {
	s.appendOp(OpMkdir, "pkg")
	through := s.appendOp(OpCreate, "pkg/flushed.txt")
	s.stream.ack = &proto.ApplyAck{Watermark: through}

	gate := make(chan struct{})
	s.stream.mu.Lock()
	s.stream.gate = gate
	s.stream.mu.Unlock()

	flushDone := make(chan error, 1)
	go func() { flushDone <- s.coord.Flush(context.Background(), through) }()

	// Record an op while the Apply is blocked in flight (seq > throughSeq).
	s.Require().NoError(s.coord.RecordOp(Op{Kind: OpCreate, Path: "pkg/inflight.txt"}))

	close(gate) // release the in-flight Apply → processAck → commitFlushed
	select {
	case err := <-flushDone:
		s.Require().NoError(err)
	case <-time.After(5 * time.Second):
		s.Fail("Flush did not complete — possible deadlock between recordMu and commitFlushed")
	}

	s.False(s.coord.Has("pkg/flushed.txt"), "flushed op must be cleared")
	s.True(s.coord.Has("pkg/inflight.txt"), "write recorded during the in-flight Apply must survive")
}

// ── Test 3: Ordered halt ──────────────────────────────────────────────────────

func (s *FlushSuite) TestFlush_OrderedHalt_OnLossCalledAndPrefixTruncated() {
	seq1 := s.appendOp(OpMkdir, "dir")
	seq2 := s.appendOp(OpCreate, "dir/f.txt")
	seq3 := s.appendOp(OpWrite, "dir/f.txt")

	// Server acks seq1, fails at seq2, seq3 is lost.
	s.stream.ack = &proto.ApplyAck{
		Watermark: seq1,
		FailedSeq: seq2,
		Fserr:     proto.FsError_FS_EPERM,
	}

	var lostOps []Op
	var lostErr proto.FsError
	s.coord.onLoss = func(_ string, ops []Op, fserr proto.FsError) {
		lostOps = ops
		lostErr = fserr
	}

	err := s.coord.Flush(context.Background(), seq3)
	// Flush returns error when there is a loss.
	s.Require().Error(err)

	// Committed prefix truncated.
	remaining, rerr := s.log.Replay(0)
	s.Require().NoError(rerr)
	s.Empty(remaining, "log must be fully truncated after ordered halt")

	// onLoss called with the ops at/after FailedSeq.
	s.Require().Len(lostOps, 2, "onLoss must receive ops at/after FailedSeq")
	s.Equal(seq2, lostOps[0].Seq)
	s.Equal(seq3, lostOps[1].Seq)
	s.Equal(proto.FsError_FS_EPERM, lostErr)

	// Watermark advanced to the committed prefix only.
	s.Equal(seq1, s.coord.watermark.Load())
}

// ── Test 4: Fsync flushes synchronously ──────────────────────────────────────

func (s *FlushSuite) TestFsync_FlushesBlockinglyAndReturnsError() {
	seq1 := s.appendOp(OpWrite, "data.bin")

	// Successful ack.
	s.stream.ack = &proto.ApplyAck{Watermark: seq1}

	ferr := s.coord.Fsync(context.Background())
	s.Equal(proto.FsError_FS_OK, ferr, "Fsync must return FS_OK on success")

	remaining, err := s.log.Replay(0)
	s.Require().NoError(err)
	s.Empty(remaining, "log must be empty after Fsync-triggered flush")
}

func (s *FlushSuite) TestFsync_ReturnsErrorOnApplyFailure() {
	s.appendOp(OpWrite, "data.bin")

	// Simulate transport error from CloseAndRecv.
	s.stream.sendErr = errors.New("transport error")

	ferr := s.coord.Fsync(context.Background())
	s.NotEqual(proto.FsError_FS_OK, ferr, "Fsync must propagate failure as FsError")
}

// ── Test 5: Size-cap triggers flush and blocks until drain ───────────────────

func (s *FlushSuite) TestSizeCap_BlocksNewOpsUntilFlush() {
	// Configure a very small cap (1 op).
	coord := NewCoordinator(s.mgr, openTestLog(s.T()), NewOverlay(),
		WithApplyFactory(func(ctx context.Context) (proto.RpcFs_ApplyClient, error) {
			return s.stream, nil
		}),
		WithVolume("vol-cap"),
		WithCapOps(1),
	)
	t := s.T()

	// Grant a delegation so ops go to the WAL.
	s.mgr.Apply(&proto.DelegationGrant{GrantedRoot: "dir"})

	// Append one op to fill the cap.
	op1 := Op{Kind: OpMkdir, Path: "dir/a"}
	_, err := coord.log.Append(op1)
	s.Require().NoError(err)
	coord.overlay.Apply(op1)

	// Now set the ack so flush will succeed.
	s.stream.ack = &proto.ApplyAck{Watermark: 1}

	// A second RecordOp must block briefly then succeed once the cap allows it.
	// We test by running RecordOp in a goroutine and asserting it completes in time.
	done := make(chan error, 1)
	go func() {
		done <- coord.RecordOp(Op{Kind: OpCreate, Path: "dir/b.txt"})
	}()

	select {
	case err := <-done:
		_ = t
		s.Require().NoError(err, "RecordOp should eventually succeed after a size-cap flush")
	case <-time.After(3 * time.Second):
		s.Fail("RecordOp blocked indefinitely — size-cap flush did not drain")
	}
}

// ── Test 6: Replay streams ops > resume watermark ────────────────────────────

func (s *FlushSuite) TestReplay_StreamsOpsAboveResumeWatermark() {
	// Pre-seed the log with 3 ops.
	seq1, _ := s.log.Append(Op{Kind: OpMkdir, Path: "a"})
	seq2, _ := s.log.Append(Op{Kind: OpCreate, Path: "a/b"})
	seq3, _ := s.log.Append(Op{Kind: OpWrite, Path: "a/b"})

	// Resume watermark = seq1 → only seq2 and seq3 should be replayed.
	s.stream.ack = &proto.ApplyAck{Watermark: seq3}
	err := s.coord.Replay(context.Background(), seq1)
	s.Require().NoError(err)

	s.stream.mu.Lock()
	sent := s.stream.sent
	s.stream.mu.Unlock()

	s.Require().Len(sent, 2, "Replay must send ops with seq > resume watermark")
	s.Equal(seq2, sent[0].Seq)
	s.Equal(seq3, sent[1].Seq)
}

// ── Test 7: Op→WalOp conversion for all 10 kinds ─────────────────────────────

func (s *FlushSuite) TestOpToWalOp_AllKinds() {
	volume := "v"
	caller := (*proto.Caller)(nil)

	cases := []struct {
		name  string
		op    Op
		check func(walOp *proto.WalOp)
	}{
		{
			name: "OpWrite",
			op:   Op{Seq: 1, Kind: OpWrite, Path: "a/b.txt", Offset: 8, Data: []byte("data")},
			check: func(w *proto.WalOp) {
				v, ok := w.Op.(*proto.WalOp_Write)
				s.Require().True(ok)
				s.Equal("a/b.txt", v.Write.Path)
				s.Equal(int64(8), v.Write.Offset)
				s.Equal([]byte("data"), v.Write.Data)
				s.Equal(volume, v.Write.Volume)
			},
		},
		{
			name: "OpCreate",
			op:   Op{Seq: 2, Kind: OpCreate, Path: "c.txt", Flags: 0o2, Mode: 0o100644},
			check: func(w *proto.WalOp) {
				v, ok := w.Op.(*proto.WalOp_Create)
				s.Require().True(ok)
				s.Equal("c.txt", v.Create.Path)
				s.Equal(uint32(0o100644), v.Create.Mode)
				s.Equal(volume, v.Create.Volume)
			},
		},
		{
			name: "OpMkdir",
			op:   Op{Seq: 3, Kind: OpMkdir, Path: "d", Mode: 0o40755},
			check: func(w *proto.WalOp) {
				v, ok := w.Op.(*proto.WalOp_Mkdir)
				s.Require().True(ok)
				s.Equal("d", v.Mkdir.Path)
				s.Equal(uint32(0o40755), v.Mkdir.Mode)
				s.Equal(volume, v.Mkdir.Volume)
			},
		},
		{
			name: "OpUnlink",
			op:   Op{Seq: 4, Kind: OpUnlink, Path: "e.txt"},
			check: func(w *proto.WalOp) {
				v, ok := w.Op.(*proto.WalOp_Unlink)
				s.Require().True(ok)
				s.Equal("e.txt", v.Unlink.Path)
				s.Equal(volume, v.Unlink.Volume)
			},
		},
		{
			name: "OpRmdir",
			op:   Op{Seq: 5, Kind: OpRmdir, Path: "f"},
			check: func(w *proto.WalOp) {
				v, ok := w.Op.(*proto.WalOp_Rmdir)
				s.Require().True(ok)
				s.Equal("f", v.Rmdir.Path)
				s.Equal(volume, v.Rmdir.Volume)
			},
		},
		{
			name: "OpRename",
			op:   Op{Seq: 6, Kind: OpRename, Path: "old/x", NewPath: "new/x"},
			check: func(w *proto.WalOp) {
				v, ok := w.Op.(*proto.WalOp_Rename)
				s.Require().True(ok)
				s.Equal("old/x", v.Rename.OldName)
				s.Equal("new/x", v.Rename.NewName)
				s.Equal(volume, v.Rename.Volume)
			},
		},
		{
			name: "OpSymlink",
			op:   Op{Seq: 7, Kind: OpSymlink, Path: "link", Data: []byte("/target")},
			check: func(w *proto.WalOp) {
				v, ok := w.Op.(*proto.WalOp_Symlink)
				s.Require().True(ok)
				s.Equal("link", v.Symlink.LinkPath)
				s.Equal("/target", v.Symlink.Target)
				s.Equal(volume, v.Symlink.Volume)
			},
		},
		{
			// MODE|UID only: timestamps are populated in the op but must NOT appear
			// in the WalOp because their FATTR bits are absent — this is the
			// regression guard for the silent timestamp clobber bug.
			name: "OpSetAttr_ModeUID",
			op: Op{
				Seq:   8,
				Kind:  OpSetAttr,
				Path:  "g.txt",
				Valid: backend.FATTR_MODE | backend.FATTR_UID,
				Mode:  0o644,
				UID:   1000,
				// AtimeSec/MtimeSec intentionally non-zero to prove suppression is
				// bit-driven, not "zero data → nil".
				AtimeSec:  123,
				AtimeNsec: 456,
				MtimeSec:  789,
				MtimeNsec: 101,
			},
			check: func(w *proto.WalOp) {
				v, ok := w.Op.(*proto.WalOp_SetAttr)
				s.Require().True(ok)
				s.Equal("g.txt", v.SetAttr.Path)
				s.Equal(uint32(backend.FATTR_MODE|backend.FATTR_UID), v.SetAttr.Valid)
				s.Equal(uint32(0o644), v.SetAttr.Mode)
				s.Equal(uint32(1000), v.SetAttr.Uid)
				s.Equal(volume, v.SetAttr.Volume)
				// Timestamps must be nil — their FATTR bits were not set.
				s.Nil(v.SetAttr.Atime, "Atime must be nil when FATTR_ATIME is not in Valid")
				s.Nil(v.SetAttr.Mtime, "Mtime must be nil when FATTR_MTIME is not in Valid")
			},
		},
		{
			// ATIME|MTIME only: timestamps must be present; mode/uid/gid/size absent.
			name: "OpSetAttr_AtimeMtime",
			op: Op{
				Seq:       9,
				Kind:      OpSetAttr,
				Path:      "h.txt",
				Valid:     backend.FATTR_ATIME | backend.FATTR_MTIME,
				AtimeSec:  1000,
				AtimeNsec: 2000,
				MtimeSec:  3000,
				MtimeNsec: 4000,
			},
			check: func(w *proto.WalOp) {
				v, ok := w.Op.(*proto.WalOp_SetAttr)
				s.Require().True(ok)
				s.Equal("h.txt", v.SetAttr.Path)
				s.Equal(uint32(backend.FATTR_ATIME|backend.FATTR_MTIME), v.SetAttr.Valid)
				s.Equal(volume, v.SetAttr.Volume)
				s.Require().NotNil(v.SetAttr.Atime)
				s.Equal(uint64(1000), v.SetAttr.Atime.Sec)
				s.Equal(uint32(2000), v.SetAttr.Atime.Nsec)
				s.Require().NotNil(v.SetAttr.Mtime)
				s.Equal(uint64(3000), v.SetAttr.Mtime.Sec)
				s.Equal(uint32(4000), v.SetAttr.Mtime.Nsec)
				// Scalar fields not in Valid must be zero.
				s.Zero(v.SetAttr.Mode)
				s.Zero(v.SetAttr.Uid)
				s.Zero(v.SetAttr.Gid)
			},
		},
		{
			name: "OpSetXAttr",
			op:   Op{Seq: 10, Kind: OpSetXAttr, Path: "h.txt", XattrName: "user.k", XattrValue: []byte("v"), XattrFlags: 1},
			check: func(w *proto.WalOp) {
				v, ok := w.Op.(*proto.WalOp_SetXattr)
				s.Require().True(ok)
				s.Equal("h.txt", v.SetXattr.Path)
				s.Equal("user.k", v.SetXattr.Attribute)
				s.Equal([]byte("v"), v.SetXattr.Data)
				// #171 retyped Flags to the OS-neutral XAttrCreateMode enum;
				// raw XATTR_CREATE (1) maps to CREATE via pkg/common/fsconv.
				s.Equal(proto.XAttrCreateMode_XATTR_CREATE_MODE_CREATE, v.SetXattr.Flags)
				s.Equal(volume, v.SetXattr.Volume)
			},
		},
		{
			name: "OpRemoveXAttr",
			op:   Op{Seq: 11, Kind: OpRemoveXAttr, Path: "i.txt", XattrName: "user.k"},
			check: func(w *proto.WalOp) {
				v, ok := w.Op.(*proto.WalOp_RemoveXattr)
				s.Require().True(ok)
				s.Equal("i.txt", v.RemoveXattr.Path)
				s.Equal("user.k", v.RemoveXattr.Attribute)
				s.Equal(volume, v.RemoveXattr.Volume)
			},
		},
	}

	for _, tc := range cases {
		s.Run(tc.name, func() {
			walOp := opToWalOp(tc.op, volume, caller, "")
			s.Require().NotNil(walOp)
			s.Equal(tc.op.Seq, walOp.Seq)
			s.Equal(tc.op.Gen, walOp.Gen)
			tc.check(walOp)
		})
	}
}

// ── Test 8: backpressure spawns at most one drain goroutine ──────────────────

// TestSizeCap_AtMostOneDrainGoroutine verifies that sustained over-cap pressure
// does NOT spawn multiple concurrent background flushes. It holds the Apply
// stream open (via a gate channel) while repeatedly broadcasting capCond
// (simulating the spurious wakeups the pre-fix loop fed on), then asserts that
// exactly one factory invocation occurred.
func (s *FlushSuite) TestSizeCap_AtMostOneDrainGoroutine() {
	var flushCount atomic.Int32

	// gate blocks CloseAndRecv until the test releases it.
	gate := make(chan struct{})
	blocked := &blockingApplyStream{gate: gate}

	coord := NewCoordinator(s.mgr, openTestLog(s.T()), NewOverlay(),
		WithApplyFactory(func(ctx context.Context) (proto.RpcFs_ApplyClient, error) {
			flushCount.Add(1)
			return blocked, nil
		}),
		WithVolume("vol-once"),
		WithCapOps(1),
	)

	// Grant delegation so ops route to the WAL.
	s.mgr.Apply(&proto.DelegationGrant{GrantedRoot: "dir"})

	// Fill the cap with one op.
	op1 := Op{Kind: OpMkdir, Path: "dir/a"}
	_, err := coord.log.Append(op1)
	s.Require().NoError(err)
	coord.overlay.Apply(op1)

	// Start a RecordOp that will block until the cap clears.
	done := make(chan error, 1)
	go func() {
		done <- coord.RecordOp(Op{Kind: OpCreate, Path: "dir/b.txt"})
	}()

	// Give waitForCap time to spawn the drain goroutine and block on capCond.Wait().
	time.Sleep(50 * time.Millisecond)

	// Fire many spurious Broadcasts while the flush is still in flight.
	// Before the fix, each would spawn a new goroutine; after the fix, the
	// flushing guard suppresses them.
	for i := 0; i < 5; i++ {
		coord.capMu.Lock()
		coord.capCond.Broadcast()
		coord.capMu.Unlock()
		time.Sleep(5 * time.Millisecond)
	}

	// Exactly one flush should have been started.
	s.Equal(int32(1), flushCount.Load(), "at most one drain goroutine must be in flight")

	// Release the in-flight flush so the test can finish cleanly.
	close(gate)

	select {
	case err := <-done:
		s.Require().NoError(err, "RecordOp must eventually succeed after cap drains")
	case <-time.After(3 * time.Second):
		s.Fail("RecordOp blocked indefinitely after gate released")
	}
}

// blockingApplyStream is a fake Apply stream that blocks CloseAndRecv until gate is closed.
type blockingApplyStream struct {
	fakeApplyStream
	gate <-chan struct{}
}

func (b *blockingApplyStream) CloseAndRecv() (*proto.ApplyAck, error) {
	<-b.gate
	return &proto.ApplyAck{Watermark: 1}, nil
}

func TestFlushSuite(t *testing.T) {
	suite.Run(t, new(FlushSuite))
}

// Compile-time assertion: fakeApplyStream must satisfy the streaming interface.
var _ proto.RpcFs_ApplyClient = (*fakeApplyStream)(nil)

// Silence unused import of grpc (used only through the interface).
var _ = grpc.CallOption(nil)
