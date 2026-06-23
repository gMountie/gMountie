package transport

// CoalesceCharacterizationSuite pins the four observable write-coalescing
// behaviours of grpcFileHandle + BackendClient. It is the behaviour-
// preservation gate for the WAL handle-seam refactor (Task 9b): any change
// that breaks one of these tests breaks the semantic contract.
//
// The four behaviours:
//
//  1. Small contiguous writes coalesce: N small contiguous Writes buffer
//     optimistically (each returns OK with no streaming Write RPC) and are
//     flushed as exactly ONE WriteAndFlush RPC on Flush — NOT N separate
//     streaming Write RPCs.
//
//  2. Big write (>= threshold) bypasses the buffer: pending coalesced bytes
//     are drained FIRST (one streaming Write RPC preserving on-disk order),
//     then the big write is sent directly (a second streaming Write RPC).
//
//  3. Clean-handle Flush skips the RPC: if nothing was written since the
//     last flush (dirty==false), Flush issues no RPC whatsoever.
//
//  4. Sticky write-back error: if a coalesced batch flush fails, the error is
//     recorded stickily and surfaced exactly once on the next Flush; Release
//     consumes and logs the sticky error (close(2) can't propagate it) and
//     still proceeds to the server-side Release RPC.

import (
	"context"
	"testing"

	grpcclient "go.gmountie.dev/gmountie/pkg/client/grpc"
	"go.gmountie.dev/gmountie/pkg/proto"
	"go.gmountie.dev/gmountie/pkg/utils/log"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"go.uber.org/zap/zaptest/observer"
)

type CoalesceCharacterizationSuite struct {
	BackendClientTestSuite
}

// TestCharacterization_SmallContiguousWritesCoalesceIntoOneWriteAndFlush
// pins behaviour 1: three small writes at contiguous offsets must not
// generate any streaming Write RPCs. The Flush that follows must issue
// exactly one WriteAndFlush RPC carrying the concatenated bytes at the
// starting offset.
func (s *CoalesceCharacterizationSuite) TestCharacterization_SmallContiguousWritesCoalesceIntoOneWriteAndFlush() {
	h := s.newHandle(grpcclient.PerFileConfig{WriteCoalesceBytes: 4096})

	// Three small contiguous writes: "aaa"@0, "bbb"@3, "ccc"@6.
	// None of them reaches the 4096-byte threshold, so no RPC must be
	// issued during these three calls. A strict mock with no Write or
	// WriteAndFlush expectation set until Flush enforces this.
	_, st1 := s.backend.Write(context.Background(), h, 0, []byte("aaa"))
	s.Require().Equal(proto.FsError_FS_OK, st1, "first small write must buffer and return OK")

	_, st2 := s.backend.Write(context.Background(), h, 3, []byte("bbb"))
	s.Require().Equal(proto.FsError_FS_OK, st2, "second small write must buffer and return OK")

	_, st3 := s.backend.Write(context.Background(), h, 6, []byte("ccc"))
	s.Require().Equal(proto.FsError_FS_OK, st3, "third small write must buffer and return OK")

	// Flush must drain the buffer as a single WriteAndFlush RPC carrying
	// the full concatenation "aaabbbccc" at offset 0.
	s.fileClient.EXPECT().WriteAndFlush(
		mock.Anything,
		mock.MatchedBy(func(r *proto.WriteAndFlushRequest) bool {
			return r.Volume == "testVolume" &&
				r.Fd == 1 &&
				r.SessionId == "test-session" &&
				r.Offset == 0 &&
				string(r.Data) == "aaabbbccc"
		}),
		mock.Anything,
	).Return(&proto.WriteAndFlushReply{
		Status:  proto.FsError_FS_OK,
		Written: 9,
	}, nil).Once()

	st := s.backend.Flush(context.Background(), h)
	s.Assert().Equal(proto.FsError_FS_OK, st)
	s.fileClient.AssertNumberOfCalls(s.T(), "WriteAndFlush", 1)
	// No streaming Write RPC (fileClient.Write) must have fired.
	s.fileClient.AssertNumberOfCalls(s.T(), "Write", 0)
}

// TestCharacterization_BigWriteDrainsPendingSmallWriteFirst pins behaviour 2:
// when a write whose payload is >= the coalesce threshold arrives, any
// buffered bytes are sent first (streaming Write), then the big payload is
// sent in a second streaming Write. Two RPCs in order, no coalescing of the
// big payload.
func (s *CoalesceCharacterizationSuite) TestCharacterization_BigWriteDrainsPendingSmallWriteFirst() {
	const threshold = 8
	h := s.newHandle(grpcclient.PerFileConfig{WriteCoalesceBytes: threshold})

	// Small write: "ab"@0 — buffers, no RPC.
	_, st1 := s.backend.Write(context.Background(), h, 0, []byte("ab"))
	s.Require().Equal(proto.FsError_FS_OK, st1, "small write must buffer and return OK without an RPC")

	// Big write: 8 bytes (== threshold) at offset 2.
	// Expected sequence:
	//   1. streaming Write for the pending "ab"@0 (drain)
	//   2. streaming Write for the big payload@2
	bigPayload := []byte("bigbigbi") // exactly 8 bytes

	drainStub := newBackendWriteStreamStub(s.T(),
		&proto.WriteReply{Written: 2, Status: proto.FsError_FS_OK}, nil)
	drainStub.EXPECT().Send(mock.MatchedBy(func(f *proto.WriteFrame) bool {
		return string(f.Data) == "ab" && f.Offset == 0
	})).RunAndReturn(drainStub.send).Maybe()

	bigStub := newBackendWriteStreamStub(s.T(),
		&proto.WriteReply{Written: 8, Status: proto.FsError_FS_OK}, nil)
	bigStub.EXPECT().Send(mock.MatchedBy(func(f *proto.WriteFrame) bool {
		return string(f.Data) == string(bigPayload) && f.Offset == 2
	})).RunAndReturn(bigStub.send).Maybe()

	// First Write() call opens the drain stream; second opens the big stream.
	s.fileClient.EXPECT().Write(mock.Anything, mock.Anything).Return(drainStub, nil).Once()
	s.fileClient.EXPECT().Write(mock.Anything, mock.Anything).Return(bigStub, nil).Once()

	_, st2 := s.backend.Write(context.Background(), h, 2, bigPayload)
	s.Require().Equal(proto.FsError_FS_OK, st2, "big write must succeed")

	// Exactly 2 streaming Write RPCs, in drain-then-big order.
	s.fileClient.AssertNumberOfCalls(s.T(), "Write", 2)
	s.Require().Len(drainStub.frames, 1, "drain stub must have received exactly one frame")
	s.Assert().Equal(int64(0), drainStub.frames[0].Offset)
	s.Assert().Equal("ab", string(drainStub.frames[0].Data))
	s.Require().Len(bigStub.frames, 1, "big stub must have received exactly one frame")
	s.Assert().Equal(int64(2), bigStub.frames[0].Offset)
	s.Assert().Equal(string(bigPayload), string(bigStub.frames[0].Data))
}

// TestCharacterization_CleanHandleFlushSkipsRPC pins behaviour 3: a handle
// on which nothing has been written issues no RPC on Flush. A strict mock
// with no Write / WriteAndFlush / Flush expectation enforces this.
func (s *CoalesceCharacterizationSuite) TestCharacterization_CleanHandleFlushSkipsRPC() {
	h := s.newHandle(grpcclient.PerFileConfig{WriteCoalesceBytes: 4096})

	st := s.backend.Flush(context.Background(), h)
	s.Assert().Equal(proto.FsError_FS_OK, st)
	s.fileClient.AssertNumberOfCalls(s.T(), "Write", 0)
	s.fileClient.AssertNumberOfCalls(s.T(), "WriteAndFlush", 0)
}

// TestCharacterization_StickyWriteErrSurfacedOnFlushThenClearedOnRelease
// pins behaviour 4: a failed coalesced batch flush inside Write records
// the error stickily. The next Flush returns that error (exactly once) and
// issues no RPC. Release then consumes (and logs) the sticky error but
// still proceeds to the server-side Release RPC and returns OK — the sticky
// state must be cleared after Release.
func (s *CoalesceCharacterizationSuite) TestCharacterization_StickyWriteErrSurfacedOnFlushThenClearedOnRelease() {
	h := s.newHandle(grpcclient.PerFileConfig{WriteCoalesceBytes: 4})

	// First small write: "ab"@0 — buffers below the 4-byte threshold, no RPC.
	_, st1 := s.backend.Write(context.Background(), h, 0, []byte("ab"))
	s.Require().Equal(proto.FsError_FS_OK, st1)

	// Second contiguous write "cd"@2 fills the buffer to 4 bytes (== threshold)
	// so the coalescer hands back the batch. The streaming Write RPC fails
	// in-band with ENOSPC. Those bytes are now lost; Write must return ENOSPC.
	failStub := newBackendWriteStreamStub(s.T(),
		&proto.WriteReply{Written: 0, Status: proto.FsError_FS_ENOSPC}, nil)
	s.fileClient.EXPECT().Write(mock.Anything, mock.Anything).Return(failStub, nil).Once()

	_, st2 := s.backend.Write(context.Background(), h, 2, []byte("cd"))
	s.Require().Equal(proto.FsError_FS_ENOSPC, st2,
		"failed batch flush must surface to the caller immediately")

	// Flush must return the sticky error — no WriteAndFlush RPC expected
	// (strict mock: no expectation set for WriteAndFlush here).
	flushSt := s.backend.Flush(context.Background(), h)
	s.Assert().Equal(proto.FsError_FS_ENOSPC, flushSt,
		"Flush must surface the sticky write-back error from the earlier lost batch")
	s.fileClient.AssertNumberOfCalls(s.T(), "WriteAndFlush", 0)

	// Release must:
	//   - consume the (now-cleared) sticky error internally (logging it)
	//   - still proceed to the server-side Release RPC
	//   - return OK (close(2) can never propagate the error anyway)
	s.fileClient.EXPECT().Release(mock.Anything,
		mock.MatchedBy(func(r *proto.ReleaseRequest) bool {
			return r.Volume == "testVolume" && r.Fd == 1
		}),
		mock.Anything,
	).Return(&proto.ReleaseReply{}, nil).Once()

	relSt := s.backend.Release(context.Background(), h)
	s.Assert().Equal(proto.FsError_FS_OK, relSt,
		"Release must return OK even when a sticky write-back error was pending")

	// The sticky error must have been cleared by Release: a subsequent
	// takeWriteErr should return FS_OK.
	s.Assert().Equal(proto.FsError_FS_OK, h.takeWriteErr(),
		"sticky write-back error must be cleared after Release")
}

// TestCharacterization_ReleaseConsumesAndLogsStickyWriteErr pins the second
// half of behaviour 4 WITHOUT a Flush in between: Release ITSELF must consume
// and log the sticky error when no prior Flush has cleared it.
//
// The critical hole in the sibling test above is that Flush calls takeWriteErr
// (clearing the sticky error) BEFORE Release runs, so by the time Release's
// sticky-error block executes the error is already gone. That test's final
// takeWriteErr()==OK assertion passes trivially regardless of what Release does.
// A 9b refactor that silently drops Release's takeWriteErr block would still pass
// the sibling — but it FAILS this test.
//
// Gate: we install a zaptest/observer core over log.Log so we can assert the
// exact Error log entry that Release emits when it consumes the sticky error
// (backend_grpc.go Release body). Deleting Release's takeWriteErr block removes
// that log.Log.Error call, which causes Len==0 and fails the Require below.
func (s *CoalesceCharacterizationSuite) TestCharacterization_ReleaseConsumesAndLogsStickyWriteErr() {
	// Install a zaptest/observer core over the package logger so we can assert
	// the error log that Release emits on a consumed sticky write-back error.
	// Restore in Cleanup so the swap never leaks to sibling tests.
	observerCore, observed := observer.New(zapcore.ErrorLevel)
	origLog := log.Log
	log.Log = zap.New(observerCore)
	s.T().Cleanup(func() { log.Log = origLog })

	h := s.newHandle(grpcclient.PerFileConfig{WriteCoalesceBytes: 4})

	// First small write "ab"@0 — buffers below threshold, no RPC.
	_, st1 := s.backend.Write(context.Background(), h, 0, []byte("ab"))
	s.Require().Equal(proto.FsError_FS_OK, st1)

	// Second contiguous write "cd"@2 fills the 4-byte buffer (== threshold)
	// causing the coalescer to hand back the batch. The streaming Write RPC
	// fails with ENOSPC → recordWriteErr sets the sticky error. Write returns
	// ENOSPC immediately; the coalescer is now empty (batch was cleared by Append).
	failStub := newBackendWriteStreamStub(s.T(),
		&proto.WriteReply{Written: 0, Status: proto.FsError_FS_ENOSPC}, nil)
	s.fileClient.EXPECT().Write(mock.Anything, mock.Anything).Return(failStub, nil).Once()

	_, st2 := s.backend.Write(context.Background(), h, 2, []byte("cd"))
	s.Require().Equal(proto.FsError_FS_ENOSPC, st2,
		"failed batch flush must surface to the caller immediately")

	// Call Release DIRECTLY — no Flush — so the sticky error is still live
	// when Release's takeWriteErr block runs. Nothing between the Write and
	// Release touches the sticky state: the structure of this test ensures
	// only Release can consume it.
	s.fileClient.EXPECT().Release(mock.Anything,
		mock.MatchedBy(func(r *proto.ReleaseRequest) bool {
			return r.Volume == "testVolume" && r.Fd == 1
		}),
		mock.Anything,
	).Return(&proto.ReleaseReply{}, nil).Once()

	relSt := s.backend.Release(context.Background(), h)
	s.Assert().Equal(proto.FsError_FS_OK, relSt,
		"Release must return OK even when a sticky write-back error was pending")

	// Assert the observable produced by Release's sticky-error consumption:
	// exactly one Error log entry with the expected message and path field.
	// If Release's takeWriteErr block is removed, this entry is never emitted
	// and the Require fails → the gate is real.
	entries := observed.FilterMessageSnippet("sticky write-back error on Release").All()
	s.Require().Len(entries, 1,
		"Release must emit exactly one Error log entry for the consumed sticky write-back error")
	s.Assert().Equal("/test/path", entries[0].ContextMap()["path"],
		"the log entry must carry the handle's file path")

	// The sticky state must be cleared by Release: takeWriteErr now returns OK.
	s.Assert().Equal(proto.FsError_FS_OK, h.takeWriteErr(),
		"sticky write-back error must be cleared after Release consumes it")
}

func TestCoalesceCharacterizationSuite(t *testing.T) {
	suite.Run(t, new(CoalesceCharacterizationSuite))
}
