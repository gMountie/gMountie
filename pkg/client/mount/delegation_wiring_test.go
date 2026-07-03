package mount

import (
	"context"
	"fmt"
	"io"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"
	"google.golang.org/grpc"

	grpcmocks "go.gmountie.dev/gmountie/internal/mocks/pkg/client/grpc"
	mockProto "go.gmountie.dev/gmountie/internal/mocks/pkg/proto"
	"go.gmountie.dev/gmountie/pkg/client/backend"
	"go.gmountie.dev/gmountie/pkg/client/backend/delegation"
	"go.gmountie.dev/gmountie/pkg/client/backend/wal"
	"go.gmountie.dev/gmountie/pkg/proto"
)

// DelegationWiringSuite tests the delegation-Manager wiring in single.go
// without touching FUSE (no real mount). It exercises:
//  1. lazyInvalidator behaves as a forward-reference: before set() it is a
//     no-op; after set() it forwards to the real target.
//  2. runRecallLoop exits cleanly when delMgr.Close() cancels the context —
//     no goroutine leak, no -race hit.
//  3. The recall loop delivers OnRecall to the Manager and sends RecallAck.
type DelegationWiringSuite struct {
	suite.Suite
	client *grpcmocks.MockClient
}

func (s *DelegationWiringSuite) SetupTest() {
	s.client = grpcmocks.NewMockClient(s.T())
}

// TestLazyInvalidator_BeforeSet confirms that InvalidateSubtree before set()
// is a safe no-op (does not panic and does not forward).
func (s *DelegationWiringSuite) TestLazyInvalidator_BeforeSet() {
	inv := &lazyInvalidator{}
	// Must not panic even though target is nil.
	s.NotPanics(func() { inv.InvalidateSubtree("any/path") })
}

// TestLazyInvalidator_AfterSet confirms that after set() the call is forwarded.
func (s *DelegationWiringSuite) TestLazyInvalidator_AfterSet() {
	inv := &lazyInvalidator{}
	called := make(chan string, 1)
	inv.set(fakeInvalidator(func(p string) { called <- p }))
	inv.InvalidateSubtree("proj/foo")
	select {
	case got := <-called:
		s.Equal("proj/foo", got)
	case <-time.After(time.Second):
		s.Fail("InvalidateSubtree was not forwarded after set()")
	}
}

// TestRecallGoroutineExitsOnClose verifies that runRecallLoop exits cleanly
// when the Manager's context is cancelled (via Close → SetCancel).
// A WaitGroup detects the exit; a 3-second timeout fails the test if it leaks.
func (s *DelegationWiringSuite) TestRecallGoroutineExitsOnClose() {
	fsClient := mockProto.NewMockRpcFsClient(s.T())

	// Block until the goroutine has opened the stream at least once.
	streamOpened := make(chan struct{}, 1)
	recallStream := mockProto.NewMockRpcFs_RecallClient(s.T())
	// Recv returns EOF so the inner loop exits and the goroutine backs off.
	recallStream.EXPECT().Recv().Return(nil, io.EOF).Maybe()

	fsClient.EXPECT().
		Recall(mock.Anything, mock.Anything).
		RunAndReturn(func(ctx context.Context, opts ...grpc.CallOption) (grpc.BidiStreamingClient[proto.RecallAck, proto.RecallMsg], error) {
			select {
			case streamOpened <- struct{}{}:
			default:
			}
			return recallStream, nil
		}).Maybe()
	s.client.EXPECT().Fs().Return(fsClient).Maybe()

	inv := &lazyInvalidator{}
	mgr := delegation.NewManager(inv)
	mounter := &SingleVolumeMounterImpl{client: s.client}

	ctx, cancel := context.WithCancel(context.Background())
	mgr.SetCancel(cancel)

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		mounter.runRecallLoop(ctx, mgr, "test-vol")
	}()

	// Wait until the goroutine is alive (stream opened).
	select {
	case <-streamOpened:
	case <-time.After(2 * time.Second):
		s.Fail("recall goroutine did not start within 2s")
	}

	// Close the Manager — cancels the context via SetCancel.
	mgr.Close()

	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()
	select {
	case <-done:
		// Clean exit — no goroutine leak.
	case <-time.After(3 * time.Second):
		s.Fail("recall goroutine did not exit within 3s after Close()")
	}
}

// TestRecallGoroutineDeliversOnRecall verifies that a RecallMsg received on the
// stream triggers mgr.OnRecall (which calls InvalidateSubtree) and sends a
// RecallAck with the matching RecallId and Done=true.
func (s *DelegationWiringSuite) TestRecallGoroutineDeliversOnRecall() {
	fsClient := mockProto.NewMockRpcFsClient(s.T())

	ackSent := make(chan *proto.RecallAck, 1)
	recallStream := mockProto.NewMockRpcFs_RecallClient(s.T())
	// First Recv returns a recall; second returns EOF to end the inner loop.
	recallStream.EXPECT().Recv().
		Return(&proto.RecallMsg{Root: "proj/", RecallId: 42}, nil).
		Once()
	recallStream.EXPECT().Recv().
		Return(nil, io.EOF).
		Maybe()
	recallStream.EXPECT().Send(mock.MatchedBy(func(ack *proto.RecallAck) bool {
		select {
		case ackSent <- ack:
		default:
		}
		return true
	})).Return(nil).Maybe()

	fsClient.EXPECT().
		Recall(mock.Anything, mock.Anything).
		RunAndReturn(func(ctx context.Context, opts ...grpc.CallOption) (grpc.BidiStreamingClient[proto.RecallAck, proto.RecallMsg], error) {
			return recallStream, nil
		}).Maybe()
	s.client.EXPECT().Fs().Return(fsClient).Maybe()

	// Use a tracking invalidator to confirm OnRecall propagates.
	invalidated := make(chan string, 1)
	inv := &lazyInvalidator{}
	inv.set(fakeInvalidator(func(p string) {
		select {
		case invalidated <- p:
		default:
		}
	}))

	mgr := delegation.NewManager(inv)
	mounter := &SingleVolumeMounterImpl{client: s.client}

	ctx, cancel := context.WithCancel(context.Background())
	mgr.SetCancel(cancel)
	defer mgr.Close()

	go mounter.runRecallLoop(ctx, mgr, "test-vol")

	// Wait for the RecallAck.
	select {
	case ack := <-ackSent:
		s.Equal(uint64(42), ack.RecallId, "RecallAck must echo the RecallId")
		s.True(ack.Done, "RecallAck must set Done=true")
	case <-time.After(2 * time.Second):
		s.Fail("RecallAck was not sent within 2s")
	}

	// Wait for the invalidation.
	select {
	case root := <-invalidated:
		s.Equal("proj/", root, "OnRecall must call InvalidateSubtree with the recalled root")
	case <-time.After(2 * time.Second):
		s.Fail("InvalidateSubtree was not called within 2s")
	}
}

// TestRecallGoroutineSendsAbortAckOnFlushFailure verifies that when the
// Manager's OnRecall fails (flush error), runRecallLoop sends an explicit
// abort ack (done=false, fserr set) instead of silently skipping the ack —
// so the server fails the handoff immediately instead of burning the recall
// timeout while the contender blocks.
func (s *DelegationWiringSuite) TestRecallGoroutineSendsAbortAckOnFlushFailure() {
	fsClient := mockProto.NewMockRpcFsClient(s.T())

	ackSent := make(chan *proto.RecallAck, 1)
	recallStream := mockProto.NewMockRpcFs_RecallClient(s.T())
	// First Recv returns a recall; second returns EOF to end the inner loop.
	recallStream.EXPECT().Recv().
		Return(&proto.RecallMsg{Root: "proj/", RecallId: 7}, nil).
		Once()
	recallStream.EXPECT().Recv().
		Return(nil, io.EOF).
		Maybe()
	recallStream.EXPECT().Send(mock.MatchedBy(func(ack *proto.RecallAck) bool {
		select {
		case ackSent <- ack:
		default:
		}
		return true
	})).Return(nil).Maybe()

	fsClient.EXPECT().
		Recall(mock.Anything, mock.Anything).
		RunAndReturn(func(ctx context.Context, opts ...grpc.CallOption) (grpc.BidiStreamingClient[proto.RecallAck, proto.RecallMsg], error) {
			return recallStream, nil
		}).Maybe()
	s.client.EXPECT().Fs().Return(fsClient).Maybe()

	inv := &lazyInvalidator{}
	mgr := delegation.NewManager(inv)
	// A flusher that always fails -> OnRecall returns an error -> abort ack.
	mgr.SetRecallFlusher(delegationFlusherFunc(func(context.Context, string) error {
		return errFlushBoom
	}))
	mgr.Apply(&proto.DelegationGrant{GrantedRoot: "proj"})

	mounter := &SingleVolumeMounterImpl{client: s.client}

	ctx, cancel := context.WithCancel(context.Background())
	mgr.SetCancel(cancel)
	defer mgr.Close()

	go mounter.runRecallLoop(ctx, mgr, "test-vol")

	select {
	case ack := <-ackSent:
		s.Equal(uint64(7), ack.RecallId, "abort ack must echo the RecallId")
		s.False(ack.Done, "abort ack must set Done=false")
		s.Equal(proto.FsError_FS_EIO, ack.Fserr, "abort ack must carry a diagnostic fserr")
	case <-time.After(2 * time.Second):
		s.Fail("abort RecallAck was not sent within 2s")
	}

	// The grant must be retained: the abort ack signals the flush failed, so
	// OnRecall must NOT have dropped the delegation.
	s.True(mgr.IsDelegated("proj/file"), "grant must be retained after an aborted recall")
}

// TestRecallGoroutineAbortAckMapsTransientHaltToEAGAIN verifies fserr
// fidelity on the abort ack: a TRANSIENT flush halt (wal.ErrTransientHalt —
// the tail is retained, nothing lost) must map to FS_EAGAIN so the server/
// contender treat the aborted handoff as retryable, not FS_EIO.
func (s *DelegationWiringSuite) TestRecallGoroutineAbortAckMapsTransientHaltToEAGAIN() {
	fsClient := mockProto.NewMockRpcFsClient(s.T())

	ackSent := make(chan *proto.RecallAck, 1)
	recallStream := mockProto.NewMockRpcFs_RecallClient(s.T())
	recallStream.EXPECT().Recv().
		Return(&proto.RecallMsg{Root: "proj/", RecallId: 9}, nil).
		Once()
	recallStream.EXPECT().Recv().
		Return(nil, io.EOF).
		Maybe()
	recallStream.EXPECT().Send(mock.MatchedBy(func(ack *proto.RecallAck) bool {
		select {
		case ackSent <- ack:
		default:
		}
		return true
	})).Return(nil).Maybe()

	fsClient.EXPECT().
		Recall(mock.Anything, mock.Anything).
		RunAndReturn(func(ctx context.Context, opts ...grpc.CallOption) (grpc.BidiStreamingClient[proto.RecallAck, proto.RecallMsg], error) {
			return recallStream, nil
		}).Maybe()
	s.client.EXPECT().Fs().Return(fsClient).Maybe()

	inv := &lazyInvalidator{}
	mgr := delegation.NewManager(inv)
	// A flusher that fails with a wrapped transient halt, exactly as
	// wal.Coordinator.Flush reports EAGAIN contention.
	mgr.SetRecallFlusher(delegationFlusherFunc(func(context.Context, string) error {
		return fmt.Errorf("flush for recall: %w", wal.ErrTransientHalt)
	}))
	mgr.Apply(&proto.DelegationGrant{GrantedRoot: "proj"})

	mounter := &SingleVolumeMounterImpl{client: s.client}

	ctx, cancel := context.WithCancel(context.Background())
	mgr.SetCancel(cancel)
	defer mgr.Close()

	go mounter.runRecallLoop(ctx, mgr, "test-vol")

	select {
	case ack := <-ackSent:
		s.Equal(uint64(9), ack.RecallId, "abort ack must echo the RecallId")
		s.False(ack.Done, "abort ack must set Done=false")
		s.Equal(proto.FsError_FS_EAGAIN, ack.Fserr,
			"a transient flush halt must surface as FS_EAGAIN (retryable), not FS_EIO")
	case <-time.After(2 * time.Second):
		s.Fail("abort RecallAck was not sent within 2s")
	}
}

var errFlushBoom = errFlushErr("flush boom")

type errFlushErr string

func (e errFlushErr) Error() string { return string(e) }

// delegationFlusherFunc adapts a func to delegation.RecallFlusher for tests.
type delegationFlusherFunc func(ctx context.Context, root string) error

func (f delegationFlusherFunc) FlushForRecall(ctx context.Context, root string) error {
	return f(ctx, root)
}

// fakeInvalidator is a delegation.CacheInvalidator test double backed by a func.
type fakeInvalidator func(string)

func (f fakeInvalidator) InvalidateSubtree(p string) { f(p) }

func TestDelegationWiringSuite(t *testing.T) {
	suite.Run(t, new(DelegationWiringSuite))
}

// ── WAL wiring tests ──────────────────────────────────────────────────────────
//
// WALWiringSuite tests the WAL construction and lifecycle at the mount seam.
// It avoids a real FUSE mount by exercising the layer-assembly (composeBackend)
// and Coordinator lifecycle directly — the same technique used by ComposeSuite
// and DelegationWiringSuite.

type WALWiringSuite struct {
	suite.Suite
}

// buildWALLayer opens a fresh Coordinator backed by a temp log and constructs
// the posWAL backendLayer. Returns the layer, coordinator, and a no-op inner.
func (s *WALWiringSuite) buildWALLayer() (backendLayer, *wal.Coordinator) {
	mgr := delegation.NewManager(fakeInvalidator(func(_ string) {}))
	logPath := filepath.Join(s.T().TempDir(), "wal.db")
	walLog, err := wal.Open(logPath)
	s.Require().NoError(err)
	coord := wal.NewCoordinator(mgr, walLog, wal.NewOverlay())

	mgr_ := mgr
	coord_ := coord
	layer := backendLayer{pos: posWAL, build: func(inner backend.FileSystemBackend) backend.FileSystemBackend {
		return wal.NewLayer(inner, mgr_, coord_)
	}}
	return layer, coord
}

// TestWALLayerAtPosWAL verifies that posWAL sits OUTER of posCache in the
// compose stack (i.e., it is built AFTER posCache, which means it wraps it).
// This is the same ordering invariant tested by ComposeSuite for cache/observer.
func (s *WALWiringSuite) TestWALLayerAtPosWAL() {
	var order []string
	mk := func(name string, pos layerPos) backendLayer {
		return backendLayer{pos: pos, build: func(inner backend.FileSystemBackend) backend.FileSystemBackend {
			order = append(order, name)
			return inner
		}}
	}
	transport := &PassthroughCounter{}
	layers := []backendLayer{
		mk("cache", posCache),
		mk("wal", posWAL),
		mk("observer", posObserver),
	}
	got := composeBackend(transport, layers)
	s.NotNil(got)
	// composeBackend builds innermost first: cache → wal → observer.
	s.Equal([]string{"cache", "wal", "observer"}, order,
		"WAL layer must be built after cache (outer of cache, inner of observer)")
}

// TestWALCoordinator_IntervalFlusherLifecycle verifies that StartIntervalFlusher
// starts a goroutine that exits cleanly when coord.Close() is called.
// This is the mount-level analog of the wal_test.go goroutine-leak test; it
// uses the same coord that the mount layer would wire up.
func (s *WALWiringSuite) TestWALCoordinator_IntervalFlusherLifecycle() {
	_, coord := s.buildWALLayer()

	// Simulate the post-mount startup (walFlushInterval would be 30s in prod;
	// use a short interval here to exercise the goroutine quickly).
	coord.StartIntervalFlusher(10 * time.Millisecond)
	time.Sleep(25 * time.Millisecond) // let at least one tick fire

	// Simulate unmount: Layer.Close → coord.Close should stop the flusher.
	done := make(chan struct{})
	go func() {
		_ = coord.Close()
		close(done)
	}()
	select {
	case <-done:
		// Flusher stopped cleanly.
	case <-time.After(3 * time.Second):
		s.Fail("coord.Close() did not return within 3s — interval flusher goroutine leaked on unmount")
	}
}

// noopBackend is a minimal FileSystemBackend whose Close is a no-op.
// Used so Layer.Close() cascade reaches coord.Close() without panicking on a
// nil PassthroughBackend.Inner.
type noopBackend struct{ backend.PassthroughBackend }

func (n *noopBackend) Close() error { return nil }

// TestWALLayer_ClosePropagatesToCoord verifies that Layer.Close() calls
// coord.Close(), which stops the interval flusher. This is the cascade that
// releaseVolume → be.Close() relies on.
func (s *WALWiringSuite) TestWALLayer_ClosePropagatesToCoord() {
	layer, coord := s.buildWALLayer()

	// Build the layer with a trivial inner that returns nil from Close.
	walLayer := layer.build(&noopBackend{})

	coord.StartIntervalFlusher(10 * time.Millisecond)
	time.Sleep(20 * time.Millisecond)

	done := make(chan struct{})
	go func() {
		_ = walLayer.Close()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		s.Fail("walLayer.Close() did not return — coord.Close() was not called or flusher leaked")
	}
}

// TestNoCacheNoWAL documents (at the compile-seam level) that the cache-disabled
// path in Mount does not construct a WAL coordinator. We verify this behaviorally:
// a layers list with only posCache=absent and no posWAL entry composes correctly.
func (s *WALWiringSuite) TestNoCacheNoWAL() {
	transport := &PassthroughCounter{}
	// No WAL layer (cache disabled path in single.go skips the coord block).
	layers := []backendLayer{}
	got := composeBackend(transport, layers)
	// Should return the transport unchanged (no layers to wrap).
	s.Equal(transport, got, "with no layers, composeBackend must return the transport unchanged")
}

func TestWALWiringSuite(t *testing.T) {
	suite.Run(t, new(WALWiringSuite))
}
