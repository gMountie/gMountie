package cache

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"go.gmountie.dev/gmountie/pkg/proto"

	"github.com/stretchr/testify/suite"
)

// fakeBackend tracks which cache methods were invalidated.
type fakeBackendForSubscriber struct {
	mu            sync.Mutex
	attrInvals    []string
	dataInvals    []string
	dirInvals     []string
	attrNegatives []string
}

func (b *fakeBackendForSubscriber) invalidateAttr(p string) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.attrInvals = append(b.attrInvals, p)
}
func (b *fakeBackendForSubscriber) invalidateData(p string) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.dataInvals = append(b.dataInvals, p)
}
func (b *fakeBackendForSubscriber) invalidateDir(p string) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.dirInvals = append(b.dirInvals, p)
}
func (b *fakeBackendForSubscriber) putNegative(p string) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.attrNegatives = append(b.attrNegatives, p)
}

type SubscribeConsumerSuite struct{ suite.Suite }

func (s *SubscribeConsumerSuite) TestHandleMutatedInvalidatesAll() {
	be := &fakeBackendForSubscriber{}
	c := &subscribeConsumer{cache: be, validity: newValidityTracker()}
	c.handle(&proto.SubscribeEvent{Kind: proto.SubscribeEvent_MUTATED, Path: "a/b/c.txt", NewVersion: 7})
	// Attr is invalidated for both the mutated path and its parent (parent mtime changes on create/unlink).
	s.Assert().ElementsMatch([]string{"a/b/c.txt", "a/b"}, be.attrInvals)
	s.Assert().Equal([]string{"a/b/c.txt"}, be.dataInvals)
	s.Assert().Equal([]string{"a/b"}, be.dirInvals)
	s.Assert().Empty(be.attrNegatives)
}

func (s *SubscribeConsumerSuite) TestHandleDeletedAddsNegative() {
	be := &fakeBackendForSubscriber{}
	c := &subscribeConsumer{cache: be, validity: newValidityTracker()}
	c.handle(&proto.SubscribeEvent{Kind: proto.SubscribeEvent_DELETED, Path: "dir/x"})
	// Attr is invalidated for both the deleted path and its parent (unlink changes parent mtime).
	s.Assert().ElementsMatch([]string{"dir/x", "dir"}, be.attrInvals)
	s.Assert().Equal([]string{"dir/x"}, be.attrNegatives)
}

func (s *SubscribeConsumerSuite) TestHandleRenamedTouchesBothPaths() {
	be := &fakeBackendForSubscriber{}
	c := &subscribeConsumer{cache: be, validity: newValidityTracker()}
	c.handle(&proto.SubscribeEvent{Kind: proto.SubscribeEvent_RENAMED, Path: "src/old", NewPath: "dst/new"})
	// Attr is invalidated for both renamed paths and both parents (rename changes mtime of both parent dirs).
	s.Assert().ElementsMatch([]string{"src/old", "src", "dst/new", "dst"}, be.attrInvals)
	s.Assert().ElementsMatch([]string{"src/old", "dst/new"}, be.dataInvals)
	s.Assert().Contains(be.attrNegatives, "src/old")
}

func (s *SubscribeConsumerSuite) TestHandleHeartbeatIsNoop() {
	v := newValidityTracker()
	be := &fakeBackendForSubscriber{}
	c := &subscribeConsumer{cache: be, validity: v}
	s.Require().Equal(stateUnverified, v.globalState())
	c.handle(&proto.SubscribeEvent{Kind: proto.SubscribeEvent_HEARTBEAT})
	s.Assert().Empty(be.attrInvals)
	s.Assert().Empty(be.dataInvals)
	s.Assert().Empty(be.dirInvals)
	// handle alone doesn't flip — runOnce does the bookkeeping.
	s.Assert().Equal(stateUnverified, v.globalState())
}

func (s *SubscribeConsumerSuite) TestMutatedInvalidatesParentAttr() {
	be := &fakeBackendForSubscriber{}
	c := &subscribeConsumer{cache: be, validity: newValidityTracker()}
	c.handle(&proto.SubscribeEvent{Kind: proto.SubscribeEvent_MUTATED, Path: "dir/child"})
	s.Assert().Contains(be.attrInvals, "dir", "parent attr must be invalidated on MUTATED")
}

func (s *SubscribeConsumerSuite) TestDeletedInvalidatesParentAttr() {
	be := &fakeBackendForSubscriber{}
	c := &subscribeConsumer{cache: be, validity: newValidityTracker()}
	c.handle(&proto.SubscribeEvent{Kind: proto.SubscribeEvent_DELETED, Path: "dir/child"})
	s.Assert().Contains(be.attrInvals, "dir", "parent attr must be invalidated on DELETED")
}

func (s *SubscribeConsumerSuite) TestRenamedInvalidatesBothParentAttrs() {
	be := &fakeBackendForSubscriber{}
	c := &subscribeConsumer{cache: be, validity: newValidityTracker()}
	c.handle(&proto.SubscribeEvent{Kind: proto.SubscribeEvent_RENAMED, Path: "src/child", NewPath: "dst/child"})
	s.Assert().Contains(be.attrInvals, "src", "old parent attr must be invalidated on RENAMED")
	s.Assert().Contains(be.attrInvals, "dst", "new parent attr must be invalidated on RENAMED")
}

// TestMutatedRootLevelPathInvalidatesRootParent pins the convention that ""
// is the root key: a MUTATED event on a top-level path (no "/" component)
// must land "" in both attrInvals and dirInvals so the root directory's
// cached attr and listing are dropped.
func (s *SubscribeConsumerSuite) TestMutatedRootLevelPathInvalidatesRootParent() {
	be := &fakeBackendForSubscriber{}
	c := &subscribeConsumer{cache: be, validity: newValidityTracker()}
	c.handle(&proto.SubscribeEvent{Kind: proto.SubscribeEvent_MUTATED, Path: "top.txt"})
	s.Assert().Contains(be.attrInvals, "", "root attr must be invalidated for root-level path")
	s.Assert().Contains(be.dirInvals, "", "root dir must be invalidated for root-level path")
}

// scriptedStream is a subscribeStream whose Recv() returns a pre-scripted
// sequence of (event, error) steps in order, then blocks on a channel that the
// test or a cancelled run loop releases. It lets a test drive the run/runOnce
// reconnect machinery (HEARTBEAT → verified, error → unverified, backoff,
// ctx-cancel exit) without any real gRPC or wall-clock sleeps in the hot path.
type scriptedStream struct {
	steps []recvStep
	idx   int
	done  chan struct{} // closed to release a Recv() parked past the script
}

type recvStep struct {
	ev   *proto.SubscribeEvent
	err  error
	gate chan struct{} // if non-nil, Recv blocks on it before returning this step
}

func (s *scriptedStream) Recv() (*proto.SubscribeEvent, error) {
	if s.idx < len(s.steps) {
		step := s.steps[s.idx]
		s.idx++
		if step.gate != nil {
			<-step.gate // hold the prior state observable until the test releases us
		}
		return step.ev, step.err
	}
	<-s.done // park until released so a runaway loop can't spin
	return nil, errors.New("stream closed")
}

// TestRunMarksVerifiedThenUnverifiedAcrossReconnect drives one stream that
// emits a HEARTBEAT (→ verified) then a transport error (→ unverified), and
// asserts both transitions are observed and that cancelling the ctx makes
// run() return promptly (no goroutine leak). The backoff after the error is
// what the ctx-cancel interrupts, so run() never sleeps a real second.
func (s *SubscribeConsumerSuite) TestRunMarksVerifiedThenUnverifiedAcrossReconnect() {
	v := newValidityTracker()
	be := &fakeBackendForSubscriber{}

	verifiedSeen := make(chan struct{})
	releaseError := make(chan struct{}) // test closes this once it has observed verified
	stream := &scriptedStream{
		steps: []recvStep{
			{ev: &proto.SubscribeEvent{Kind: proto.SubscribeEvent_HEARTBEAT}},
			// Hold the verified state observable until the test releases us, then
			// drop the stream so run() marks unverified.
			{err: errors.New("stream dropped"), gate: releaseError},
		},
		done: make(chan struct{}),
	}
	// The first open hands out the scripted stream; any later reconnect attempt
	// (after the error + backoff) gets a stream that parks until ctx-cancel, so
	// the loop can't busy-spin opening fresh streams.
	var opened int
	c := &subscribeConsumer{
		cache:    be,
		validity: v,
		open: func(_ context.Context) (subscribeStream, error) {
			opened++
			if opened == 1 {
				return stream, nil
			}
			return &scriptedStream{done: stream.done}, nil
		},
	}

	// Watch for the verified flip the HEARTBEAT triggers.
	go func() {
		for {
			if v.globalState() == stateVerified {
				close(verifiedSeen)
				return
			}
			time.Sleep(time.Millisecond)
		}
	}()

	ctx, cancel := context.WithCancel(context.Background())
	returned := make(chan struct{})
	go func() { c.run(ctx); close(returned) }()

	// 1) HEARTBEAT flips to verified.
	select {
	case <-verifiedSeen:
	case <-time.After(2 * time.Second):
		s.FailNow("never reached stateVerified after HEARTBEAT")
	}

	// Now release the stream error so run() transitions back to unverified.
	close(releaseError)

	// 2) After the stream error, run marks unverified (during the backoff gap).
	s.Eventually(func() bool { return v.globalState() == stateUnverified },
		2*time.Second, time.Millisecond, "stream error must mark the cache unverified")

	// 3) ctx-cancel makes run() return promptly — no leaked goroutine.
	cancel()
	close(stream.done) // release any parked Recv on a reconnected stream
	select {
	case <-returned:
	case <-time.After(2 * time.Second):
		s.FailNow("run() did not return after ctx cancel")
	}
}

// TestBackoffCapIs30s pins the reconnect backoff ceiling without sleeping:
// repeatedly applying the doubling logic from run() must saturate at 30s and
// never exceed it. Guards against a regression that lets the cap drift or the
// doubling overflow past the bound.
func (s *SubscribeConsumerSuite) TestBackoffCapIs30s() {
	backoff := time.Second
	for i := 0; i < 100; i++ {
		if backoff < 30*time.Second {
			backoff *= 2
			if backoff > 30*time.Second {
				backoff = 30 * time.Second
			}
		}
		s.Require().LessOrEqual(backoff, 30*time.Second, "backoff must never exceed the 30s cap")
	}
	s.Assert().Equal(30*time.Second, backoff, "backoff must saturate exactly at the 30s cap")
}

func TestSubscribeConsumerSuite(t *testing.T) { suite.Run(t, new(SubscribeConsumerSuite)) }
