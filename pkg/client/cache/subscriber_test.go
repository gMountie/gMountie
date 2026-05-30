package cache

import (
	"sync"
	"testing"

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
	s.Assert().Equal([]string{"a/b/c.txt"}, be.attrInvals)
	s.Assert().Equal([]string{"a/b/c.txt"}, be.dataInvals)
	s.Assert().Equal([]string{"a/b"}, be.dirInvals)
	s.Assert().Empty(be.attrNegatives)
}

func (s *SubscribeConsumerSuite) TestHandleDeletedAddsNegative() {
	be := &fakeBackendForSubscriber{}
	c := &subscribeConsumer{cache: be, validity: newValidityTracker()}
	c.handle(&proto.SubscribeEvent{Kind: proto.SubscribeEvent_DELETED, Path: "x"})
	s.Assert().Equal([]string{"x"}, be.attrInvals)
	s.Assert().Equal([]string{"x"}, be.attrNegatives)
}

func (s *SubscribeConsumerSuite) TestHandleRenamedTouchesBothPaths() {
	be := &fakeBackendForSubscriber{}
	c := &subscribeConsumer{cache: be, validity: newValidityTracker()}
	c.handle(&proto.SubscribeEvent{Kind: proto.SubscribeEvent_RENAMED, Path: "old", NewPath: "new"})
	s.Assert().ElementsMatch([]string{"old", "new"}, be.attrInvals)
	s.Assert().ElementsMatch([]string{"old", "new"}, be.dataInvals)
	s.Assert().Contains(be.attrNegatives, "old")
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

func TestSubscribeConsumerSuite(t *testing.T) { suite.Run(t, new(SubscribeConsumerSuite)) }
