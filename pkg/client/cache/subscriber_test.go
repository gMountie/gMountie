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

func TestSubscribeConsumerSuite(t *testing.T) { suite.Run(t, new(SubscribeConsumerSuite)) }
