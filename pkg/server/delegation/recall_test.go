package delegation

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/suite"
	"go.gmountie.dev/gmountie/pkg/proto"
)

type RecallSuite struct{ suite.Suite }

func TestRecallSuite(t *testing.T) { suite.Run(t, new(RecallSuite)) }

func (s *RecallSuite) TestRecallSucceedsOnAck() {
	reg := NewRecallRegistry(time.Second)
	var got atomic.Pointer[proto.RecallMsg]
	release := reg.Register("sessA", func(m *proto.RecallMsg) error { got.Store(m); return nil })
	defer release()

	done := make(chan error, 1)
	go func() { done <- reg.Recall("sessA", "proj/src") }()

	s.Eventually(func() bool { return got.Load() != nil }, time.Second, time.Millisecond)
	msg := got.Load()
	reg.Ack("sessA", msg.RecallId, true, proto.FsError_FS_OK)
	s.Require().NoError(<-done)
	s.Equal("proj/src", msg.Root)
}

func (s *RecallSuite) TestRecallTimesOutWithoutAck() {
	reg := NewRecallRegistry(50 * time.Millisecond)
	release := reg.Register("sessA", func(m *proto.RecallMsg) error { return nil })
	defer release()
	s.Error(reg.Recall("sessA", "x"))
}

func (s *RecallSuite) TestRecallNoStreamIsError() {
	reg := NewRecallRegistry(time.Second)
	s.Error(reg.Recall("ghost", "x")) // never registered -> treat as released
}

func (s *RecallSuite) TestAbortedAckFailsRecallImmediately() {
	reg := NewRecallRegistry(30 * time.Second) // long timeout: the test must NOT wait it out
	sent := make(chan *proto.RecallMsg, 1)
	release := reg.Register("sess-a", func(m *proto.RecallMsg) error { sent <- m; return nil })
	defer release()

	done := make(chan error, 1)
	go func() { done <- reg.Recall("sess-a", "proj") }()
	msg := <-sent
	reg.Ack("sess-a", msg.RecallId, false, proto.FsError_FS_EIO)

	select {
	case err := <-done:
		s.Require().Error(err, "aborted ack must fail the recall")
		s.Contains(err.Error(), "abort")
	case <-time.After(2 * time.Second):
		s.Fail("Recall waited for the timeout despite an explicit abort ack")
	}
}

func (s *RecallSuite) TestConcurrentRecallsDistinctIDs() {
	reg := NewRecallRegistry(time.Second)
	var mu sync.Mutex
	ids := map[uint64]bool{}
	release := reg.Register("sessA", func(m *proto.RecallMsg) error {
		mu.Lock()
		ids[m.RecallId] = true
		mu.Unlock()
		go reg.Ack("sessA", m.RecallId, true, proto.FsError_FS_OK)
		return nil
	})
	defer release()
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() { defer wg.Done(); _ = reg.Recall("sessA", "r") }()
	}
	wg.Wait()
	s.Len(ids, 8)
}
