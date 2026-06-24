package delegation

import (
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pkg/errors"
	"go.gmountie.dev/gmountie/pkg/proto"
)

// ErrNoStream is returned by RecallRegistry.Recall when the owner session has no
// registered recall stream (it disconnected or never opened one). The arbiter
// treats this specifically (see OnMutation): an unreachable holder is handed off
// on contention — its delegation gen is fenced and the contender proceeds —
// rather than blocking the contender until the grace-period reap.
var ErrNoStream = errors.New("recall: no recall stream for session")

// Recaller is the arbiter's view of the recall transport.
type Recaller interface {
	Recall(ownerSession, root string) error
}

type pending struct {
	ackCh chan struct{}
}

type streamSlot struct {
	send func(*proto.RecallMsg) error
}

// RecallRegistry maps owner session_id -> its open Recall stream and correlates
// RecallMsg/RecallAck by recall_id. Safe for concurrent use.
type RecallRegistry struct {
	timeout time.Duration
	nextID  atomic.Uint64

	mu       sync.Mutex
	streams  map[string]*streamSlot
	inflight map[uint64]*pending
}

func NewRecallRegistry(timeout time.Duration) *RecallRegistry {
	return &RecallRegistry{
		timeout:  timeout,
		streams:  make(map[string]*streamSlot),
		inflight: make(map[uint64]*pending),
	}
}

// Register installs the send-fn for a session's Recall stream and returns a
// release closure to call when the stream ends (deregister).
func (r *RecallRegistry) Register(sessionID string, send func(*proto.RecallMsg) error) (release func()) {
	slot := &streamSlot{send: send}
	r.mu.Lock()
	r.streams[sessionID] = slot
	r.mu.Unlock()
	return func() {
		r.mu.Lock()
		if r.streams[sessionID] == slot {
			delete(r.streams, sessionID)
		}
		r.mu.Unlock()
	}
}

// Ack completes the in-flight recall for recallID, if any.
func (r *RecallRegistry) Ack(sessionID string, recallID uint64) {
	r.mu.Lock()
	p := r.inflight[recallID]
	delete(r.inflight, recallID)
	r.mu.Unlock()
	if p != nil {
		close(p.ackCh)
	}
}

// Recall pushes a RecallMsg to ownerSession and blocks until the matching Ack
// or the timeout. Never holds any caller lock — the arbiter calls this AFTER
// releasing its table mutex.
func (r *RecallRegistry) Recall(ownerSession, root string) error {
	id := r.nextID.Add(1)
	p := &pending{ackCh: make(chan struct{})}

	r.mu.Lock()
	slot := r.streams[ownerSession]
	if slot == nil {
		r.mu.Unlock()
		return fmt.Errorf("%w %s", ErrNoStream, ownerSession)
	}
	r.inflight[id] = p
	r.mu.Unlock()

	if err := slot.send(&proto.RecallMsg{Root: root, RecallId: id}); err != nil {
		r.mu.Lock()
		delete(r.inflight, id)
		r.mu.Unlock()
		return errors.Wrap(err, "recall: send")
	}

	select {
	case <-p.ackCh:
		return nil
	case <-time.After(r.timeout):
		r.mu.Lock()
		delete(r.inflight, id)
		r.mu.Unlock()
		return errors.Errorf("recall: timed out after %s waiting for ack from %s", r.timeout, ownerSession)
	}
}
