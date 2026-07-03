package delegation

import (
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pkg/errors"
	"go.uber.org/zap"

	"go.gmountie.dev/gmountie/pkg/common"
	"go.gmountie.dev/gmountie/pkg/proto"
	"go.gmountie.dev/gmountie/pkg/utils/log"
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
	// owner is the session the RecallMsg was pushed to — the ONLY session
	// whose ack may complete this recall. recall_ids are guessable
	// (sequential), so without this check any session with a Recall stream
	// could ack another holder's recall: done=true forges a clean handoff
	// against un-flushed holder state; done=false is a targeted DoS.
	owner string
	ackCh chan struct{}
	// err is set (before ackCh closes) when the holder aborted the recall
	// (done=false): its flush failed, the handoff must fail closed NOW rather
	// than after the timeout.
	err error
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

// Ack completes the in-flight recall for recallID. done=false marks the
// recall aborted by the holder; fserr is the holder-reported cause.
//
// Only the session the recall was pushed to may ack it: an ack from any other
// session is logged and ignored (the recall stays pending and, absent the
// owner's ack, times out).
func (r *RecallRegistry) Ack(sessionID string, recallID uint64, done bool, fserr proto.FsError) {
	r.mu.Lock()
	p := r.inflight[recallID]
	if p != nil && p.owner != sessionID {
		// Leave the entry in place: a non-owner cannot complete OR abort a
		// recall it does not hold. p.owner is immutable after creation, so
		// reading it after unlock is safe.
		r.mu.Unlock()
		log.Log.Warn("recall ack from non-owner session ignored",
			zap.Uint64("recall_id", recallID),
			zap.Bool("done", done),
			zap.String("owner_fp", common.FingerprintID(p.owner)),
			zap.String("acker_fp", common.FingerprintID(sessionID)),
		)
		return
	}
	delete(r.inflight, recallID)
	r.mu.Unlock()
	if p != nil {
		if !done {
			p.err = errors.Errorf("recall: holder aborted the recall (fserr=%s)", fserr)
		}
		close(p.ackCh) // err write happens-before the close
	}
}

// Recall pushes a RecallMsg to ownerSession and blocks until the matching Ack
// or the timeout. Never holds any caller lock — the arbiter calls this AFTER
// releasing its table mutex.
func (r *RecallRegistry) Recall(ownerSession, root string) error {
	id := r.nextID.Add(1)
	p := &pending{owner: ownerSession, ackCh: make(chan struct{})}

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
		return p.err
	case <-time.After(r.timeout):
		r.mu.Lock()
		delete(r.inflight, id)
		r.mu.Unlock()
		return errors.Errorf("recall: timed out after %s waiting for ack from %s", r.timeout, ownerSession)
	}
}
