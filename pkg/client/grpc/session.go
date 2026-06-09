package grpc

import (
	"context"
	"io"
	"sync"
	"sync/atomic"
	"time"

	"go.gmountie.dev/gmountie/pkg/common"
	"go.gmountie.dev/gmountie/pkg/proto"
	"go.gmountie.dev/gmountie/pkg/utils/log"

	"github.com/pkg/errors"
	"go.uber.org/zap"
)

const (
	recoveryInitialBackoff     = 200 * time.Millisecond
	recoveryMaxBackoff         = 5 * time.Second
	defaultReattachCallTimeout = 5 * time.Second
)

// SessionHandshake owns the client-side lifecycle of a server session: it
// calls Create on connect, runs a goroutine that drains the Keepalive
// stream, and — when the stream breaks — reattaches via Resume (or falls
// back to a fresh Create) and reopens the stream. The loop exits only
// when Close cancels the long-lived stream context. Establish is not
// safe under concurrent calls; the expected usage is one Establish on
// connect, then one Close at teardown.
type SessionHandshake struct {
	client    proto.SessionServiceClient
	sessionID string
	running   atomic.Bool
	// healthy is true exactly while a keepalive stream is currently open
	// and draining. It goes false initially, during recovery, and after
	// teardown. It is safe to read from any goroutine (atomic).
	healthy atomic.Bool

	streamCtx    context.Context
	streamCancel context.CancelFunc
	done         chan struct{}

	// callTimeout bounds the unary Resume and Create RPCs inside tryReattach.
	// The Keepalive stream itself is still opened on the long-lived streamCtx
	// so a timeout firing does not tear down an otherwise-healthy stream.
	callTimeout time.Duration

	mu sync.Mutex
}

func NewSessionHandshake(client proto.SessionServiceClient) *SessionHandshake {
	return &SessionHandshake{
		client:      client,
		callTimeout: defaultReattachCallTimeout,
	}
}

func (h *SessionHandshake) SessionID() string {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.sessionID
}

func (h *SessionHandshake) isRunning() bool {
	return h.running.Load()
}

// IsHealthy reports whether a keepalive stream is currently open and draining.
// It is true only after Establish succeeds and until the stream breaks (at which
// point it goes false for the duration of recovery) or Close is called.
func (h *SessionHandshake) IsHealthy() bool {
	return h.healthy.Load()
}

func (h *SessionHandshake) setSessionID(id string) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.sessionID = id
}

// Establish calls Create and opens the initial Keepalive stream, then
// launches the background recovery loop.
func (h *SessionHandshake) Establish(ctx context.Context) error {
	h.mu.Lock()
	if h.sessionID != "" {
		h.mu.Unlock()
		return nil
	}
	h.mu.Unlock()

	reply, err := h.client.Create(ctx, &proto.SessionCreateRequest{})
	if err != nil {
		return errors.Wrap(err, "session create")
	}
	h.setSessionID(reply.SessionId)

	streamCtx, cancel := context.WithCancel(context.Background())
	// Open the initial Keepalive on the long-lived streamCtx. A streaming
	// RPC's stream lives for the life of the context it was opened with, so
	// opening it on a short-lived/bounded context and cancelling that would
	// tear the stream down the instant Establish returned — forcing a
	// spurious recover() and a misleading "keepalive stream errored" warning
	// on every connect. The open itself doesn't block (gRPC returns the
	// stream before the first Recv), and the unary Create above is already
	// bounded by ctx, so Connect can't hang here.
	stream, err := h.client.Keepalive(streamCtx, &proto.KeepaliveRequest{SessionId: reply.SessionId})
	if err != nil {
		cancel()
		h.setSessionID("")
		return errors.Wrap(err, "session keepalive open")
	}

	h.streamCtx = streamCtx
	h.streamCancel = cancel
	h.done = make(chan struct{})
	h.running.Store(true)
	h.healthy.Store(true)
	go h.keepaliveLoop(stream)
	return nil
}

// keepaliveLoop drains the current stream, and on any non-EOF error tries
// to re-establish the session (Resume → Create fallback). Exits only when
// h.streamCtx is cancelled.
func (h *SessionHandshake) keepaliveLoop(initial proto.SessionService_KeepaliveClient) {
	defer func() {
		h.running.Store(false)
		h.healthy.Store(false)
		close(h.done)
	}()

	stream := initial
	for {
		// Drain the current stream until error.
		for {
			if _, err := stream.Recv(); err != nil {
				if h.streamCtx.Err() != nil {
					return
				}
				if errors.Is(err, io.EOF) {
					log.Log.Info("keepalive stream closed by server; recovering",
						zap.String("session_fp", common.FingerprintID(h.SessionID())))
				} else {
					log.Log.Warn("keepalive stream errored; recovering",
						zap.String("session_fp", common.FingerprintID(h.SessionID())),
						zap.Error(err))
				}
				// Mark unhealthy before recovery so that basic-auth is
				// re-sent on Create/Resume during the reattach window.
				h.healthy.Store(false)
				break
			}
		}

		newStream, err := h.recover()
		if err != nil {
			// recover() only returns an error when streamCtx is cancelled.
			return
		}
		// Recovery succeeded; the new keepalive stream is open.
		h.healthy.Store(true)
		stream = newStream
	}
}

// recover attempts to reattach via Resume, falling back to Create, with
// capped exponential backoff. Returns the new Keepalive stream on success,
// or an error if h.streamCtx is cancelled.
func (h *SessionHandshake) recover() (proto.SessionService_KeepaliveClient, error) {
	backoff := recoveryInitialBackoff
	for {
		if h.streamCtx.Err() != nil {
			return nil, h.streamCtx.Err()
		}

		stream, err := h.tryReattach()
		if err == nil {
			return stream, nil
		}
		// If Close cancelled streamCtx while we were reattaching, this
		// failure is just teardown — exit quietly instead of logging a
		// recovery warning and backing off.
		if h.streamCtx.Err() != nil {
			return nil, h.streamCtx.Err()
		}
		log.Log.Warn("session recovery attempt failed; backing off",
			zap.Duration("backoff", backoff),
			zap.Error(err))

		t := time.NewTimer(backoff)
		select {
		case <-h.streamCtx.Done():
			t.Stop()
			return nil, h.streamCtx.Err()
		case <-t.C:
		}
		t.Stop()
		if backoff < recoveryMaxBackoff {
			backoff *= 2
			if backoff > recoveryMaxBackoff {
				backoff = recoveryMaxBackoff
			}
		}
	}
}

// tryReattach makes ONE attempt at session recovery: Resume first, then a
// fresh Create if Resume returns Resumed=false. Returns the new Keepalive
// stream on success.
//
// The unary Resume and Create calls are bounded by a callTimeout-derived
// context so a TCP-reachable-but-unresponsive server (e.g. mid rolling-restart)
// cannot stall the recovery loop indefinitely while pending FS ops burn their
// retry budget. The Keepalive stream is deliberately opened on h.streamCtx —
// not the bounded callCtx — so the stream survives past the per-call deadline.
func (h *SessionHandshake) tryReattach() (proto.SessionService_KeepaliveClient, error) {
	callCtx, cancel := context.WithTimeout(h.streamCtx, h.callTimeout)
	defer cancel()

	currentID := h.SessionID()
	if currentID != "" {
		resp, err := h.client.Resume(callCtx, &proto.SessionResumeRequest{SessionId: currentID})
		if err != nil {
			return nil, errors.Wrap(err, "resume")
		}
		if resp.Resumed {
			log.Log.Info("session resumed", zap.String("session_fp", common.FingerprintID(currentID)))
			// Keepalive must use the long-lived streamCtx, not callCtx — a bounded
			// ctx would tear the stream down when the timeout fires.
			return h.client.Keepalive(h.streamCtx, &proto.KeepaliveRequest{SessionId: currentID})
		}
	}
	// Resume said no — fall back to a fresh Create.
	resp, err := h.client.Create(callCtx, &proto.SessionCreateRequest{})
	if err != nil {
		return nil, errors.Wrap(err, "create after resume failure")
	}
	h.setSessionID(resp.SessionId)
	log.Log.Info("session re-created after resume failure (open fds are now invalid)",
		zap.String("old_session_fp", common.FingerprintID(currentID)),
		zap.String("new_session_fp", common.FingerprintID(resp.SessionId)))
	// Same as above: Keepalive on streamCtx so the stream outlives the call budget.
	return h.client.Keepalive(h.streamCtx, &proto.KeepaliveRequest{SessionId: resp.SessionId})
}

// Close cancels the long-lived stream context and waits for the recovery
// loop to exit. Safe to call when Establish was never called or failed.
func (h *SessionHandshake) Close() error {
	if h.streamCancel != nil {
		h.streamCancel()
	}
	if h.done != nil {
		<-h.done
	}
	return nil
}
