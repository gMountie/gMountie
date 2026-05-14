package grpc

import (
	"context"
	"io"
	"sync"
	"sync/atomic"
	"time"

	"gmountie/pkg/proto"
	"gmountie/pkg/utils/log"

	"github.com/pkg/errors"
	"go.uber.org/zap"
)

const (
	recoveryInitialBackoff = 200 * time.Millisecond
	recoveryMaxBackoff     = 5 * time.Second
)

// SessionHandshake owns the client-side lifecycle of a server session: it
// calls Create on connect, runs a goroutine that drains the Keepalive
// stream, and — when the stream breaks — reattaches via Resume (or falls
// back to a fresh Create) and reopens the stream. The loop exits only
// when Close cancels the long-lived stream context.
type SessionHandshake struct {
	client    proto.SessionServiceClient
	sessionID string
	running   atomic.Bool

	streamCtx    context.Context
	streamCancel context.CancelFunc
	done         chan struct{}

	mu sync.Mutex
}

func NewSessionHandshake(client proto.SessionServiceClient) *SessionHandshake {
	return &SessionHandshake{client: client}
}

func (h *SessionHandshake) SessionID() string {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.sessionID
}

func (h *SessionHandshake) IsRunning() bool {
	return h.running.Load()
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
	stream, err := h.client.Keepalive(streamCtx, &proto.KeepaliveRequest{SessionId: reply.SessionId})
	if err != nil {
		cancel()
		return errors.Wrap(err, "session keepalive open")
	}

	h.streamCtx = streamCtx
	h.streamCancel = cancel
	h.done = make(chan struct{})
	h.running.Store(true)
	go h.keepaliveLoop(stream)
	return nil
}

// keepaliveLoop drains the current stream, and on any non-EOF error tries
// to re-establish the session (Resume → Create fallback). Exits only when
// h.streamCtx is cancelled.
func (h *SessionHandshake) keepaliveLoop(initial proto.SessionService_KeepaliveClient) {
	defer func() {
		h.running.Store(false)
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
				if err == io.EOF {
					log.Log.Info("keepalive stream closed by server; recovering",
						zap.String("session_id", h.SessionID()))
				} else {
					log.Log.Warn("keepalive stream errored; recovering",
						zap.String("session_id", h.SessionID()),
						zap.Error(err))
				}
				break
			}
		}

		newStream, err := h.recover()
		if err != nil {
			// recover() only returns an error when streamCtx is cancelled.
			return
		}
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
		log.Log.Warn("session recovery attempt failed; backing off",
			zap.Duration("backoff", backoff),
			zap.Error(err))

		select {
		case <-h.streamCtx.Done():
			return nil, h.streamCtx.Err()
		case <-time.After(backoff):
		}
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
func (h *SessionHandshake) tryReattach() (proto.SessionService_KeepaliveClient, error) {
	currentID := h.SessionID()
	if currentID != "" {
		resp, err := h.client.Resume(h.streamCtx, &proto.SessionResumeRequest{SessionId: currentID})
		if err != nil {
			return nil, errors.Wrap(err, "resume")
		}
		if resp.Resumed {
			log.Log.Info("session resumed", zap.String("session_id", currentID))
			return h.client.Keepalive(h.streamCtx, &proto.KeepaliveRequest{SessionId: currentID})
		}
	}
	// Resume said no — fall back to a fresh Create.
	resp, err := h.client.Create(h.streamCtx, &proto.SessionCreateRequest{})
	if err != nil {
		return nil, errors.Wrap(err, "create after resume failure")
	}
	h.setSessionID(resp.SessionId)
	log.Log.Info("session re-created after resume failure (open fds are now invalid)",
		zap.String("old_session_id", currentID),
		zap.String("new_session_id", resp.SessionId))
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
