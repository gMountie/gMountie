package grpc

import (
	"context"
	"io"
	"sync"
	"sync/atomic"

	"gmountie/pkg/proto"
	"gmountie/pkg/utils/log"

	"github.com/pkg/errors"
	"go.uber.org/zap"
)

// SessionHandshake owns the client-side lifecycle of a server session: it
// calls Create on connect, then runs a goroutine that drains the Keepalive
// stream until either the server closes it or the local Close() is called.
type SessionHandshake struct {
	client    proto.SessionServiceClient
	sessionID string
	running   atomic.Bool

	cancel context.CancelFunc
	done   chan struct{}

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

// Establish calls Create then starts the Keepalive stream in a goroutine.
// Returns as soon as Create succeeds; the Keepalive goroutine continues in
// the background. Subsequent calls without an intervening Close are a
// no-op (returns nil, leaves existing sessionID intact).
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

	h.mu.Lock()
	h.sessionID = reply.SessionId
	h.mu.Unlock()

	streamCtx, cancel := context.WithCancel(context.Background())
	h.cancel = cancel
	h.done = make(chan struct{})

	stream, err := h.client.Keepalive(streamCtx, &proto.KeepaliveRequest{SessionId: reply.SessionId})
	if err != nil {
		cancel()
		return errors.Wrap(err, "session keepalive open")
	}

	h.running.Store(true)
	go h.drainKeepalive(stream)
	return nil
}

func (h *SessionHandshake) drainKeepalive(stream proto.SessionService_KeepaliveClient) {
	defer func() {
		h.running.Store(false)
		close(h.done)
	}()
	for {
		if _, err := stream.Recv(); err != nil {
			if err != io.EOF {
				log.Log.Warn("session keepalive stream ended",
					zap.String("session_id", h.SessionID()),
					zap.Error(err))
			}
			return
		}
	}
}

// Close cancels the keepalive stream and waits for the background goroutine
// to exit.
func (h *SessionHandshake) Close() error {
	if h.cancel != nil {
		h.cancel()
	}
	if h.done != nil {
		<-h.done
	}
	return nil
}
