package grpc

import (
	"context"
	"sync"

	"go.gmountie.dev/gmountie/pkg/common"

	"github.com/google/uuid"
	"github.com/grpc-ecosystem/go-grpc-middleware/v2/interceptors/logging"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

// ServerStreamRequestID mirrors ServerUnaryRequestID for streaming RPCs: it
// extracts the request id from incoming metadata (or generates a fresh UUID)
// and injects it both on the context (NewContextWithRequestID) and as a log
// field. Metadata is available at stream start, so the plain inject-before-
// the-logger pattern works — unlike session/volume, which only arrive with
// the first message (see ServerStreamLogContext).
func ServerStreamRequestID() grpc.StreamServerInterceptor {
	return func(srv any, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		ctx := ss.Context()
		id := ""
		if md, ok := metadata.FromIncomingContext(ctx); ok {
			if vals := md.Get(RequestIDMetadataKey); len(vals) > 0 && vals[0] != "" {
				id = vals[0]
			}
		}
		if id == "" {
			id = uuid.NewString()
		}
		ctx = NewContextWithRequestID(ctx, id)
		ctx = logging.InjectLogField(ctx, "request_id", id)
		return handler(srv, &ctxServerStream{ServerStream: ss, ctx: ctx})
	}
}

// streamLogState is a mutable holder filled by the first received message of
// a stream. It must be mutable: the finish-call logging interceptor captures
// its context when the stream STARTS, before any message has been received,
// so late-arriving fields can only reach it through shared state read at
// PostCall time (via StreamLogFields + logging.WithFieldsFromContext).
type streamLogState struct {
	mu        sync.Mutex
	sessionFP string
	volume    string
}

func (st *streamLogState) capture(msg any) {
	st.mu.Lock()
	defer st.mu.Unlock()
	if st.sessionFP == "" {
		if r, ok := msg.(SessionIDCarrier); ok {
			if id := r.GetSessionId(); id != "" {
				st.sessionFP = common.FingerprintID(id)
			}
		}
	}
	if st.volume == "" {
		if r, ok := msg.(VolumeCarrier); ok {
			if v := r.GetVolume(); v != "" {
				st.volume = v
			}
		}
	}
}

func (st *streamLogState) fields() logging.Fields {
	st.mu.Lock()
	defer st.mu.Unlock()
	var f logging.Fields
	if st.sessionFP != "" {
		f = append(f, "session_fp", st.sessionFP)
	}
	if st.volume != "" {
		f = append(f, "volume", st.volume)
	}
	return f
}

type streamLogStateKey struct{}

// ServerStreamLogContext is the stream counterpart of ServerUnaryLogContext.
// It places a streamLogState holder on the stream context and wraps RecvMsg
// to capture session_fp (fingerprinted, never the raw bearer token) and
// volume from the first message that carries them. Both server-streaming
// requests and client-streaming header frames pass through RecvMsg, so the
// capture covers Read/Subscribe/Keepalive and Write alike.
//
// Pair it with logging.WithFieldsFromContext(StreamLogFields) on the
// finish-call logger — plain ctx field injection cannot work here because the
// fields only become known after the logger has captured its context.
func ServerStreamLogContext() grpc.StreamServerInterceptor {
	return func(srv any, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		st := &streamLogState{}
		ctx := context.WithValue(ss.Context(), streamLogStateKey{}, st)
		return handler(srv, &peekServerStream{ServerStream: ss, ctx: ctx, state: st})
	}
}

// StreamLogFields is a logging.WithFieldsFromContext hook: at log-emission
// time it returns the session_fp/volume captured from the stream's first
// message. It returns nil for unary RPCs (no holder in ctx) and for streams
// that never received a carrier message, so it is safe to install on a
// logger shared between the unary and stream chains.
func StreamLogFields(ctx context.Context) logging.Fields {
	st, ok := ctx.Value(streamLogStateKey{}).(*streamLogState)
	if !ok {
		return nil
	}
	return st.fields()
}

// ctxServerStream overrides Context() on a wrapped grpc.ServerStream.
type ctxServerStream struct {
	grpc.ServerStream
	ctx context.Context
}

func (s *ctxServerStream) Context() context.Context { return s.ctx }

// peekServerStream feeds every received message to the streamLogState.
type peekServerStream struct {
	grpc.ServerStream
	ctx   context.Context
	state *streamLogState
}

func (s *peekServerStream) Context() context.Context { return s.ctx }

func (s *peekServerStream) RecvMsg(msg any) error {
	err := s.ServerStream.RecvMsg(msg)
	if err == nil {
		s.state.capture(msg)
	}
	return err
}
