package grpc

import (
	"context"
	"path"
	"sync"
	"time"

	commongrpc "go.gmountie.dev/gmountie/pkg/common/grpc"
	"go.gmountie.dev/gmountie/pkg/server/metrics"

	"google.golang.org/grpc"
	"google.golang.org/grpc/status"
)

// UnaryServerMetricsInterceptor records per-RPC duration and error
// counters tagged with volume + op + (on error) the gRPC code. The
// volume is read from the request via the GetVolume getter — requests
// without one (e.g. SessionService.Create) are tagged with volume="".
func UnaryServerMetricsInterceptor(m *metrics.Metrics) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		start := time.Now()
		volume := ""
		if v, ok := req.(commongrpc.VolumeCarrier); ok {
			volume = v.GetVolume()
		}
		// "/gmountie.RpcFs/Mkdir" -> "Mkdir"
		op := path.Base(info.FullMethod)

		resp, err := handler(ctx, req)
		m.RequestDurationObserve(volume, op, time.Since(start).Seconds())
		if err != nil {
			m.RpcErrorInc(volume, op, status.Code(err).String())
		}
		return resp, err
	}
}

// StreamServerMetricsInterceptor records per-RPC duration and error counters
// for streaming RPCs (Read/Write/Subscribe/Keepalive) — the hottest data-path
// methods, previously invisible to gmountie_server_request_duration_seconds
// and rpc_errors_total. The volume label is read from the first received
// message carrying a GetVolume getter: for server-streaming methods the
// request passes through RecvMsg before the handler runs; for client-
// streaming Write it is the header frame. Streams that never receive a
// volume-carrying message (e.g. Keepalive) are tagged volume="".
//
// Duration here is whole-stream wall time (open → finish), not per-frame —
// the per-frame signal lives in gmountie_server_bytes_total.
func StreamServerMetricsInterceptor(m *metrics.Metrics) grpc.StreamServerInterceptor {
	return func(srv any, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		start := time.Now()
		ws := &volumePeekStream{ServerStream: ss}
		err := handler(srv, ws)
		op := path.Base(info.FullMethod)
		m.RequestDurationObserve(ws.volume(), op, time.Since(start).Seconds())
		if err != nil {
			m.RpcErrorInc(ws.volume(), op, status.Code(err).String())
		}
		return err
	}
}

// volumePeekStream captures the volume from the first received message that
// carries one. The mutex guards against the (handler goroutine) writer racing
// the (interceptor) reader on handler return.
type volumePeekStream struct {
	grpc.ServerStream
	mu  sync.Mutex
	vol string
}

func (s *volumePeekStream) RecvMsg(msg any) error {
	err := s.ServerStream.RecvMsg(msg)
	if err == nil {
		if v, ok := msg.(commongrpc.VolumeCarrier); ok {
			s.mu.Lock()
			if s.vol == "" {
				s.vol = v.GetVolume()
			}
			s.mu.Unlock()
		}
	}
	return err
}

func (s *volumePeekStream) volume() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.vol
}
