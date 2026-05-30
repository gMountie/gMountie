package grpc

import (
	"context"

	"gmountie/pkg/common"

	"github.com/grpc-ecosystem/go-grpc-middleware/v2/interceptors/logging"
	"google.golang.org/grpc"
)

// SessionIDCarrier matches any proto request with a GetSessionId getter.
type SessionIDCarrier interface{ GetSessionId() string }

// VolumeCarrier matches any proto request with a GetVolume getter.
type VolumeCarrier interface{ GetVolume() string }

// ServerUnaryLogContext peeks at the request via the standard proto-
// generated getters and injects session_id / volume as log fields so
// downstream finish-call log lines pick them up.
func ServerUnaryLogContext() grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		if r, ok := req.(SessionIDCarrier); ok {
			if id := r.GetSessionId(); id != "" {
				ctx = logging.InjectLogField(ctx, "session_fp", common.FingerprintID(id))
			}
		}
		if r, ok := req.(VolumeCarrier); ok {
			if v := r.GetVolume(); v != "" {
				ctx = logging.InjectLogField(ctx, "volume", v)
			}
		}
		return handler(ctx, req)
	}
}
