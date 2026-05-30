package grpc

import (
	"context"
	"path"
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
