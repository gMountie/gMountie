package grpc

import (
	"context"
	"path"

	"gmountie/pkg/client/metrics"

	"google.golang.org/grpc"
)

// UnaryClientInFlightInterceptor increments InFlight{op} before the
// invoker call and decrements via defer so error paths still decrement.
// op is derived from path.Base(method) — for "/gmountie.RpcFs/Mkdir" that
// yields "Mkdir", matching the server-side op label convention.
func UnaryClientInFlightInterceptor(m *metrics.Metrics) grpc.UnaryClientInterceptor {
	return func(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
		op := path.Base(method)
		m.InFlightInc(op)
		defer m.InFlightDec(op)
		return invoker(ctx, method, req, reply, cc, opts...)
	}
}
