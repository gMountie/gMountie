package grpc

import (
	"context"
	"testing"

	"gmountie/pkg/client/metrics"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/suite"
	"google.golang.org/grpc"
)

type ClientMetricsInterceptorTestSuite struct {
	suite.Suite
	m *metrics.Metrics
}

func (s *ClientMetricsInterceptorTestSuite) SetupTest() { s.m = metrics.NewMetrics() }

func (s *ClientMetricsInterceptorTestSuite) TestIncDuringCallDecAfter() {
	interceptor := UnaryClientInFlightInterceptor(s.m)
	var midCallGauge float64
	invoker := func(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, opts ...grpc.CallOption) error {
		midCallGauge = testutil.ToFloat64(s.m.InFlight.WithLabelValues("Mkdir"))
		return nil
	}
	err := interceptor(context.Background(), "/gmountie.RpcFs/Mkdir", nil, nil, nil, invoker)
	s.Require().NoError(err)
	// Cast to int — these gauges only ever hold integer in-flight counts,
	// so float comparison would be a false-positive precision risk.
	s.Assert().Equal(1, int(midCallGauge), "gauge must read 1 during the invoker")
	s.Assert().Equal(0, int(testutil.ToFloat64(s.m.InFlight.WithLabelValues("Mkdir"))),
		"gauge must read 0 after the call returns")
}

func (s *ClientMetricsInterceptorTestSuite) TestDecreasesOnError() {
	interceptor := UnaryClientInFlightInterceptor(s.m)
	invoker := func(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, opts ...grpc.CallOption) error {
		return context.DeadlineExceeded
	}
	err := interceptor(context.Background(), "/gmountie.RpcFs/Mkdir", nil, nil, nil, invoker)
	s.Require().Error(err)
	s.Assert().Equal(0, int(testutil.ToFloat64(s.m.InFlight.WithLabelValues("Mkdir"))))
}

func TestClientMetricsInterceptorTestSuite(t *testing.T) {
	suite.Run(t, new(ClientMetricsInterceptorTestSuite))
}
