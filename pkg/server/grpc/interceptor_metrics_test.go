package grpc

import (
	"context"
	"testing"

	"go.gmountie.dev/gmountie/pkg/server/metrics"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/suite"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type vReq struct{}

func (vReq) GetVolume() string { return "photos" }

type MetricsInterceptorTestSuite struct {
	suite.Suite
	m *metrics.Metrics
}

func (s *MetricsInterceptorTestSuite) SetupTest() { s.m = metrics.NewMetrics() }

func (s *MetricsInterceptorTestSuite) TestRecordsDurationAndError() {
	interceptor := UnaryServerMetricsInterceptor(s.m)
	info := &grpc.UnaryServerInfo{FullMethod: "/gmountie.RpcFs/Mkdir"}
	handler := func(ctx context.Context, req any) (any, error) {
		return nil, status.Error(codes.Unavailable, "boom")
	}
	_, err := interceptor(context.Background(), vReq{}, info, handler)
	s.Require().Error(err)
	s.Assert().Equal(1, int(testutil.ToFloat64(s.m.RpcErrors.WithLabelValues("photos", "Mkdir", "Unavailable"))))
	s.Assert().Equal(1, testutil.CollectAndCount(s.m.RequestDuration))
}

func (s *MetricsInterceptorTestSuite) TestNoErrorCounterOnOK() {
	interceptor := UnaryServerMetricsInterceptor(s.m)
	info := &grpc.UnaryServerInfo{FullMethod: "/gmountie.RpcFs/Mkdir"}
	handler := func(ctx context.Context, req any) (any, error) { return struct{}{}, nil }
	_, err := interceptor(context.Background(), vReq{}, info, handler)
	s.Require().NoError(err)
	s.Assert().Equal(0, int(testutil.ToFloat64(s.m.RpcErrors.WithLabelValues("photos", "Mkdir", "OK"))))
	s.Assert().Equal(1, testutil.CollectAndCount(s.m.RequestDuration))
}

func (s *MetricsInterceptorTestSuite) TestMissingVolumeGetter() {
	// Requests without GetVolume (e.g. SessionService) tag volume="".
	interceptor := UnaryServerMetricsInterceptor(s.m)
	info := &grpc.UnaryServerInfo{FullMethod: "/gmountie.SessionService/Create"}
	handler := func(ctx context.Context, req any) (any, error) { return struct{}{}, nil }
	_, err := interceptor(context.Background(), struct{}{}, info, handler)
	s.Require().NoError(err)
	s.Assert().Equal(1, testutil.CollectAndCount(s.m.RequestDuration))
}

func TestMetricsInterceptorTestSuite(t *testing.T) {
	suite.Run(t, new(MetricsInterceptorTestSuite))
}
