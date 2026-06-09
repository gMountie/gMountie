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

// fakeMetricsStream is a minimal grpc.ServerStream delivering one
// volume-carrying message.
type fakeMetricsStream struct {
	grpc.ServerStream
	delivered bool
}

func (f *fakeMetricsStream) Context() context.Context { return context.Background() }

func (f *fakeMetricsStream) RecvMsg(msg any) error {
	if f.delivered {
		return status.Error(codes.Canceled, "done")
	}
	f.delivered = true
	*(msg.(*vMsg)) = vMsg{volume: "photos"}
	return nil
}

type vMsg struct{ volume string }

func (m *vMsg) GetVolume() string { return m.volume }

func (s *MetricsInterceptorTestSuite) TestStreamRecordsDurationWithVolumeFromFirstFrame() {
	interceptor := StreamServerMetricsInterceptor(s.m)
	info := &grpc.StreamServerInfo{FullMethod: "/gmountie.RpcFile/Read"}
	err := interceptor(nil, &fakeMetricsStream{}, info, func(_ any, ss grpc.ServerStream) error {
		var m vMsg
		return ss.RecvMsg(&m) // handler receives the request frame
	})
	s.Require().NoError(err)
	s.Assert().Equal(1, testutil.CollectAndCount(s.m.RequestDuration))
	// The histogram series must carry the volume captured from the first frame.
	c, errC := s.m.RequestDuration.GetMetricWithLabelValues("photos", "Read")
	s.Require().NoError(errC)
	s.Require().NotNil(c)
}

func (s *MetricsInterceptorTestSuite) TestStreamRecordsErrorCode() {
	interceptor := StreamServerMetricsInterceptor(s.m)
	info := &grpc.StreamServerInfo{FullMethod: "/gmountie.RpcFile/Write"}
	err := interceptor(nil, &fakeMetricsStream{}, info, func(_ any, ss grpc.ServerStream) error {
		var m vMsg
		_ = ss.RecvMsg(&m)
		return status.Error(codes.Unavailable, "boom")
	})
	s.Require().Error(err)
	s.Assert().Equal(1, int(testutil.ToFloat64(s.m.RpcErrors.WithLabelValues("photos", "Write", "Unavailable"))))
}

func (s *MetricsInterceptorTestSuite) TestStreamWithoutVolumeTagsEmpty() {
	// Keepalive-style stream: no message carries a volume.
	interceptor := StreamServerMetricsInterceptor(s.m)
	info := &grpc.StreamServerInfo{FullMethod: "/gmountie.SessionService/Keepalive"}
	err := interceptor(nil, &fakeMetricsStream{delivered: true}, info, func(_ any, _ grpc.ServerStream) error {
		return nil
	})
	s.Require().NoError(err)
	c, errC := s.m.RequestDuration.GetMetricWithLabelValues("", "Keepalive")
	s.Require().NoError(errC)
	s.Require().NotNil(c)
}

func TestMetricsInterceptorTestSuite(t *testing.T) {
	suite.Run(t, new(MetricsInterceptorTestSuite))
}
