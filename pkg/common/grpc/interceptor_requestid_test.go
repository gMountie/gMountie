package grpc

import (
	"context"
	"testing"

	"github.com/stretchr/testify/suite"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

type RequestIDInterceptorTestSuite struct{ suite.Suite }

func (s *RequestIDInterceptorTestSuite) TestServerGeneratesIDWhenMissing() {
	interceptor := ServerUnaryRequestID()
	info := &grpc.UnaryServerInfo{FullMethod: "/Test/Method"}
	var seen string
	handler := func(ctx context.Context, req any) (any, error) {
		seen = RequestIDFromContext(ctx)
		return nil, nil
	}
	_, err := interceptor(context.Background(), nil, info, handler)
	s.Require().NoError(err)
	s.Assert().NotEmpty(seen, "server must generate a request id when client supplied none")
}

func (s *RequestIDInterceptorTestSuite) TestServerHonoursClientID() {
	interceptor := ServerUnaryRequestID()
	info := &grpc.UnaryServerInfo{FullMethod: "/Test/Method"}
	var seen string
	handler := func(ctx context.Context, req any) (any, error) {
		seen = RequestIDFromContext(ctx)
		return nil, nil
	}
	md := metadata.Pairs(RequestIDMetadataKey, "from-client")
	ctx := metadata.NewIncomingContext(context.Background(), md)
	_, err := interceptor(ctx, nil, info, handler)
	s.Require().NoError(err)
	s.Assert().Equal("from-client", seen)
}

func (s *RequestIDInterceptorTestSuite) TestClientGeneratesAndPropagates() {
	interceptor := ClientUnaryRequestID()
	var outgoingID string
	invoker := func(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, opts ...grpc.CallOption) error {
		md, ok := metadata.FromOutgoingContext(ctx)
		s.Require().True(ok)
		outgoingID = md.Get(RequestIDMetadataKey)[0]
		return nil
	}
	err := interceptor(context.Background(), "/Test/Method", nil, nil, nil, invoker)
	s.Require().NoError(err)
	s.Assert().NotEmpty(outgoingID)
}

func (s *RequestIDInterceptorTestSuite) TestClientPreservesExistingID() {
	interceptor := ClientUnaryRequestID()
	var outgoingID string
	invoker := func(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, opts ...grpc.CallOption) error {
		md, _ := metadata.FromOutgoingContext(ctx)
		outgoingID = md.Get(RequestIDMetadataKey)[0]
		return nil
	}
	ctx := NewContextWithRequestID(context.Background(), "pre-set")
	err := interceptor(ctx, "/Test/Method", nil, nil, nil, invoker)
	s.Require().NoError(err)
	s.Assert().Equal("pre-set", outgoingID)
}

func TestRequestIDInterceptorTestSuite(t *testing.T) {
	suite.Run(t, new(RequestIDInterceptorTestSuite))
}
