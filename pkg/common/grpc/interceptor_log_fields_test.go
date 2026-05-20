package grpc

import (
	"context"
	"testing"

	"github.com/grpc-ecosystem/go-grpc-middleware/v2/interceptors/logging"
	"github.com/stretchr/testify/suite"
	"google.golang.org/grpc"
)

type withSessionAndVolume struct{}

func (withSessionAndVolume) GetSessionId() string { return "sess-X" }
func (withSessionAndVolume) GetVolume() string    { return "vol-Y" }

type onlyVolume struct{}

func (onlyVolume) GetVolume() string { return "vol-only" }

type LogContextInterceptorTestSuite struct{ suite.Suite }

// fieldsFromCtx is a test helper that extracts every field
// logging.ExtractFields can see on ctx, as a flat key/value map.
func fieldsFromCtx(ctx context.Context) map[string]any {
	out := map[string]any{}
	flds := logging.ExtractFields(ctx)
	// logging.Fields is alternating key/value.
	for i := 0; i+1 < len(flds); i += 2 {
		if k, ok := flds[i].(string); ok {
			out[k] = flds[i+1]
		}
	}
	return out
}

func (s *LogContextInterceptorTestSuite) TestInjectsSessionIDAndVolume() {
	interceptor := ServerUnaryLogContext()
	info := &grpc.UnaryServerInfo{FullMethod: "/x/y"}
	var seen map[string]any
	handler := func(ctx context.Context, req any) (any, error) {
		seen = fieldsFromCtx(ctx)
		return struct{}{}, nil // any non-nil response satisfies the interceptor contract
	}
	_, err := interceptor(context.Background(), withSessionAndVolume{}, info, handler)
	s.Require().NoError(err)
	s.Assert().Equal("sess-X", seen["session_id"])
	s.Assert().Equal("vol-Y", seen["volume"])
}

func (s *LogContextInterceptorTestSuite) TestInjectsOnlyPresentGetters() {
	interceptor := ServerUnaryLogContext()
	info := &grpc.UnaryServerInfo{FullMethod: "/x/y"}
	var seen map[string]any
	handler := func(ctx context.Context, req any) (any, error) {
		seen = fieldsFromCtx(ctx)
		return struct{}{}, nil // any non-nil response satisfies the interceptor contract
	}
	_, err := interceptor(context.Background(), onlyVolume{}, info, handler)
	s.Require().NoError(err)
	_, hasSession := seen["session_id"]
	s.Assert().False(hasSession, "session_id must not be injected when GetSessionId is absent")
	s.Assert().Equal("vol-only", seen["volume"])
}

func TestLogContextInterceptorTestSuite(t *testing.T) {
	suite.Run(t, new(LogContextInterceptorTestSuite))
}
