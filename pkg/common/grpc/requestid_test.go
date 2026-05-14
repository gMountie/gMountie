package grpc

import (
	"context"
	"testing"

	"github.com/stretchr/testify/suite"
)

type RequestIDTestSuite struct{ suite.Suite }

func (s *RequestIDTestSuite) TestFromContextEmptyByDefault() {
	s.Assert().Empty(RequestIDFromContext(context.Background()))
}

func (s *RequestIDTestSuite) TestNewContextRoundTrip() {
	ctx := NewContextWithRequestID(context.Background(), "abc-123")
	s.Assert().Equal("abc-123", RequestIDFromContext(ctx))
}

func TestRequestIDTestSuite(t *testing.T) { suite.Run(t, new(RequestIDTestSuite)) }
