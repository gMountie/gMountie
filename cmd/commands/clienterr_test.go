package commands

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/suite"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type ClientErrSuite struct{ suite.Suite }

func TestClientErrSuite(t *testing.T) { suite.Run(t, new(ClientErrSuite)) }

func (s *ClientErrSuite) TestUnreachable() {
	err := remediate(errors.New("connection refused"), "host:9449", "shared")
	s.Contains(err.Error(), "unreachable")
	s.Contains(err.Error(), "host:9449")
}

func (s *ClientErrSuite) TestAuthFailed() {
	err := remediate(status.Error(codes.Unauthenticated, "bad creds"), "host:9449", "shared")
	s.Contains(err.Error(), "authentication failed")
}

func (s *ClientErrSuite) TestVolumeNotFound() {
	err := remediate(status.Error(codes.NotFound, "no volume"), "host:9449", "shared")
	s.Contains(err.Error(), "shared")
	s.Contains(err.Error(), "gmountie ls")
}

func (s *ClientErrSuite) TestNilPassesThrough() {
	s.NoError(remediate(nil, "host:9449", "shared"))
}
