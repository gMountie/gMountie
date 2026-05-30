package controller

import (
	"context"
	"testing"

	"go.gmountie.dev/gmountie/pkg/proto"

	"github.com/stretchr/testify/suite"
)

type VersionControllerTestSuite struct {
	suite.Suite
	c *VersionController
}

func (s *VersionControllerTestSuite) SetupTest() {
	s.c = NewVersionController(1 << 20)
}

func (s *VersionControllerTestSuite) TestGetReturnsBuildInfo() {
	reply, err := s.c.Get(context.Background(), &proto.VersionRequest{})
	s.Require().NoError(err)
	s.Assert().NotEmpty(reply.Version)
	s.Assert().NotEmpty(reply.Commit)
	s.Assert().NotEmpty(reply.Date)
}

func (s *VersionControllerTestSuite) TestGetReturnsFrameSize() {
	reply, err := s.c.Get(context.Background(), &proto.VersionRequest{})
	s.Require().NoError(err)
	s.Assert().Equal(int32(1<<20), reply.FrameSizeBytes)
}

func (s *VersionControllerTestSuite) TestGetReturnsZeroFrameSizeWhenUnset() {
	c := NewVersionController(0)
	reply, err := c.Get(context.Background(), &proto.VersionRequest{})
	s.Require().NoError(err)
	s.Assert().Equal(int32(0), reply.FrameSizeBytes)
}

func TestVersionControllerTestSuite(t *testing.T) {
	suite.Run(t, new(VersionControllerTestSuite))
}
