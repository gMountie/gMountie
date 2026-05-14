package controller

import (
	"context"
	"testing"

	"gmountie/pkg/proto"

	"github.com/stretchr/testify/suite"
)

type VersionControllerTestSuite struct {
	suite.Suite
	c *VersionController
}

func (s *VersionControllerTestSuite) SetupTest() { s.c = NewVersionController() }

func (s *VersionControllerTestSuite) TestGetReturnsBuildInfo() {
	reply, err := s.c.Get(context.Background(), &proto.VersionRequest{})
	s.Require().NoError(err)
	s.Assert().NotEmpty(reply.Version)
	s.Assert().NotEmpty(reply.Commit)
	s.Assert().NotEmpty(reply.Date)
}

func TestVersionControllerTestSuite(t *testing.T) {
	suite.Run(t, new(VersionControllerTestSuite))
}
