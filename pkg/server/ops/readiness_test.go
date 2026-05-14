package ops

import (
	"context"
	"os"
	"testing"

	"github.com/stretchr/testify/suite"
)

type ReadinessTestSuite struct {
	suite.Suite
	tmp string
}

func (s *ReadinessTestSuite) SetupTest() {
	dir, err := os.MkdirTemp("", "gmountie-readiness-*")
	s.Require().NoError(err)
	s.tmp = dir
}

func (s *ReadinessTestSuite) TearDownTest() { _ = os.RemoveAll(s.tmp) }

func (s *ReadinessTestSuite) TestExistingPathIsReady() {
	s.Assert().NoError(PathReadinessChecker{Path: s.tmp}.Ready(context.Background()))
}

func (s *ReadinessTestSuite) TestMissingPathFails() {
	s.Assert().Error(PathReadinessChecker{Path: "/no/such/path/here"}.Ready(context.Background()))
}

func (s *ReadinessTestSuite) TestEmptyPathFails() {
	s.Assert().Error(PathReadinessChecker{Path: ""}.Ready(context.Background()))
}

func TestReadinessTestSuite(t *testing.T) { suite.Run(t, new(ReadinessTestSuite)) }
