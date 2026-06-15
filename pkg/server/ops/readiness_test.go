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

// stubServing is a ServingChecker whose state the test controls.
type stubServing struct{ serving bool }

func (s stubServing) Serving() bool { return s.serving }

func (s *ReadinessTestSuite) TestExistingPathIsReady() {
	c := MultiPathReadinessChecker{Paths: []string{s.tmp}}
	s.Assert().NoError(c.Ready(context.Background()))
}

func (s *ReadinessTestSuite) TestMissingPathFails() {
	c := MultiPathReadinessChecker{Paths: []string{"/no/such/path/here"}}
	s.Assert().Error(c.Ready(context.Background()))
}

func (s *ReadinessTestSuite) TestNoPathsFails() {
	c := MultiPathReadinessChecker{Paths: nil}
	s.Assert().Error(c.Ready(context.Background()))
}

func (s *ReadinessTestSuite) TestEmptyPathFails() {
	c := MultiPathReadinessChecker{Paths: []string{""}}
	s.Assert().Error(c.Ready(context.Background()))
}

// TestMissingSecondVolumeFails is the core OB-M1 fix: a single missing volume
// among several must fail readiness — the old single-path checker was blind to
// every volume but the first.
func (s *ReadinessTestSuite) TestMissingSecondVolumeFails() {
	c := MultiPathReadinessChecker{Paths: []string{s.tmp, "/no/such/second/volume"}}
	s.Assert().Error(c.Ready(context.Background()))
}

// TestNotServingFails: with all paths present, a draining gRPC server (not
// serving) must still fail readiness.
func (s *ReadinessTestSuite) TestNotServingFails() {
	c := MultiPathReadinessChecker{Paths: []string{s.tmp}, Health: stubServing{serving: false}}
	s.Assert().Error(c.Ready(context.Background()))
}

// TestServingPasses: all paths present and the server serving ⇒ ready.
func (s *ReadinessTestSuite) TestServingPasses() {
	c := MultiPathReadinessChecker{Paths: []string{s.tmp}, Health: stubServing{serving: true}}
	s.Assert().NoError(c.Ready(context.Background()))
}

func TestReadinessTestSuite(t *testing.T) { suite.Run(t, new(ReadinessTestSuite)) }
