package principal

import (
	"context"
	"testing"

	"github.com/stretchr/testify/suite"
)

type PrincipalSuite struct{ suite.Suite }

func TestPrincipalSuite(t *testing.T) { suite.Run(t, new(PrincipalSuite)) }

func (s *PrincipalSuite) TestRoundTrip() {
	ctx := WithPrincipal(context.Background(), "alice")
	p, ok := FromContext(ctx)
	s.True(ok)
	s.Equal("alice", p)
}

func (s *PrincipalSuite) TestMissing() {
	_, ok := FromContext(context.Background())
	s.False(ok)
}
