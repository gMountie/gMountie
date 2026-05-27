package service

import (
	"testing"

	"github.com/stretchr/testify/suite"
)

type SquashResolverSuite struct{ suite.Suite }

func TestSquashResolverSuite(t *testing.T) { suite.Run(t, new(SquashResolverSuite)) }

func (s *SquashResolverSuite) TestResolvesFixedIdentityRegardlessOfPrincipal() {
	r := NewSquashResolver(1000, 1000)
	for _, p := range []string{"alice", "bob", "anonymous"} {
		id, err := r.Resolve(p)
		s.Require().NoError(err)
		s.Equal(uint32(1000), id.Uid)
		s.Equal(uint32(1000), id.Gid)
		s.Equal([]uint32{1000}, id.Gids)
	}
}
