package service

import (
	"context"
	"testing"
	"time"

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

func (s *SquashResolverSuite) TestPopulatesNamesFromRunner() {
	fake := func(_ context.Context, name string, args ...string) ([]byte, error) {
		switch {
		case name == "getent" && len(args) == 2 && args[0] == "passwd" && args[1] == "1000":
			return []byte("appuser:x:1000:1000:App:/:/usr/sbin/nologin\n"), nil
		case name == "getent" && len(args) == 2 && args[0] == "group" && args[1] == "1000":
			return []byte("appgroup:x:1000:\n"), nil
		}
		return nil, nil
	}
	r := newSquashResolverWithRunner(1000, 1000, fake, time.Second)
	id, err := r.Resolve("anyone")
	s.Require().NoError(err)
	s.Equal("appuser", id.UserName)
	s.Equal(map[uint32]string{1000: "appgroup"}, id.GroupNames)
}
