package service

import (
	"testing"

	"gmountie/pkg/server/config"

	"github.com/stretchr/testify/suite"
)

type StaticResolverSuite struct{ suite.Suite }

func TestStaticResolverSuite(t *testing.T) { suite.Run(t, new(StaticResolverSuite)) }

func (s *StaticResolverSuite) mapping() config.MappingConfig {
	return config.MappingConfig{
		Mode:   config.MappingModeStatic,
		Users:  map[string]config.StaticUser{"alice": {Uid: 1001, Gid: 1001, Groups: []string{"developers"}}},
		Groups: map[string]uint32{"developers": 2000},
	}
}

func (s *StaticResolverSuite) TestResolvesUserWithSupplementaryGroups() {
	r := NewStaticResolver(s.mapping())
	id, err := r.Resolve("alice")
	s.Require().NoError(err)
	s.Equal(uint32(1001), id.Uid)
	s.Equal(uint32(1001), id.Gid)
	s.ElementsMatch([]uint32{1001, 2000}, id.Gids)
}

func (s *StaticResolverSuite) TestUnknownPrincipalFailsClosed() {
	r := NewStaticResolver(s.mapping())
	_, err := r.Resolve("mallory")
	s.Require().ErrorIs(err, ErrPrincipalNotFound)
}

func (s *StaticResolverSuite) TestUnknownGroupIsSkipped() {
	m := s.mapping()
	m.Users["alice"] = config.StaticUser{Uid: 1001, Gid: 1001, Groups: []string{"developers", "ghosts"}}
	r := NewStaticResolver(m)
	id, err := r.Resolve("alice")
	s.Require().NoError(err)
	s.ElementsMatch([]uint32{1001, 2000}, id.Gids)
}
