package service

import (
	"testing"

	"go.gmountie.dev/gmountie/pkg/server/config"

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

func (s *StaticResolverSuite) TestPopulatesNames() {
	r := NewStaticResolver(s.mapping())
	id, err := r.Resolve("alice")
	s.Require().NoError(err)
	s.Equal("alice", id.UserName)
	s.Equal(map[uint32]string{2000: "developers"}, id.GroupNames)
}

// TestPlumbsCapsFromConfig: a static-mode user's caps come straight from the
// config table and reach the resolved Identity unmodified. The Phase 3
// identity-bound FS (T5) consumes these via io.Identity.HasCap; this test
// asserts the plumbing into service.Identity remains intact.
func (s *StaticResolverSuite) TestPlumbsCapsFromConfig() {
	m := s.mapping()
	m.Users["bob"] = config.StaticUser{
		Uid: 1002, Gid: 1002,
		Caps: []string{"dac_read_search"},
	}
	m.Users["root-admin"] = config.StaticUser{
		Uid: 1003, Gid: 1003,
		Caps: []string{"dac_override"},
	}
	r := NewStaticResolver(m)

	id, err := r.Resolve("bob")
	s.Require().NoError(err)
	s.Equal([]string{"dac_read_search"}, id.Caps)

	id, err = r.Resolve("root-admin")
	s.Require().NoError(err)
	s.Equal([]string{"dac_override"}, id.Caps)

	// Original alice has no caps configured — should round-trip as nil/empty.
	id, err = r.Resolve("alice")
	s.Require().NoError(err)
	s.Empty(id.Caps)
}
