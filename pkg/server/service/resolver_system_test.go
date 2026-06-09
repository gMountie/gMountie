package service

import (
	"context"
	"testing"
	"time"

	"go.gmountie.dev/gmountie/pkg/server/config"

	"github.com/stretchr/testify/suite"
)

type SystemResolverSuite struct{ suite.Suite }

func TestSystemResolverSuite(t *testing.T) { suite.Run(t, new(SystemResolverSuite)) }

// fakeID returns a runner serving a canned single-invocation `id <user>` line.
// It also asserts the resolver issues exactly ONE call with the bare
// principal as the only argument (the CQ-11 contract: no -G/-nG index zipping
// across separate process snapshots).
func (s *SystemResolverSuite) fakeID(output string) commandRunner {
	calls := 0
	return func(_ context.Context, name string, args ...string) ([]byte, error) {
		calls++
		s.Require().Equal("id", name)
		s.Require().Len(args, 1, "resolver must issue a single `id <user>` call, got args %v", args)
		s.Require().LessOrEqual(calls, 1, "resolver must not fork more than one subprocess per resolve")
		return []byte(output), nil
	}
}

func (s *SystemResolverSuite) TestResolvesViaSingleIdCommand() {
	r := newSystemResolverWithRunner(
		s.fakeID("uid=1001(alice) gid=1001(alice) groups=1001(alice),2000(developers),2001(admins)\n"),
		time.Second, config.MappingConfig{})
	id, err := r.Resolve("alice")
	s.Require().NoError(err)
	s.Equal(uint32(1001), id.Uid)
	s.Equal(uint32(1001), id.Gid)
	s.ElementsMatch([]uint32{1001, 2000, 2001}, id.Gids)
}

func (s *SystemResolverSuite) TestUnknownPrincipalFailsClosed() {
	fake := func(context.Context, string, ...string) ([]byte, error) { return nil, ErrPrincipalNotFound }
	r := newSystemResolverWithRunner(fake, time.Second, config.MappingConfig{})
	_, err := r.Resolve("mallory")
	s.Require().ErrorIs(err, ErrPrincipalNotFound)
}

func (s *SystemResolverSuite) TestRejectsMalformedPrincipal() {
	r := newSystemResolverWithRunner(func(context.Context, string, ...string) ([]byte, error) {
		s.Fail("runner must not be called for an invalid principal")
		return nil, nil
	}, time.Second, config.MappingConfig{})
	_, err := r.Resolve("alice; rm -rf /")
	s.Require().Error(err)
}

func (s *SystemResolverSuite) TestPopulatesNames() {
	r := newSystemResolverWithRunner(
		s.fakeID("uid=1001(alice) gid=1001(alice) groups=1001(alice),2000(developers)\n"),
		time.Second, config.MappingConfig{})
	id, err := r.Resolve("alice")
	s.Require().NoError(err)
	s.Equal("alice", id.UserName)
	s.Equal(map[uint32]string{1001: "alice", 2000: "developers"}, id.GroupNames)
}

// TestNamesStayPairedWithGids pins the skew bug the rewrite fixes: every
// name comes from the SAME token as its gid, so a nameless gid in the middle
// of the list cannot shift the remaining names onto wrong gids (the old
// index-zipping of `id -G` × `id -nG` did exactly that).
func (s *SystemResolverSuite) TestNamesStayPairedWithGids() {
	r := newSystemResolverWithRunner(
		s.fakeID("uid=1001(alice) gid=1001(alice) groups=1001(alice),3000,2000(developers)\n"),
		time.Second, config.MappingConfig{})
	id, err := r.Resolve("alice")
	s.Require().NoError(err)
	s.ElementsMatch([]uint32{1001, 3000, 2000}, id.Gids)
	s.Equal(map[uint32]string{1001: "alice", 2000: "developers"}, id.GroupNames,
		"the nameless gid 3000 must not shift 'developers' onto the wrong gid")
}

func (s *SystemResolverSuite) TestIgnoresSELinuxContextField() {
	r := newSystemResolverWithRunner(
		s.fakeID("uid=1001(alice) gid=1001(alice) groups=1001(alice) context=unconfined_u:unconfined_r:unconfined_t:s0\n"),
		time.Second, config.MappingConfig{})
	id, err := r.Resolve("alice")
	s.Require().NoError(err)
	s.Equal(uint32(1001), id.Uid)
}

func (s *SystemResolverSuite) TestMalformedOutputErrors() {
	r := newSystemResolverWithRunner(
		s.fakeID("something unexpected\n"),
		time.Second, config.MappingConfig{})
	_, err := r.Resolve("alice")
	s.Require().Error(err)
	s.Contains(err.Error(), "missing uid/gid")
}

func (s *SystemResolverSuite) TestAdminGroupsGrantsCaps() {
	cfg := config.MappingConfig{
		AdminGroups: map[string][]string{
			"dac_override": {"wheel"},
		},
	}
	r := newSystemResolverWithRunner(
		s.fakeID("uid=1001(alice) gid=1001(alice) groups=1001(alice),100(wheel)\n"),
		time.Second, cfg)
	id, err := r.Resolve("alice")
	s.Require().NoError(err)
	s.Contains(id.Caps, "dac_override")
}

func (s *SystemResolverSuite) TestNoMembershipNoCaps() {
	cfg := config.MappingConfig{
		AdminGroups: map[string][]string{
			"dac_override": {"wheel"},
		},
	}
	r := newSystemResolverWithRunner(
		s.fakeID("uid=1002(bob) gid=1002(bob) groups=1002(bob),1000(users)\n"),
		time.Second, cfg)
	id, err := r.Resolve("bob")
	s.Require().NoError(err)
	s.Empty(id.Caps)
}
