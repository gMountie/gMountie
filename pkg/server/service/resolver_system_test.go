package service

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/suite"
)

type SystemResolverSuite struct{ suite.Suite }

func TestSystemResolverSuite(t *testing.T) { suite.Run(t, new(SystemResolverSuite)) }

func (s *SystemResolverSuite) TestResolvesViaIdCommand() {
	fake := func(_ context.Context, name string, args ...string) ([]byte, error) {
		switch args[0] {
		case "-u":
			return []byte("1001\n"), nil
		case "-g":
			return []byte("1001\n"), nil
		case "-G":
			return []byte("1001 2000 2001\n"), nil
		case "-un":
			return []byte("alice\n"), nil
		case "-nG":
			return []byte("alice developers admins\n"), nil
		}
		return nil, ErrPrincipalNotFound
	}
	r := newSystemResolverWithRunner(fake, time.Second)
	id, err := r.Resolve("alice")
	s.Require().NoError(err)
	s.Equal(uint32(1001), id.Uid)
	s.Equal(uint32(1001), id.Gid)
	s.ElementsMatch([]uint32{1001, 2000, 2001}, id.Gids)
}

func (s *SystemResolverSuite) TestUnknownPrincipalFailsClosed() {
	fake := func(context.Context, string, ...string) ([]byte, error) { return nil, ErrPrincipalNotFound }
	r := newSystemResolverWithRunner(fake, time.Second)
	_, err := r.Resolve("mallory")
	s.Require().ErrorIs(err, ErrPrincipalNotFound)
}

func (s *SystemResolverSuite) TestRejectsMalformedPrincipal() {
	r := newSystemResolverWithRunner(func(context.Context, string, ...string) ([]byte, error) {
		s.Fail("runner must not be called for an invalid principal")
		return nil, nil
	}, time.Second)
	_, err := r.Resolve("alice; rm -rf /")
	s.Require().Error(err)
}

func (s *SystemResolverSuite) TestPopulatesNames() {
	fake := func(_ context.Context, name string, args ...string) ([]byte, error) {
		switch args[0] {
		case "-u":
			return []byte("1001\n"), nil
		case "-g":
			return []byte("1001\n"), nil
		case "-G":
			return []byte("1001 2000\n"), nil
		case "-un":
			return []byte("alice\n"), nil
		case "-nG":
			return []byte("alice developers\n"), nil
		}
		return nil, ErrPrincipalNotFound
	}
	r := newSystemResolverWithRunner(fake, time.Second)
	id, err := r.Resolve("alice")
	s.Require().NoError(err)
	s.Equal("alice", id.UserName)
	s.Equal(map[uint32]string{1001: "alice", 2000: "developers"}, id.GroupNames)
}
