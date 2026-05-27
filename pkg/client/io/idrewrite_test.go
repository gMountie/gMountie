package io

import (
	"testing"

	"github.com/stretchr/testify/suite"
)

type IDRewriteSuite struct{ suite.Suite }

func TestIDRewriteSuite(t *testing.T) { suite.Run(t, new(IDRewriteSuite)) }

func (s *IDRewriteSuite) rw() *IDRewriter {
	return NewIDRewriter(&Identity{Uid: 1001, Gid: 1001, Gids: []uint32{1001, 2000}}, 500, 500)
}

func (s *IDRewriteSuite) TestInboundOwnFilesMapToLocal() {
	uid, gid := s.rw().Inbound(1001, 1001)
	s.Equal(uint32(500), uid)
	s.Equal(uint32(500), gid)
}

func (s *IDRewriteSuite) TestInboundOtherUserMapsToNobody() {
	uid, _ := s.rw().Inbound(1002, 9999)
	s.Equal(uint32(65534), uid)
}

func (s *IDRewriteSuite) TestOutboundLocalMapsToServer() {
	uid, gid := s.rw().Outbound(500, 500)
	s.Equal(uint32(1001), uid)
	s.Equal(uint32(1001), gid)
}

func (s *IDRewriteSuite) TestNilRewriterIsIdentity() {
	var r *IDRewriter
	uid, gid := r.Inbound(1234, 5678)
	s.Equal(uint32(1234), uid)
	s.Equal(uint32(5678), gid)
	uid, gid = r.Outbound(1234, 5678)
	s.Equal(uint32(1234), uid)
	s.Equal(uint32(5678), gid)
}
