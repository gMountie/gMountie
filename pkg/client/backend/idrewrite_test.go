package backend

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

func (s *IDRewriteSuite) TestInboundSharedGroupKeepsGid() {
	// Identity gid=1001 (primary) + 2000 (developers). File owned uid 1001
	// gid 2000 — caller is in 2000, so gid 2000 must pass through unchanged
	// (not nobody), while uid 1001 still rewrites to local (own file).
	uid, gid := s.rw().Inbound(1001, 2000)
	s.Equal(uint32(500), uid)
	s.Equal(uint32(2000), gid)
}

func (s *IDRewriteSuite) TestInboundUnrelatedGroupStillNobody() {
	// Sanity: a gid NOT in identity.Gids still maps to nobody.
	_, gid := s.rw().Inbound(1001, 9999)
	s.Equal(uint32(65534), gid)
}
