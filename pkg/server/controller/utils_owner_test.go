package controller

import (
	"testing"

	"gmountie/pkg/server/service"

	"github.com/hanwen/go-fuse/v2/fuse"
	"github.com/stretchr/testify/suite"
)

type ToProtoAttrSuite struct{ suite.Suite }

func TestToProtoAttrSuite(t *testing.T) { suite.Run(t, new(ToProtoAttrSuite)) }

func mkAttr(uid, gid uint32) *fuse.Attr {
	a := &fuse.Attr{Mode: 0o644}
	a.Uid = uid
	a.Gid = gid
	return a
}

func mkID() *service.Identity {
	return &service.Identity{
		Uid: 1001, Gid: 1001, Gids: []uint32{1001, 2000},
		UserName:   "alice",
		GroupNames: map[uint32]string{1001: "alice", 2000: "developers"},
	}
}

func (s *ToProtoAttrSuite) TestFillsUserNameAndGroupForOwnFile() {
	p := toProtoAttr(mkAttr(1001, 1001), mkID())
	s.Equal("alice", p.Owner.UserName)
	s.Equal("alice", p.Owner.GroupName)
}

func (s *ToProtoAttrSuite) TestUserNameHiddenForOthersGroupShownIfMember() {
	p := toProtoAttr(mkAttr(9999, 2000), mkID())
	s.Equal("", p.Owner.UserName) // never leak other users' names
	s.Equal("developers", p.Owner.GroupName)
}

func (s *ToProtoAttrSuite) TestGroupHiddenWhenNotMember() {
	p := toProtoAttr(mkAttr(9999, 3000), mkID())
	s.Equal("", p.Owner.UserName)
	s.Equal("", p.Owner.GroupName)
}

func (s *ToProtoAttrSuite) TestNilIdentityYieldsNoNames() {
	p := toProtoAttr(mkAttr(1001, 1001), nil)
	s.Equal("", p.Owner.UserName)
	s.Equal("", p.Owner.GroupName)
}
