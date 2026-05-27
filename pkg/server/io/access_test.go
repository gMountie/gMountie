package io

import (
	"testing"

	"github.com/hanwen/go-fuse/v2/fuse"
	"github.com/stretchr/testify/suite"
)

type AccessSuite struct{ suite.Suite }

func TestAccessSuite(t *testing.T) { suite.Run(t, new(AccessSuite)) }

func attr(uid, gid uint32, mode uint32) *fuse.Attr {
	a := &fuse.Attr{Mode: mode}
	a.Uid = uid
	a.Gid = gid
	return a
}

func (s *AccessSuite) TestOwnerReadAllowed() {
	// file 0640 owned 1001:2000; caller is owner 1001 -> owner bits rw-
	s.True(accessAllowed(attr(1001, 2000, 0o640), &Identity{Uid: 1001, Gid: 1001, Gids: []uint32{1001}}, 4))
}

func (s *AccessSuite) TestOwnerWriteAllowed() {
	s.True(accessAllowed(attr(1001, 2000, 0o640), &Identity{Uid: 1001, Gid: 1001, Gids: []uint32{1001}}, 2))
}

func (s *AccessSuite) TestGroupReadViaSupplementary() {
	// file 0640 owned 1001:2000; caller 1002 is in supplementary group 2000 -> group r--
	s.True(accessAllowed(attr(1001, 2000, 0o640), &Identity{Uid: 1002, Gid: 1002, Gids: []uint32{1002, 2000}}, 4))
}

func (s *AccessSuite) TestGroupWriteDeniedWhenGroupLacksW() {
	// 0640: group is r-- only
	s.False(accessAllowed(attr(1001, 2000, 0o640), &Identity{Uid: 1002, Gid: 1002, Gids: []uint32{1002, 2000}}, 2))
}

func (s *AccessSuite) TestOtherDenied() {
	// 0640: other has no bits; caller is neither owner nor in group 2000
	s.False(accessAllowed(attr(1001, 2000, 0o640), &Identity{Uid: 3000, Gid: 3000, Gids: []uint32{3000}}, 4))
}

func (s *AccessSuite) TestExistenceOnlyAllowed() {
	s.True(accessAllowed(attr(1001, 2000, 0o000), &Identity{Uid: 3000, Gid: 3000, Gids: []uint32{3000}}, 0))
}
