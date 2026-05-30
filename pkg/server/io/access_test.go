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

// --- dac_override capability tests ---

func (s *AccessSuite) TestDacOverride_GrantsReadOnModeZero() {
	// dac_override on a 0000 file → R_OK allowed
	s.True(accessAllowed(attr(1001, 2000, 0o100000 /* S_IFREG | 0000 */), &Identity{Uid: 3000, Gid: 3000, Gids: []uint32{3000}, Caps: []string{CapDacOverride}}, 4))
}

func (s *AccessSuite) TestDacOverride_GrantsWriteOnModeZero() {
	s.True(accessAllowed(attr(1001, 2000, 0o100000), &Identity{Uid: 3000, Gid: 3000, Gids: []uint32{3000}, Caps: []string{CapDacOverride}}, 2))
}

func (s *AccessSuite) TestDacOverride_DeniesExecOnRegularNoExecBit() {
	// 0600 regular file (owner-only rw, no exec bit): dac_override must NOT grant X_OK
	// Use mode 0o100600 so normal permission check denies for non-owner before caps.
	s.False(accessAllowed(attr(1001, 2000, 0o100600), &Identity{Uid: 3000, Gid: 3000, Gids: []uint32{3000}, Caps: []string{CapDacOverride}}, 1))
}

func (s *AccessSuite) TestDacOverride_GrantsExecOnRegularWithExecBit() {
	// 0711 regular file has exec bit: dac_override grants X_OK (other has x bit set,
	// but we use uid=3000 as non-owner to ensure the cap path is exercised via the
	// group/other check — other gets only x, so req for R+X (5) needs cap).
	// Simpler: use mode 0o100100 (only exec for owner) and caller uid=3000 (non-owner):
	// normal perm check would deny R and X for non-owner; dac_override grants R+W+X.
	s.True(accessAllowed(attr(1001, 2000, 0o100100), &Identity{Uid: 3000, Gid: 3000, Gids: []uint32{3000}, Caps: []string{CapDacOverride}}, 1))
}

func (s *AccessSuite) TestDacOverride_GrantsExecOnDirectory() {
	// directory (mode 0000 dir) — dac_override grants X_OK (search)
	s.True(accessAllowed(attr(1001, 2000, 0o040000), &Identity{Uid: 3000, Gid: 3000, Gids: []uint32{3000}, Caps: []string{CapDacOverride}}, 1))
}

// --- dac_read_search capability tests ---

func (s *AccessSuite) TestDacReadSearch_GrantsReadOnModeZeroFile() {
	s.True(accessAllowed(attr(1001, 2000, 0o100000), &Identity{Uid: 3000, Gid: 3000, Gids: []uint32{3000}, Caps: []string{CapDacReadSearch}}, 4))
}

func (s *AccessSuite) TestDacReadSearch_DeniesWriteOnModeZeroFile() {
	// dac_read_search never grants W_OK
	s.False(accessAllowed(attr(1001, 2000, 0o100000), &Identity{Uid: 3000, Gid: 3000, Gids: []uint32{3000}, Caps: []string{CapDacReadSearch}}, 2))
}

func (s *AccessSuite) TestDacReadSearch_DeniesExecOnRegularFile() {
	// dac_read_search does not grant file-execute on a regular file (mode 0600
	// so normal permission check denies exec for non-owner before caps are consulted).
	s.False(accessAllowed(attr(1001, 2000, 0o100600), &Identity{Uid: 3000, Gid: 3000, Gids: []uint32{3000}, Caps: []string{CapDacReadSearch}}, 1))
}

func (s *AccessSuite) TestDacReadSearch_GrantsSearchOnDirectory() {
	// dac_read_search grants X_OK on directories (search permission)
	s.True(accessAllowed(attr(1001, 2000, 0o040000), &Identity{Uid: 3000, Gid: 3000, Gids: []uint32{3000}, Caps: []string{CapDacReadSearch}}, 1))
}
