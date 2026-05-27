package io

import (
	"github.com/hanwen/go-fuse/v2/fuse"
)

// accessAllowed implements a POSIX access(2) check of `mode` (R_OK=4, W_OK=2,
// X_OK=1; F_OK=0 is existence-only) for identity `id` against file `attr`. It
// is needed because the loopback's access(2) tests the process's real uid
// (root on the server), which would always succeed. Capability bypass
// (dac_read/dac_override) is a later phase and not handled here.
func accessAllowed(attr *fuse.Attr, id *Identity, mode uint32) bool {
	req := mode & 7
	if req == 0 { // F_OK: GetAttr already proved existence/traversal
		return true
	}
	var perm uint32
	switch {
	case id.Uid == attr.Uid:
		perm = (attr.Mode >> 6) & 7
	case inGids(attr.Gid, id.Gids):
		perm = (attr.Mode >> 3) & 7
	default:
		perm = attr.Mode & 7
	}
	return perm&req == req
}

func inGids(gid uint32, gids []uint32) bool {
	for _, g := range gids {
		if g == gid {
			return true
		}
	}
	return false
}
