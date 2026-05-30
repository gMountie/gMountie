package io

import (
	"github.com/hanwen/go-fuse/v2/fuse"
)

// accessAllowed implements a POSIX access(2) check of `mode` (R_OK=4, W_OK=2,
// X_OK=1; F_OK=0 is existence-only) for identity `id` against file `attr`. It
// is needed because the loopback's access(2) tests the process's real uid
// (root on the server), which would always succeed.
//
// Capability bypass follows Linux access(2) semantics:
//   - dac_override: grants R and W unconditionally; grants X only if the file
//     is a directory or has at least one exec bit set (matches kernel behaviour).
//   - dac_read_search: grants R unconditionally; grants X (search) only if the
//     file is a directory. Never grants W.
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
	if perm&req == req {
		return true
	}
	// Capability bypass: compute additional bits the cap grants, then re-check.
	const (
		rOK uint32 = 4
		wOK uint32 = 2
		xOK uint32 = 1
	)
	isDir := attr.Mode&0o170000 == 0o040000 // S_IFDIR
	hasExecBit := attr.Mode&0o111 != 0
	switch {
	case id.HasCap(CapDacOverride):
		// dac_override: R and W always; X only for directories or files with an exec bit.
		capPerm := rOK | wOK
		if isDir || hasExecBit {
			capPerm |= xOK
		}
		return capPerm&req == req
	case id.HasCap(CapDacReadSearch):
		// dac_read_search: R always; X (search) only for directories. Never W.
		capPerm := rOK
		if isDir {
			capPerm |= xOK
		}
		return capPerm&req == req
	}
	return false
}

func inGids(gid uint32, gids []uint32) bool {
	for _, g := range gids {
		if g == gid {
			return true
		}
	}
	return false
}
