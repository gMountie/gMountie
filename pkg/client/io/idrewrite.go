package io

// nobodyID is the conventional unprivileged display id for files the mounting
// user neither owns nor shares a (primary) group with.
const nobodyID uint32 = 65534

// Identity mirrors the server-resolved identity the client learns via WhoAmI.
type Identity struct {
	Uid  uint32
	Gid  uint32
	Gids []uint32
}

// IDRewriter maps between the server identity namespace and the local mounting
// user's namespace for display. A nil *IDRewriter is the identity transform
// (raw_ids mounts, or when WhoAmI returned nothing).
type IDRewriter struct {
	id       *Identity
	localUID uint32
	localGID uint32
}

// NewIDRewriter returns a rewriter, or nil if id is nil (no rewriting).
func NewIDRewriter(id *Identity, localUID, localGID uint32) *IDRewriter {
	if id == nil {
		return nil
	}
	return &IDRewriter{id: id, localUID: localUID, localGID: localGID}
}

// Inbound rewrites server uid/gid → local display uid/gid (server→client).
func (r *IDRewriter) Inbound(uid, gid uint32) (uint32, uint32) {
	if r == nil {
		return uid, gid
	}
	outUID, outGID := nobodyID, nobodyID
	if uid == r.id.Uid {
		outUID = r.localUID
	}
	if gid == r.id.Gid {
		outGID = r.localGID
	}
	return outUID, outGID
}

// Outbound rewrites local uid/gid → server uid/gid (client→server, for chown).
func (r *IDRewriter) Outbound(uid, gid uint32) (uint32, uint32) {
	if r == nil {
		return uid, gid
	}
	if uid == r.localUID {
		uid = r.id.Uid
	}
	if gid == r.localGID {
		gid = r.id.Gid
	}
	return uid, gid
}
