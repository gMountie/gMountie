package controller

import (
	"context"
	"gmountie/pkg/proto"

	"github.com/hanwen/go-fuse/v2/fuse"
)

// createContext creates a new fuse.Context from the given context.Context.
// A nil caller (or nil caller.Owner) is treated as uid/gid/pid = 0 — the
// downstream filesystem layer is responsible for rejecting requests that
// require a real caller identity (e.g. the assume-user middleware will fail
// authentication checks on uid 0 if the server isn't running as root).
func createContext(ctx context.Context, caller *proto.Caller) *fuse.Context {
	var uid, gid, pid uint32
	if caller != nil {
		pid = caller.Pid
		if caller.Owner != nil {
			uid = caller.Owner.Uid
			gid = caller.Owner.Gid
		}
	}
	return &fuse.Context{
		Caller: fuse.Caller{
			Owner: fuse.Owner{Uid: uid, Gid: gid},
			Pid:   pid,
		},
		Cancel: ctx.Done(),
	}
}
