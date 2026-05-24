package controller

import (
	"context"
	"gmountie/pkg/proto"
	serverio "gmountie/pkg/server/io"

	"github.com/hanwen/go-fuse/v2/fuse"
)

// toProtoAttr maps a server-side FUSE Attr to the wire Attr, including the
// derived version. nil in -> nil out.
func toProtoAttr(a *fuse.Attr) *proto.Attr {
	if a == nil {
		return nil
	}
	return &proto.Attr{
		Ino: a.Ino, Size: a.Size, Blocks: a.Blocks,
		Atime: a.Atime, Mtime: a.Mtime, Ctime: a.Ctime,
		Atimensec: a.Atimensec, Mtimensec: a.Mtimensec, Ctimensec: a.Ctimensec,
		Mode: a.Mode, Nlink: a.Nlink,
		Owner: &proto.Owner{Uid: a.Uid, Gid: a.Gid},
		Rdev:  a.Rdev, Blksize: a.Blksize, Padding: a.Padding,
		Version: serverio.VersionFromAttr(a),
	}
}

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
