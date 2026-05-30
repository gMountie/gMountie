package controller

import (
	"context"

	"go.gmountie.dev/gmountie/pkg/proto"
	serverio "go.gmountie.dev/gmountie/pkg/server/io"
	"go.gmountie.dev/gmountie/pkg/server/service"

	"github.com/hanwen/go-fuse/v2/fuse"
	"github.com/hanwen/go-fuse/v2/fuse/pathfs"
)

// toProtoAttr maps a server-side FUSE Attr to the wire Attr, including the
// derived version. nil in -> nil out.
//
// id is the caller's resolved server-side identity, used to fill the hybrid
// Owner.user_name / Owner.group_name fields per §3.6:
//   - user_name is set IFF the file is owned by the caller (a.Uid == id.Uid);
//     we never leak other users' names through stat.
//   - group_name is set IFF the file's gid has a name in id.GroupNames AND the
//     caller is a member of that group (a.Gid is in id.Gids).
//
// A nil id (e.g. the call has no caller, StatFs nil-caller path) is safe:
// no names are filled and the wire shape is identical to the pre-1b-2 reply.
func toProtoAttr(a *fuse.Attr, id *service.Identity) *proto.Attr {
	if a == nil {
		return nil
	}
	owner := &proto.Owner{Uid: a.Uid, Gid: a.Gid}
	if id != nil {
		if a.Uid == id.Uid {
			owner.UserName = id.UserName
		}
		if name, ok := id.GroupNames[a.Gid]; ok && groupMember(id.Gids, a.Gid) {
			owner.GroupName = name
		}
	}
	return &proto.Attr{
		Ino: a.Ino, Size: a.Size, Blocks: a.Blocks,
		Atime: a.Atime, Mtime: a.Mtime, Ctime: a.Ctime,
		Atimensec: a.Atimensec, Mtimensec: a.Mtimensec, Ctimensec: a.Ctimensec,
		Mode: a.Mode, Nlink: a.Nlink,
		Owner: owner,
		Rdev:  a.Rdev, Blksize: a.Blksize, Padding: a.Padding,
		Version: serverio.VersionFromAttr(a),
	}
}

// groupMember reports whether gid appears in gids. Identity.Gids is typically
// short (primary + a handful of supplementary groups), so a linear scan is
// cheaper than building a set.
func groupMember(gids []uint32, gid uint32) bool {
	for _, g := range gids {
		if g == gid {
			return true
		}
	}
	return false
}

// versionAfter re-Stats path on fs and returns the freshness token for use in
// an Emit call. Returns 0 if GetAttr fails — the event still fires; the client
// falls back to GetAttrIfChanged.
func versionAfter(ctx context.Context, fs pathfs.FileSystem, path string, caller *proto.Caller) uint64 {
	attr, st := fs.GetAttr(path, createContext(ctx, caller))
	if !st.Ok() || attr == nil {
		return 0
	}
	return serverio.VersionFromAttr(attr)
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
