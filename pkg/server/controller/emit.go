package controller

import (
	"context"

	"go.gmountie.dev/gmountie/pkg/proto"
	serverio "go.gmountie.dev/gmountie/pkg/server/io"

	"github.com/hanwen/go-fuse/v2/fuse"
	"github.com/hanwen/go-fuse/v2/fuse/pathfs"
)

// Cache-invalidation emission helpers.
//
// Client cache coherence depends on EVERY mutating RPC emitting an event on
// the bus — a handler that forgets the Emit compiles fine and silently serves
// stale data. To make that structurally hard, mutating handlers do not call
// r.bus.Emit by hand; they wrap the filesystem call in one of the helpers
// below, which couple "the mutation succeeded" to "the event fired":
//
//   - mutateEmit:       path mutation; event seeded with a fresh version stat.
//   - deleteEmit:       removal; event carries version 0 + KindDeleted.
//   - renameEmit:       rename; emits the old→new pair with the new path's version.
//   - emitMutatedAttr:  path mutation where the handler already holds a
//     fresh post-op attr (or knows the mutation happened but has no attr).
//
// POLICY: a new mutating RPC must route its filesystem call through one of
// these (or, for fd-based ops in file.go, through emitMutatedFd). If none
// fits, extend this file rather than inlining a bus.Emit in the handler.

// mutateEmit runs op and, when it returns fuse.OK, emits the KindMutated
// event for (volume, path) seeded with a fresh version from versionAfter.
func (r *RpcServerImpl) mutateEmit(ctx context.Context, fs pathfs.FileSystem, volume, path string, caller *proto.Caller, op func() fuse.Status) fuse.Status {
	s := op()
	if s == fuse.OK {
		r.bus.Emit(volume, path, versionAfter(ctx, fs, path, caller), serverio.KindMutated)
	}
	return s
}

// deleteEmit runs op and, when it returns fuse.OK, emits the KindDeleted
// event for (volume, path). Deletions carry version 0 — there is nothing
// left to stat.
func (r *RpcServerImpl) deleteEmit(volume, path string, op func() fuse.Status) fuse.Status {
	s := op()
	if s == fuse.OK {
		r.bus.Emit(volume, path, 0, serverio.KindDeleted)
	}
	return s
}

// emitMutatedAttr emits the KindMutated event for (volume, path) seeded from
// an attr the handler already holds — SetAttr stats the path for its reply
// attrs anyway, so re-statting via versionAfter would waste a syscall. A nil
// attr yields version 0; clients fall back to GetAttrIfChanged. Mirrors
// RpcFileServerImpl.emitMutatedAttr in file.go.
func (r *RpcServerImpl) emitMutatedAttr(volume, path string, attr *fuse.Attr) {
	r.bus.Emit(volume, path, serverio.VersionFromAttr(attr), serverio.KindMutated)
}

// renameEmit runs op and, when it returns fuse.OK, emits the rename event
// for oldName→newName with the new path's fresh version.
func (r *RpcServerImpl) renameEmit(ctx context.Context, fs pathfs.FileSystem, volume, oldName, newName string, caller *proto.Caller, op func() fuse.Status) fuse.Status {
	s := op()
	if s == fuse.OK {
		r.bus.EmitRename(volume, oldName, newName, versionAfter(ctx, fs, newName, caller))
	}
	return s
}
