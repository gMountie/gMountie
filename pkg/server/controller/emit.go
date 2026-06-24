package controller

import (
	"context"

	"go.gmountie.dev/gmountie/pkg/proto"
	serverio "go.gmountie.dev/gmountie/pkg/server/io"
	"go.gmountie.dev/gmountie/pkg/server/service"
	"go.gmountie.dev/gmountie/pkg/utils/log"

	"github.com/hanwen/go-fuse/v2/fuse"
	"github.com/hanwen/go-fuse/v2/fuse/pathfs"
	"go.uber.org/zap"
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
//
// PERF: every helper gates on bus.HasSubscribers(volume). With nobody
// subscribed both the version-seeding stat and the emit are pure waste —
// every Write RPC otherwise paid an openat2+fstatat+close (under server
// credentials via the raw filesystem; the bound-fs paths in mutateEmit/
// renameEmit additionally paid a credential switch) for an event nobody
// received. Stats that also serve the
// RPC reply (statAttrsAndEmit, SetAttr/Create inlines) are NOT gated; only
// the emit is. The gate deliberately races with Subscribe: a client attaching
// between the check and the mutation misses that one event, which is harmless
// — a new subscriber starts in the Unverified state and revalidates every
// cached entry via GetAttrIfChanged before trusting it
// (docs/design/caching-and-consistency.md §4.2).

// mutateEmit runs op and, when it returns fuse.OK, emits the KindMutated
// event for (volume, path) seeded with a fresh version from versionAfter.
func (r *RpcServerImpl) mutateEmit(ctx context.Context, fs pathfs.FileSystem, volume, path string, caller *proto.Caller, op func() fuse.Status) fuse.Status {
	s := op()
	if s == fuse.OK && r.bus.HasSubscribers(volume) {
		r.bus.Emit(volume, path, versionAfter(ctx, fs, path, caller), serverio.KindMutated, sessionIDFromContext(ctx))
	}
	return s
}

// deleteEmit runs op and, when it returns fuse.OK, emits the KindDeleted
// event for (volume, path). Deletions carry version 0 — there is nothing
// left to stat.
func (r *RpcServerImpl) deleteEmit(ctx context.Context, volume, path string, op func() fuse.Status) fuse.Status {
	s := op()
	if s == fuse.OK && r.bus.HasSubscribers(volume) {
		r.bus.Emit(volume, path, 0, serverio.KindDeleted, sessionIDFromContext(ctx))
	}
	return s
}

// emitMutatedAttr emits the KindMutated event for (volume, path) seeded from
// an attr the handler already holds — SetAttr stats the path for its reply
// attrs anyway, so re-statting via versionAfter would waste a syscall. A nil
// attr yields version 0; clients fall back to GetAttrIfChanged. Mirrors
// RpcFileServerImpl.emitMutatedAttr in file.go.
func (r *RpcServerImpl) emitMutatedAttr(ctx context.Context, volume, path string, attr *fuse.Attr) {
	if !r.bus.HasSubscribers(volume) {
		return
	}
	r.bus.Emit(volume, path, serverio.VersionFromAttr(attr), serverio.KindMutated, sessionIDFromContext(ctx))
}

// statAttrsAndEmit performs the ONE trailing stat shared by create-style
// handlers (Mkdir, Symlink) that need both reply attrs and an event seed from
// the same stat. On a successful stat it emits the mutation event seeded with
// the fresh attr and returns the proto attrs; on a stat failure it emits
// version 0 (clients revalidate via GetAttrIfChanged) and returns nil.
//
// SetAttr and Create are siblings that handle the same pattern but have
// different gating (mutated guard in SetAttr, fd-receiver in Create) —
// they inline the block directly rather than routing through this helper.
func (r *RpcServerImpl) statAttrsAndEmit(ctx context.Context, fs pathfs.FileSystem, volume, path string, id *service.Identity, fctx *fuse.Context) *proto.Attr {
	attr, gst := fs.GetAttr(path, fctx)
	if gst.Ok() {
		r.emitMutatedAttr(ctx, volume, path, attr)
		return toProtoAttr(attr, id)
	}
	// The mutation succeeded but the trailing stat failed: we return nil attrs
	// (partial-success contract) and the client falls back to a fresh Stat. This
	// is invisible without a log — and when the fallback Stat ALSO fails the user
	// sees an EIO on a just-created path. Log loudly so the errno is diagnosable.
	log.Log.Warn("post-op stat failed after successful mutation; returning nil attrs (client will fall back to Stat)",
		zap.String("path", path),
		zap.String("volume", volume),
		zap.String("fuse_status", gst.String()),
		zap.Int("errno", int(gst)),
		zap.Uint32("caller_uid", fctx.Uid),
		zap.Uint32("caller_gid", fctx.Gid),
	)
	r.emitMutatedAttr(ctx, volume, path, nil)
	return nil
}

// renameEmit runs op and, when it returns fuse.OK, emits the rename event
// for oldName→newName with the new path's fresh version.
func (r *RpcServerImpl) renameEmit(ctx context.Context, fs pathfs.FileSystem, volume, oldName, newName string, caller *proto.Caller, op func() fuse.Status) fuse.Status {
	s := op()
	if s == fuse.OK && r.bus.HasSubscribers(volume) {
		r.bus.EmitRename(volume, oldName, newName, versionAfter(ctx, fs, newName, caller), sessionIDFromContext(ctx))
	}
	return s
}
