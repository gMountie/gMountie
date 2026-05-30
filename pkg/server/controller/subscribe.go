package controller

import (
	"syscall"

	"gmountie/pkg/proto"
	serverio "gmountie/pkg/server/io"

	"github.com/hanwen/go-fuse/v2/fuse"
	"github.com/hanwen/go-fuse/v2/fuse/pathfs"
)

// Subscribe is the server-streaming RPC for cache-invalidation push.
// One stream per (client connection, volume); the controller subscribes
// to the local EventBus, filters each event by the principal's per-path
// access, and forwards survivors to the stream until ctx cancels or send
// fails. The per-path filter closes the spec §11 metadata-leak gap: a
// subscriber must not learn the existence of paths it cannot stat.
func (r *RpcServerImpl) Subscribe(request *proto.SubscribeRequest, stream proto.RpcFs_SubscribeServer) error {
	// Bind the principal ONCE for the lifetime of the stream. The handler
	// is a long-lived goroutine without per-op ctx; identity-refresh-on-
	// resume is deferred (spec §11). BindIdentity also validates the volume.
	ctx := stream.Context()
	boundFS, _, err := r.fsService.BindIdentity(ctx, request.Volume, request.Caller)
	if err != nil {
		return err
	}
	if r.metrics != nil {
		r.metrics.SubscribeSubscribersInc(request.Volume)
		defer r.metrics.SubscribeSubscribersDec(request.Volume)
	}
	events, cancel := r.bus.Subscribe(request.Volume)
	defer cancel()
	fctx := createContext(ctx, request.Caller)
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case ev, ok := <-events:
			if !ok {
				// Bus closed our channel due to backpressure overflow.
				return nil
			}
			if !accessibleToPrincipal(boundFS, ev, fctx) {
				continue
			}
			if err := stream.Send(toPBEvent(ev)); err != nil {
				return err
			}
		}
	}
}

// accessibleToPrincipal reports whether the bound principal can see the
// event's path(s). Heartbeats carry no path and always pass. Rename
// requires access to BOTH old and new paths — otherwise we'd leak that a
// rename happened between two paths the subscriber can't see, or expose
// a partially-visible rename whose semantics they couldn't reconstruct.
// F_OK (existence) is the right granularity: the kernel checks search
// (X) on every component, which matches "could this principal have
// observed this path through a normal stat".
func accessibleToPrincipal(boundFS pathfs.FileSystem, ev serverio.Event, fctx *fuse.Context) bool {
	if ev.Kind == serverio.KindHeartbeat {
		return true
	}
	if !boundFS.Access(ev.Path, syscall.F_OK, fctx).Ok() {
		return false
	}
	if ev.Kind == serverio.KindRenamed && ev.NewPath != "" {
		if !boundFS.Access(ev.NewPath, syscall.F_OK, fctx).Ok() {
			return false
		}
	}
	return true
}

func toPBEvent(ev serverio.Event) *proto.SubscribeEvent {
	pb := &proto.SubscribeEvent{
		Path:       ev.Path,
		NewPath:    ev.NewPath,
		NewVersion: ev.NewVersion,
	}
	switch ev.Kind {
	case serverio.KindMutated:
		pb.Kind = proto.SubscribeEvent_MUTATED
	case serverio.KindDeleted:
		pb.Kind = proto.SubscribeEvent_DELETED
	case serverio.KindRenamed:
		pb.Kind = proto.SubscribeEvent_RENAMED
	case serverio.KindHeartbeat:
		pb.Kind = proto.SubscribeEvent_HEARTBEAT
	default:
		pb.Kind = proto.SubscribeEvent_KIND_UNSPECIFIED
	}
	return pb
}
