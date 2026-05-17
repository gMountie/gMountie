package controller

import (
	"gmountie/pkg/proto"
	serverio "gmountie/pkg/server/io"
)

// Subscribe is the server-streaming RPC for cache-invalidation push.
// One stream per (client connection, volume); the controller subscribes
// to the local EventBus and drains events into the stream until ctx
// cancels or send fails.
func (r *RpcServerImpl) Subscribe(request *proto.SubscribeRequest, stream proto.RpcFs_SubscribeServer) error {
	// Auth + volume access: the gRPC interceptor already authenticated the
	// connection; we trust the volume name here. A future tightening could
	// check per-volume ACLs.
	if _, err := r.fsService.GetVolumeFileSystem(request.Volume); err != nil {
		return err
	}
	events, cancel := r.bus.Subscribe(request.Volume)
	defer cancel()
	for {
		select {
		case <-stream.Context().Done():
			return stream.Context().Err()
		case ev, ok := <-events:
			if !ok {
				// Bus closed our channel due to backpressure overflow.
				return nil
			}
			if err := stream.Send(toPBEvent(ev)); err != nil {
				return err
			}
		}
	}
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
