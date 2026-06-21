package controller

import (
	"context"

	"go.gmountie.dev/gmountie/pkg/common"

	"google.golang.org/grpc/metadata"
)

// sessionIDFromContext returns the caller's session_id from the incoming gRPC
// metadata, or "" when absent. The client's session-id interceptors stamp this
// header on every post-handshake unary and stream RPC (including Subscribe), so
// it is the one identifier shared between a mutating RPC and that same client's
// Subscribe stream. Used to tag mutation events with their origin session
// (Event.OriginSession) so the bus can skip echoing a client its own changes —
// matching the subscriber identity the Subscribe controller reads from the
// stream's metadata. Reading metadata (not request.SessionId) keeps both sides
// on the same source.
func sessionIDFromContext(ctx context.Context) string {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return ""
	}
	if ids := md.Get(common.MetadataSessionID); len(ids) > 0 {
		return ids[0]
	}
	return ""
}
