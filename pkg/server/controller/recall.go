package controller

import (
	"go.gmountie.dev/gmountie/pkg/proto"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Recall is the server side of the coherence control stream. The client opens
// it once per mount; the server pushes RecallMsg (via the RecallRegistry) and
// reads RecallAck. Registration is keyed by the session id from the stream
// context (gRPC metadata header, same source as Subscribe).
//
// The stream lifecycle mirrors Subscribe: the controller registers on entry,
// reads in a loop, and deregisters on exit (EOF/cancel). The defer ensures
// deregistration even if the send-path inside RecallRegistry errors.
func (r *RpcServerImpl) Recall(stream proto.RpcFs_RecallServer) error {
	sessionID := sessionIDFromContext(stream.Context())
	if sessionID == "" {
		return status.Error(codes.Unauthenticated, "recall: no session")
	}
	release := r.recalls.Register(sessionID, stream.Send)
	defer release()
	for {
		ack, err := stream.Recv()
		if err != nil {
			return err // EOF / cancel -> defer release() deregisters
		}
		r.recalls.Ack(sessionID, ack.RecallId)
	}
}
