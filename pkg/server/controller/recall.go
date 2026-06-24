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
//
// Security: the session is resolved via resolveSession (same ownership-enforcing
// helper used by all other handlers) before any registration occurs. This
// prevents a foreign principal from opening a Recall stream under another
// principal's session_id and receiving or acking their recalls.
func (r *RpcServerImpl) Recall(stream proto.RpcFs_RecallServer) error {
	sessionID := sessionIDFromContext(stream.Context())
	if sessionID == "" {
		return status.Error(codes.Unauthenticated, "recall: no session")
	}
	// Ownership check: reject if the stream's authenticated principal does not
	// own the session (prevents cert-CN=bob from hijacking alice's recall stream).
	sess, err := resolveSession(stream.Context(), r.sessions, sessionID)
	if err != nil {
		return err
	}
	release := r.recalls.Register(sess.ID(), stream.Send)
	// On stream close, deregister the send-fn AND release this session's
	// delegations from the arbiter table so a contender is never blocked by a
	// holder that can no longer be recalled. The gen-revoke is deferred to the
	// grace-period reap (a transient blip must not fence still-valid WAL).
	defer func() {
		release()
		if r.arbiter != nil {
			r.arbiter.DeferRevokeOnStreamClose(sess.ID())
		}
	}()
	for {
		ack, err := stream.Recv()
		if err != nil {
			return err // EOF / cancel -> defer release() deregisters
		}
		r.recalls.Ack(sess.ID(), ack.RecallId)
	}
}
