package controller

import (
	"context"

	"go.gmountie.dev/gmountie/pkg/common"
	"go.gmountie.dev/gmountie/pkg/server/principal"
	"go.gmountie.dev/gmountie/pkg/server/service"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// resolveSession looks up a session by id and enforces ownership.
// Empty id → InvalidArgument; unknown id → NotFound.
// When the session was created with a non-empty principal, the caller's
// principal (from ctx) must match — otherwise PermissionDenied is returned.
// This check is a no-op for sessions created without an auth layer (empty
// principal) and for basic-auth (the interceptor sets ctx principal = session
// principal, so they are equal by construction). It binds only mTLS callers
// to their own sessions, preventing cert-CN=bob from using alice's session_id.
func resolveSession(ctx context.Context, sessions service.SessionManager, sessionID string) (service.Session, error) {
	if sessionID == "" {
		return nil, status.Error(codes.InvalidArgument, "session_id is required")
	}
	sess, err := sessions.Get(sessionID)
	if err != nil {
		// Fingerprint, never the raw id: session_id is a bearer token and this
		// string lands in the finish-call log line via grpc.error (#62 redaction).
		return nil, status.Errorf(codes.NotFound, "session not found: %s", common.FingerprintID(sessionID))
	}
	// Ownership check: reject if session belongs to a specific principal and
	// the caller is different (or unauthenticated under a session-owning server).
	ctxP, _ := principal.FromContext(ctx)
	if sess.Principal() != "" && ctxP != sess.Principal() {
		return nil, status.Error(codes.PermissionDenied, "session does not belong to the caller")
	}
	return sess, nil
}

// withIdempotency wraps do with the session's request_id dedup cache. An
// empty requestID is rejected with InvalidArgument — every mutating RPC
// must carry one. The generic parameter T is the concrete reply type so
// callers get a typed value back instead of any.
func withIdempotency[T any](sess service.Session, requestID string, do func() (T, error)) (T, error) {
	var zero T
	if requestID == "" {
		return zero, status.Error(codes.InvalidArgument, "request_id is required")
	}
	raw, err := sess.DoOnce(requestID, func() (any, error) {
		v, err := do()
		if err != nil {
			return nil, err
		}
		return v, nil
	})
	if err != nil {
		return zero, err
	}
	typed, ok := raw.(T)
	if !ok {
		return zero, status.Error(codes.Internal, "idempotency cache: unexpected reply type")
	}
	return typed, nil
}

// withOptionalIdempotency is withIdempotency for RPCs whose request_id is
// legitimately optional (WriteAndFlush pure-flush calls carry none, and
// existing clients send none for CopyFileRange either). An empty id runs do
// directly: it must NEVER reach the session cache, where "" would be a single
// shared key deduping ALL id-less calls against each other (Session.DoOnce
// itself has no empty-key guard). A non-empty id gets the standard dedup path.
func withOptionalIdempotency[T any](sess service.Session, requestID string, do func() (T, error)) (T, error) {
	if requestID == "" {
		return do()
	}
	return withIdempotency(sess, requestID, do)
}
