package controller

import (
	"gmountie/pkg/server/service"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// resolveSession looks up a session by id. Empty id → InvalidArgument;
// unknown id → NotFound. Shared by file.go and fs.go handlers.
func resolveSession(sessions service.SessionManager, sessionID string) (service.Session, error) {
	if sessionID == "" {
		return nil, status.Error(codes.InvalidArgument, "session_id is required")
	}
	sess, err := sessions.Get(sessionID)
	if err != nil {
		return nil, status.Errorf(codes.NotFound, "session not found: %s", sessionID)
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
