package grpc

import "context"

// RequestIDMetadataKey is the gRPC metadata key used for the per-RPC
// request id. Lowercase to match gRPC's metadata canonicalization.
const RequestIDMetadataKey = "gmountie-request-id"

type ctxKeyRequestID struct{}

// RequestIDFromContext returns the request id stamped on ctx, or "" if
// none is set.
func RequestIDFromContext(ctx context.Context) string {
	if v, ok := ctx.Value(ctxKeyRequestID{}).(string); ok {
		return v
	}
	return ""
}

// NewContextWithRequestID returns a derived context that carries id.
func NewContextWithRequestID(ctx context.Context, id string) context.Context {
	return context.WithValue(ctx, ctxKeyRequestID{}, id)
}
