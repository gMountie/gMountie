// Package principal carries the authenticated principal across the request
// context, decoupled from both the grpc and service packages to avoid an
// import cycle.
package principal

import "context"

type keyT struct{}

var key keyT

// WithPrincipal returns a context carrying the authenticated principal.
func WithPrincipal(ctx context.Context, principal string) context.Context {
	return context.WithValue(ctx, key, principal)
}

// FromContext returns the authenticated principal, if any.
func FromContext(ctx context.Context) (string, bool) {
	p, ok := ctx.Value(key).(string)
	return p, ok
}
