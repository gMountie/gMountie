package grpc

import (
	"context"
	"gmountie/pkg/common"
)

// BasicAuthCredentials is a struct that holds the basic authentication credentials
type BasicAuthCredentials struct {
	Username string
	Password string
	// sessionLive, when non-nil, is called before each RPC. If it returns true
	// the session is confirmed live and basic-auth metadata is omitted — the
	// session_id injected by sessionIDUnaryInterceptor / sessionIDStreamInterceptor
	// is sufficient for the server to authorize the call.
	//
	// Why sessionLive alone is the correct gate (no per-method URI check needed):
	//   - Create runs in Establish *before* healthy=true is set → creds sent.
	//   - Resume runs inside recover() while healthy=false → creds sent.
	//   - All steady-state ops (File, Fs, Volume, …) run with healthy=true → creds omitted.
	//
	// nil means always send (preserves callers that do not wire a session).
	sessionLive func() bool
}

// NewBasicAuthCredentials creates a new BasicAuthCredentials.
// The returned credentials always send basic-auth; wire sessionLive via
// WithBasicAuth (which sets the field after construction) to enable
// the session-live optimisation.
func NewBasicAuthCredentials(username, password string) *BasicAuthCredentials {
	return &BasicAuthCredentials{
		Username: username,
		Password: password,
	}
}

// GetRequestMetadata returns the request metadata.
// When sessionLive is wired and returns true, basic-auth is omitted: the
// per-RPC session_id (added by sessionIDUnaryInterceptor) authorises the call
// on the server side without requiring argon2 re-verification.
func (b *BasicAuthCredentials) GetRequestMetadata(context.Context, ...string) (map[string]string, error) {
	if b.sessionLive != nil && b.sessionLive() {
		// Session is live; omit basic-auth to avoid the ~11% small-read
		// throughput regression from carrying redundant per-RPC metadata.
		return nil, nil
	}
	return map[string]string{
		common.MetadataAuthBasicUsername: b.Username,
		common.MetadataAuthBasicPassword: b.Password,
	}, nil
}

// RequireTransportSecurity returns if the transport security is required.
// Always true — basic-auth credentials must not be sent over plaintext.
func (b *BasicAuthCredentials) RequireTransportSecurity() bool {
	return true
}
