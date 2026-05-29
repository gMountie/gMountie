package common

const (
	MetadataAuthBasicUsername = "auth-basic-username"
	MetadataAuthBasicPassword = "auth-basic-password"

	// MetadataSessionID carries the server-assigned session id on every
	// outgoing client RPC after the handshake. The AuthInterceptor uses it
	// to skip argon2 for already-authenticated sessions.
	MetadataSessionID = "session-id"
)
