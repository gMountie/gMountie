package config

// AuthConfigType names an authentication scheme. It is wire-shared
// vocabulary: the client config declares which scheme it will use and the
// server config declares which scheme it accepts.
type AuthConfigType string

const (
	// AuthConfigTypeBasic is username/password auth (argon2id-hashed on the
	// server side, cleartext-over-TLS from the client).
	AuthConfigTypeBasic AuthConfigType = "basic"
	// AuthConfigTypeMTLS is mutual-TLS auth: the verified client certificate
	// is the credential and the cert CN (or first DNS SAN) is the principal.
	AuthConfigTypeMTLS AuthConfigType = "mtls"
)

// AuthConfig is the common shape of an `auth:` config section. The concrete
// per-scheme structs live with their owners (pkg/server/config,
// pkg/client/config); this interface is what generic code passes around.
type AuthConfig interface {
	// GetType returns the type of the auth configuration
	GetType() AuthConfigType
}

// AuthConfigBase is the minimal AuthConfig carrier: just the scheme type.
// Concrete auth configs embed it; it is also used directly where the scheme
// needs no further fields (e.g. the client's mtls auth section, where the
// TLS client certificate carries the actual credential).
type AuthConfigBase struct {
	Type AuthConfigType
}

func (a *AuthConfigBase) GetType() AuthConfigType {
	return a.Type
}
