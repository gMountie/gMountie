package service

import (
	"context"
	"crypto/x509"

	"go.gmountie.dev/gmountie/pkg/common"
	"go.gmountie.dev/gmountie/pkg/common/passhash"
	"go.gmountie.dev/gmountie/pkg/server/config"
	"go.gmountie.dev/gmountie/pkg/utils/log"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"
)

type UserDetails struct {
	Username string
}

// AuthService is a service for authentication
type AuthService interface {
	// Authorize authenticates the caller. A non-nil *UserDetails and nil error
	// means authorized; a non-nil error means denied (caller receives the error).
	Authorize(ctx context.Context, method string) (*UserDetails, error)
}

// --------------------------- Factory ---------------------------

// NewAuthServiceFromConfig creates a new AuthService from the config
func NewAuthServiceFromConfig(cfg config.AuthConfig) AuthService {
	switch cfg.GetType() {
	case config.AuthConfigTypeBasic:
		// Create users map
		// Switch case AuthConfigTypeBasic guarantees cfg is *BasicAuthConfig.
		authConfig, _ := cfg.(*config.BasicAuthConfig)
		users := make(map[string]string)
		for _, user := range authConfig.Users {
			users[user.Username] = user.PasswordHash
		}
		log.Log.Info("basic authentication is enabled")
		return NewBasicAuthService(users)
	case config.AuthConfigTypeMTLS:
		log.Log.Info("mTLS authentication is enabled — principal from verified client certificate")
		return &mtlsAuthService{}
	default:
		// Unreachable: config parsing rejects unknown auth types. Fail closed
		// (deny) rather than open, so a misconfiguration can never run unauthed.
		log.Log.Error("unknown auth type; denying all requests")
		return &denyAllAuthService{}
	}
}

// --------------------------- Implementations ---------------------------

// ----------- denyAllAuthService -----------

// denyAllAuthService fails closed for an unconfigured/unknown auth type. It is
// unreachable in practice (config parsing rejects unknown types) but ensures a
// misconfiguration denies rather than silently runs unauthenticated.
type denyAllAuthService struct{}

func (denyAllAuthService) Authorize(_ context.Context, _ string) (*UserDetails, error) {
	return nil, status.Errorf(codes.Unauthenticated, "authentication is not configured")
}

// ----------- BasicAuthService -----------

// BasicAuthService is a service that performs basic authentication
type BasicAuthService struct {
	users map[string]string
}

// NewBasicAuthService creates a new BasicAuthService
func NewBasicAuthService(users map[string]string) *BasicAuthService {
	return &BasicAuthService{users: users}
}

// Authorize checks if the user is authorized
func (a *BasicAuthService) Authorize(ctx context.Context, _ string) (*UserDetails, error) {
	// Get the user and password from the metadata
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return nil, status.Errorf(codes.Internal, "metadata is not provided")
	}

	user := md.Get(common.MetadataAuthBasicUsername)
	password := md.Get(common.MetadataAuthBasicPassword)
	if len(user) == 0 || len(password) == 0 {
		return nil, status.Errorf(codes.Unauthenticated, "user or password is not provided")
	}

	// Check if the user exists
	if storedHash, ok := a.users[user[0]]; ok {
		// Verify the password against the stored argon2id PHC hash.
		// On a malformed hash treat as auth-denied (fail closed); never panic.
		match, err := passhash.Verify(storedHash, password[0])
		if err != nil || !match {
			return nil, status.Errorf(codes.Unauthenticated, "invalid user or password")
		}
		return &UserDetails{Username: user[0]}, nil
	}
	return nil, status.Errorf(codes.Unauthenticated, "invalid user or password")
}

// ----------- mtlsAuthService -----------

// mtlsAuthService authenticates by the verified client certificate. The TLS
// layer (RequireAndVerifyClientCert) has already validated the chain against
// the configured client CA, so a present verified cert is a trusted identity.
// The principal is the cert CN, or the first DNS SAN when CN is empty
// (Phase 7 decision #6). Per-volume authorization is the ACL's job
// (VolumeService.PrincipalCanAccess), run later in BindIdentity.
type mtlsAuthService struct{}

func (mtlsAuthService) Authorize(ctx context.Context, _ string) (*UserDetails, error) {
	p, ok := peer.FromContext(ctx)
	if !ok {
		return nil, status.Error(codes.Unauthenticated, "no peer info")
	}
	ti, ok := p.AuthInfo.(credentials.TLSInfo)
	if !ok {
		return nil, status.Error(codes.Unauthenticated, "connection is not mTLS")
	}
	name := principalFromVerifiedChains(ti.State.VerifiedChains)
	if name == "" {
		return nil, status.Error(codes.Unauthenticated, "no verified client certificate")
	}
	return &UserDetails{Username: name}, nil
}

// VerifiedCertPrincipal returns the verified client-cert principal (CN, or
// first DNS SAN) and true when a verified client certificate is present on
// the connection. Returns ("", false) when there is no client cert (e.g.
// server-only TLS with basic-auth, or no peer info).
//
// Key subtlety: basic-auth connections also carry TLSInfo (the server always
// terminates TLS) but VerifiedChains is empty — RequireAndVerifyClientCert is
// only set in mTLS mode. This function gates on a non-empty VerifiedChains so
// basic-auth connections correctly return present=false.
func VerifiedCertPrincipal(ctx context.Context) (principal string, present bool) {
	p, ok := peer.FromContext(ctx)
	if !ok {
		return "", false
	}
	ti, ok := p.AuthInfo.(credentials.TLSInfo)
	if !ok {
		return "", false
	}
	if len(ti.State.VerifiedChains) == 0 || len(ti.State.VerifiedChains[0]) == 0 {
		// Server-only TLS (basic-auth case): no client cert was verified.
		return "", false
	}
	return principalFromVerifiedChains(ti.State.VerifiedChains), true
}

// VerifiedCertSerial returns the canonical SerialKey of the verified client
// cert's leaf and true when a verified client certificate is present. Mirrors
// VerifiedCertPrincipal: basic-auth connections (empty VerifiedChains) return
// ("", false). The session records this key so the reaper can match a blocked
// serial out of band; the handshake hook and interceptor compute it live.
func VerifiedCertSerial(ctx context.Context) (serialKey string, present bool) {
	p, ok := peer.FromContext(ctx)
	if !ok {
		return "", false
	}
	ti, ok := p.AuthInfo.(credentials.TLSInfo)
	if !ok {
		return "", false
	}
	if len(ti.State.VerifiedChains) == 0 || len(ti.State.VerifiedChains[0]) == 0 {
		return "", false
	}
	return SerialKey(ti.State.VerifiedChains[0][0].SerialNumber), true
}

// principalFromVerifiedChains returns the leaf cert's CN, or its first DNS SAN
// when CN is empty. Empty string when there is no verified leaf.
func principalFromVerifiedChains(chains [][]*x509.Certificate) string {
	if len(chains) == 0 || len(chains[0]) == 0 {
		return ""
	}
	leaf := chains[0][0]
	if leaf.Subject.CommonName != "" {
		return leaf.Subject.CommonName
	}
	if len(leaf.DNSNames) > 0 {
		return leaf.DNSNames[0]
	}
	return ""
}
