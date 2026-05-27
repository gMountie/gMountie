package service

import (
	"context"
	"gmountie/pkg/common"
	"gmountie/pkg/server/config"
	"gmountie/pkg/utils/log"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

type UserDetails struct {
	Username string
}

// AuthService is a service for authentication
type AuthService interface {
	// Authorize checks if the user is authorized
	Authorize(ctx context.Context, method string) (bool, *UserDetails, error)
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
			users[user.Username] = user.Password
		}
		log.Log.Info("basic authentication is enabled")
		return NewBasicAuthService(users)
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

func (denyAllAuthService) Authorize(_ context.Context, _ string) (bool, *UserDetails, error) {
	return false, nil, status.Errorf(codes.Unauthenticated, "authentication is not configured")
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
func (a *BasicAuthService) Authorize(ctx context.Context, _ string) (bool, *UserDetails, error) {
	// Get the user and password from the metadata
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return false, nil, status.Errorf(codes.Internal, "metadata is not provided")
	}

	user := md.Get(common.MetadataAuthBasicUsername)
	password := md.Get(common.MetadataAuthBasicPassword)
	if len(user) == 0 || len(password) == 0 {
		return false, nil, status.Errorf(codes.Unauthenticated, "user or password is not provided")
	}

	// Check if the user exists
	if pass, ok := a.users[user[0]]; ok {
		// Check if the password is correct
		if pass == password[0] {
			userDetails := &UserDetails{Username: user[0]}
			return true, userDetails, nil
		}
	}
	return false, nil, status.Errorf(codes.Unauthenticated, "invalid user or password")
}
