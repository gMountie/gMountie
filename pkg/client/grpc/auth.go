package grpc

import (
	"context"
	"gmountie/pkg/common"
)

// BasicAuthCredentials is a struct that holds the basic authentication credentials
type BasicAuthCredentials struct {
	Username string
	Password string
}

// NewBasicAuthCredentials creates a new BasicAuthCredentials
func NewBasicAuthCredentials(username, password string) *BasicAuthCredentials {
	return &BasicAuthCredentials{
		Username: username,
		Password: password,
	}
}

// GetRequestMetadata returns the request metadata
func (b *BasicAuthCredentials) GetRequestMetadata(context.Context, ...string) (map[string]string, error) {
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
