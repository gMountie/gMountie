package grpc

import (
	"context"
	"gmountie/pkg/server/principal"
	"gmountie/pkg/server/service"

	"github.com/grpc-ecosystem/go-grpc-middleware/v2/interceptors/logging"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// AuthInterceptor is a server interceptor for authentication
type AuthInterceptor struct {
	authService service.AuthService
}

// NewAuthInterceptor creates a new AuthInterceptor
func NewAuthInterceptor(authService service.AuthService) *AuthInterceptor {
	return &AuthInterceptor{authService: authService}
}

// Unary returns a UnaryServerInterceptor
func (i *AuthInterceptor) Unary() grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
		ok, user, err := i.authService.Authorize(ctx, info.FullMethod)
		if err != nil {
			return nil, err
		} else if !ok {
			return nil, status.Errorf(codes.PermissionDenied, "unauthorized")
		}
		ctx = logging.InjectLogField(ctx, "user", user.Username)
		ctx = principal.WithPrincipal(ctx, user.Username)
		return handler(ctx, req)
	}
}

// Stream returns a StreamServerInterceptor
func (i *AuthInterceptor) Stream() grpc.StreamServerInterceptor {
	return func(srv interface{}, stream grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		ok, _, err := i.authService.Authorize(stream.Context(), info.FullMethod)
		if err != nil {
			return err
		} else if !ok {
			return status.Errorf(codes.PermissionDenied, "unauthorized")
		}
		return handler(srv, stream)
	}
}
