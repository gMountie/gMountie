package grpc

import (
	"context"

	"gmountie/pkg/common"
	"gmountie/pkg/proto"
	"gmountie/pkg/server/principal"
	"gmountie/pkg/server/service"

	"github.com/grpc-ecosystem/go-grpc-middleware/v2/interceptors/logging"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

// fullAuthMethods lists every RPC that MUST run the full authService.Authorize
// path (argon2 for basic-auth, cert-check for mTLS) regardless of whether a
// session_id is present.
//
//   - SessionService/Create  — establishes the session→principal binding.
//   - SessionService/Resume  — re-proves identity after a reconnect.
//
// IMPORTANT: these methods must NEVER be moved to the session-skip path.
// Create is where argon2 runs (once); removing it here would let an
// unauthenticated caller create sessions. Resume re-authenticates on reconnect
// so a lost/expired session cannot be hijacked by replaying an old session_id.
const (
	methodSessionCreate = proto.SessionService_Create_FullMethodName
	methodSessionResume = proto.SessionService_Resume_FullMethodName
)

// AuthInterceptor is a server interceptor for authentication.
type AuthInterceptor struct {
	authService service.AuthService
	sessions    service.SessionManager
}

// NewAuthInterceptor creates a new AuthInterceptor.
// sessions must be the same SessionManager instance used by the
// SessionController so principals written at Create are visible here.
func NewAuthInterceptor(authService service.AuthService, sessions service.SessionManager) *AuthInterceptor {
	return &AuthInterceptor{authService: authService, sessions: sessions}
}

// authorize is the single point of auth decision for both Unary and Stream.
//
// Decision order:
//  1. If fullMethod is a full-auth method (Create/Resume), always run argon2.
//  2. If metadata carries a known session_id, skip argon2 and return the
//     session's principal.
//  3. Fall through to full authService.Authorize (argon2 / cert check).
//
// FAIL-CLOSED: an absent, empty, or unknown session_id NEVER short-circuits to
// "allowed" — it always falls through to Authorize, which denies missing creds.
func (i *AuthInterceptor) authorize(ctx context.Context, fullMethod string) (context.Context, error) {
	// Step 1: methods that must always re-prove identity.
	if fullMethod != methodSessionCreate && fullMethod != methodSessionResume {
		// Step 2: try session-id fast path — but only when no verified client
		// cert is present. When a cert IS present (mTLS), the principal must come
		// from the cert so the ownership check in resolveSession is meaningful and
		// a cert-CN=bob cannot be labeled "alice" by presenting alice's session_id.
		// For basic-auth the server uses server-only TLS (VerifiedChains is empty),
		// so VerifiedCertPrincipal returns false and the session fast-path applies
		// — that is where the argon2-skip perf win is needed.
		if _, certPresent := service.VerifiedCertPrincipal(ctx); !certPresent {
			if md, ok := metadata.FromIncomingContext(ctx); ok {
				ids := md.Get(common.MetadataSessionID)
				if len(ids) > 0 && ids[0] != "" {
					sess, err := i.sessions.Get(ids[0])
					if err == nil {
						// Known session — skip argon2, inject principal from session.
						p := sess.Principal()
						ctx = logging.InjectLogField(ctx, "user", p)
						ctx = principal.WithPrincipal(ctx, p)
						return ctx, nil
					}
					// Unknown / bogus session_id — fall through to full auth.
					// This is intentional: treat it identically to "no session_id".
				}
			}
		}
	}

	// Step 3: full auth path (argon2 or mTLS cert check).
	user, err := i.authService.Authorize(ctx, fullMethod)
	if err != nil {
		return nil, err
	}
	if user == nil {
		return nil, status.Errorf(codes.PermissionDenied, "unauthorized")
	}
	ctx = logging.InjectLogField(ctx, "user", user.Username)
	ctx = principal.WithPrincipal(ctx, user.Username)
	return ctx, nil
}

// Unary returns a UnaryServerInterceptor.
func (i *AuthInterceptor) Unary() grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
		ctx, err := i.authorize(ctx, info.FullMethod)
		if err != nil {
			return nil, err
		}
		return handler(ctx, req)
	}
}

// principalServerStream wraps a grpc.ServerStream to replace its Context.
// Used by Stream() to inject the post-auth principal into the streaming
// handler's context so that resolveSession's ownership check can read it.
type principalServerStream struct {
	grpc.ServerStream
	ctx context.Context
}

func (s *principalServerStream) Context() context.Context { return s.ctx }

// Stream returns a StreamServerInterceptor. It injects the authenticated
// principal into the stream's context so streaming handlers (Read/Write) can
// enforce the session-ownership check in resolveSession. The session-id fast
// path still applies — argon2 is skipped for authenticated sessions.
func (i *AuthInterceptor) Stream() grpc.StreamServerInterceptor {
	return func(srv interface{}, stream grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		newCtx, err := i.authorize(stream.Context(), info.FullMethod)
		if err != nil {
			return err
		}
		return handler(srv, &principalServerStream{ServerStream: stream, ctx: newCtx})
	}
}
