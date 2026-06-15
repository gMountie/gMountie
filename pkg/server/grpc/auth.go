package grpc

import (
	"context"
	"path"
	"sync"
	"sync/atomic"
	"time"

	"go.gmountie.dev/gmountie/pkg/common"
	"go.gmountie.dev/gmountie/pkg/proto"
	"go.gmountie.dev/gmountie/pkg/server/metrics"
	"go.gmountie.dev/gmountie/pkg/server/principal"
	"go.gmountie.dev/gmountie/pkg/server/service"
	"go.gmountie.dev/gmountie/pkg/utils/log"

	"github.com/grpc-ecosystem/go-grpc-middleware/v2/interceptors/logging"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"
)

// authWarnInterval throttles the per-denial Warn to at most one line per reason
// per interval. The revoked branch fires PER-RPC on an established connection,
// so a revoked client (or a credential-spammer) would otherwise drive an
// unbounded log stream — an attacker-triggerable log-volume amplification. The
// AuthFailures counter stays exact (incremented every denial); only the human
// log is sampled (OB-H1).
const authWarnInterval = 10 * time.Second

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
	revocation  *service.RevocationStore
	// metrics is an optional sink for the auth-failure counter. Nil-guarded so
	// tests and metrics-disabled servers are unaffected (OB-H1).
	metrics *metrics.Metrics
	// warnLast throttles the denial Warn per reason: reason -> *atomic.Int64
	// holding the last log time (unix-nanos). The counter is never throttled.
	warnLast sync.Map
}

// shouldWarn reports whether a denial Warn for reason is due now, and if so
// records the time. At most one log per reason per authWarnInterval. Lock-free
// on the hot (recently-logged) path; a CAS guards the once-per-interval window.
func (i *AuthInterceptor) shouldWarn(reason string) bool {
	now := time.Now().UnixNano()
	v, _ := i.warnLast.LoadOrStore(reason, new(atomic.Int64))
	last := v.(*atomic.Int64)
	prev := last.Load()
	if now-prev < int64(authWarnInterval) && prev != 0 {
		return false
	}
	return last.CompareAndSwap(prev, now)
}

// NewAuthInterceptor creates a new AuthInterceptor.
// sessions must be the same SessionManager instance used by the
// SessionController so principals written at Create are visible here.
// revocation is consulted per-RPC to reject blocked cert serials on
// already-established connections (complements the TLS handshake hook).
// Passing nil disables the per-RPC check (safe for non-mTLS servers).
// m is an optional metrics sink for the auth-failure counter; nil is safe.
func NewAuthInterceptor(authService service.AuthService, sessions service.SessionManager, revocation *service.RevocationStore, m *metrics.Metrics) *AuthInterceptor {
	return &AuthInterceptor{authService: authService, sessions: sessions, revocation: revocation, metrics: m}
}

// denied records an auth denial: it ALWAYS bumps the auth-failure counter
// (nil-safe) and emits a THROTTLED Warn so a denial — invisible to the
// downstream finish-call logger and metrics interceptors, which run AFTER auth
// — is observable without giving a revoked/spamming client an unbounded log
// faucet (OB-H1).
func (i *AuthInterceptor) denied(ctx context.Context, fullMethod, reason string, err error) {
	method := path.Base(fullMethod)
	if i.metrics != nil {
		i.metrics.AuthFailureInc(method, reason)
	}
	if !i.shouldWarn(reason) {
		return
	}
	fields := []zap.Field{
		zap.String("method", method),
		zap.String("reason", reason),
	}
	if p, ok := peer.FromContext(ctx); ok && p.Addr != nil {
		fields = append(fields, zap.String("peer", p.Addr.String()))
	}
	if err != nil {
		fields = append(fields, zap.Error(err))
	}
	log.Log.Warn("auth denied", fields...)
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
	// Step 0: per-RPC revocation check — catches connections that handshook
	// before a serial was added to the blocklist. Nil-guarded so non-mTLS
	// servers and tests without a store are unaffected.
	if i.revocation != nil {
		if serial, ok := service.VerifiedCertSerial(ctx); ok && i.revocation.IsBlocked(serial) {
			err := status.Errorf(codes.Unauthenticated, "client certificate revoked")
			i.denied(ctx, fullMethod, "revoked", err)
			return nil, err
		}
	}

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
		i.denied(ctx, fullMethod, "authorize_error", err)
		return nil, err
	}
	if user == nil {
		denyErr := status.Errorf(codes.PermissionDenied, "unauthorized")
		i.denied(ctx, fullMethod, "nil_user", denyErr)
		return nil, denyErr
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
