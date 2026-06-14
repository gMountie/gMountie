package controller

import (
	"context"
	"time"

	"go.gmountie.dev/gmountie/pkg/common"
	"go.gmountie.dev/gmountie/pkg/proto"
	"go.gmountie.dev/gmountie/pkg/server/principal"
	"go.gmountie.dev/gmountie/pkg/server/service"
	"go.gmountie.dev/gmountie/pkg/utils/log"

	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// keepalivePingInterval is how often the server emits a heartbeat to clients
// holding open a Keepalive stream. Short enough that a half-broken TCP
// connection surfaces before the next file RPC.
const keepalivePingInterval = 10 * time.Second

type SessionController struct {
	sessions   service.SessionManager
	volSvc     service.VolumeService
	bootEpoch  string
	proto.UnimplementedSessionServiceServer
}

var _ proto.SessionServiceServer = (*SessionController)(nil)

func NewSessionController(mgr service.SessionManager, volSvc service.VolumeService, bootEpoch string) *SessionController {
	return &SessionController{sessions: mgr, volSvc: volSvc, bootEpoch: bootEpoch}
}

func (c *SessionController) Register(server *grpc.Server) {
	proto.RegisterSessionServiceServer(server, c)
}

func (c *SessionController) Create(ctx context.Context, _ *proto.SessionCreateRequest) (*proto.SessionCreateReply, error) {
	// The AuthInterceptor runs full argon2 Authorize on SessionService/Create
	// and injects the principal before this handler is reached. Bind the
	// session to that principal so later RPCs can skip argon2 by session_id.
	// When no principal is in context (e.g. test servers without an auth
	// interceptor), bind an empty string — an empty principal will fail any
	// subsequent PrincipalCanAccess check and cannot grant volume access.
	p, _ := principal.FromContext(ctx)
	serial, _ := service.VerifiedCertSerial(ctx)
	id, err := c.sessions.Create(p, serial)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to create session: %v", err)
	}
	log.Log.Info("session created", zap.String("session_fp", common.FingerprintID(id)), zap.String("principal", p))
	return &proto.SessionCreateReply{SessionId: id, BootEpoch: c.bootEpoch}, nil
}

func (c *SessionController) Resume(ctx context.Context, req *proto.SessionResumeRequest) (*proto.SessionResumeReply, error) {
	if req.SessionId == "" {
		return nil, status.Error(codes.InvalidArgument, "session_id is required")
	}
	// Ownership: only the session's owner may resume it. An unknown session is
	// owned by no one (already reaped) — let Resume report resumed=false so the
	// client falls back to Create. No-op for basic-auth (principal is derived
	// from the session); binds mTLS (cert CN is independent of the session_id).
	if sess, err := c.sessions.Get(req.SessionId); err == nil {
		ctxP, _ := principal.FromContext(ctx)
		if sess.Principal() != "" && ctxP != sess.Principal() {
			return nil, status.Error(codes.PermissionDenied, "session does not belong to the caller")
		}
	}
	resumed, err := c.sessions.Resume(req.SessionId)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "resume failed: %v", err)
	}
	log.Log.Info("session resume requested",
		zap.String("session_fp", common.FingerprintID(req.SessionId)),
		zap.Bool("resumed", resumed))
	return &proto.SessionResumeReply{Resumed: resumed}, nil
}

func (c *SessionController) WhoAmI(ctx context.Context, req *proto.WhoAmIRequest) (*proto.Identity, error) {
	if err := c.volSvc.PrincipalCanAccess(ctx, req.Volume); err != nil {
		return nil, err
	}
	id, err := c.volSvc.ResolveIdentity(ctx, req.Volume, req.Caller)
	if err != nil {
		return nil, status.Errorf(codes.PermissionDenied, "whoami: %v", err)
	}
	return &proto.Identity{
		Principal:  id.Principal,
		Uid:        id.Uid,
		PrimaryGid: id.Gid,
		Gids:       id.Gids,
		UserName:   id.UserName,
		GroupNames: id.GroupNames,
	}, nil
}

func (c *SessionController) Keepalive(req *proto.KeepaliveRequest, stream proto.SessionService_KeepaliveServer) error {
	if req.SessionId == "" {
		return status.Error(codes.InvalidArgument, "session_id is required")
	}
	sess, err := c.sessions.Get(req.SessionId)
	if err != nil {
		// Fingerprint, never the raw id (bearer token; lands in logs via grpc.error).
		return status.Errorf(codes.NotFound, "unknown session: %s", common.FingerprintID(req.SessionId))
	}
	// Ownership: only the session's owner may drive its keepalive — otherwise a
	// cross-user caller could open+close a keepalive on someone else's session
	// and trigger MarkDisconnected, reaping their fds mid-use (availability DoS).
	// No-op for basic-auth (bearer model); binds mTLS via the cert principal.
	ctxP, _ := principal.FromContext(stream.Context())
	if sess.Principal() != "" && ctxP != sess.Principal() {
		return status.Error(codes.PermissionDenied, "session does not belong to the caller")
	}

	log.Log.Info("keepalive stream opened", zap.String("session_fp", common.FingerprintID(req.SessionId)))
	defer func() {
		c.sessions.MarkDisconnected(req.SessionId)
		log.Log.Info("keepalive stream closed; session marked disconnected",
			zap.String("session_fp", common.FingerprintID(req.SessionId)))
	}()

	ticker := time.NewTicker(keepalivePingInterval)
	defer ticker.Stop()
	for {
		select {
		case <-stream.Context().Done():
			return nil
		case <-ticker.C:
			if err := stream.Send(&proto.KeepalivePing{}); err != nil {
				return err
			}
		}
	}
}
