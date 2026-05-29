package controller

import (
	"context"
	"time"

	"gmountie/pkg/proto"
	"gmountie/pkg/server/principal"
	"gmountie/pkg/server/service"
	"gmountie/pkg/utils/log"

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
	sessions service.SessionManager
	volSvc   service.VolumeService
	proto.UnimplementedSessionServiceServer
}

var _ proto.SessionServiceServer = (*SessionController)(nil)

func NewSessionController(mgr service.SessionManager, volSvc service.VolumeService) *SessionController {
	return &SessionController{sessions: mgr, volSvc: volSvc}
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
	id, err := c.sessions.Create(p)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to create session: %v", err)
	}
	log.Log.Info("session created", zap.String("session_id", id), zap.String("principal", p))
	return &proto.SessionCreateReply{SessionId: id}, nil
}

func (c *SessionController) Resume(_ context.Context, req *proto.SessionResumeRequest) (*proto.SessionResumeReply, error) {
	if req.SessionId == "" {
		return nil, status.Error(codes.InvalidArgument, "session_id is required")
	}
	resumed, err := c.sessions.Resume(req.SessionId)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "resume failed: %v", err)
	}
	log.Log.Info("session resume requested",
		zap.String("session_id", req.SessionId),
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
	if _, err := c.sessions.Get(req.SessionId); err != nil {
		return status.Errorf(codes.NotFound, "unknown session: %s", req.SessionId)
	}

	log.Log.Info("keepalive stream opened", zap.String("session_id", req.SessionId))
	defer func() {
		c.sessions.MarkDisconnected(req.SessionId)
		log.Log.Info("keepalive stream closed; session marked disconnected",
			zap.String("session_id", req.SessionId))
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
