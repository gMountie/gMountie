package api

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"go.gmountie.dev/gmountie/pkg/proto"
	"go.gmountie.dev/gmountie/test/e2e/utils"

	"github.com/stretchr/testify/suite"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// reapGrace is deliberately short so a disconnect reaps the session in well
// under a second. The test polls for the reap rather than sleeping an exact
// duration, so the precise value is not load-bearing.
const reapGrace = 200 * time.Millisecond

// SessionReapE2ESuite pins the WIRE contract of session reap past the grace
// period — the failure half of the session lifecycle, complementing
// ResilienceSuite (which proves the same semantics behaviourally through the
// client's recovery loop):
//
//   - Resume on a reaped session returns resumed=false (not an error): the
//     signal the client's recovery loop keys on to fall back to Create.
//   - fd-ops referencing the reaped session fail with codes.NotFound
//     ("session not found"), the defined permanent status that makes the
//     client short-circuit instead of retrying.
//   - A fresh Create fully restores service: new session id, usable fds.
//
// The suite drives raw proto clients over a dedicated conn (NewRawConn), so
// no client-side machinery can mask or auto-repair the contract. bufconn has
// no severable link; the disconnect is injected via
// SessionManager.MarkDisconnected — exactly the call the server's own
// Keepalive handler makes when a client's stream drops (controller/
// session.go), so the test enters the reap path at the same seam a real
// network drop would.
type SessionReapE2ESuite struct {
	suite.Suite
	appCtx   *utils.AppTestingContext
	conn     *grpc.ClientConn
	sessions proto.SessionServiceClient
	file     proto.RpcFileClient
	volName  string
}

func (s *SessionReapE2ESuite) SetupSuite() {
	appCtx, err := utils.NewAppTestingContext(
		utils.WithBasicAuth("test", "test"),
		utils.WithRandomTestVolume(false),
		utils.WithSessionGracePeriod(reapGrace),
	)
	s.Require().NoError(err)
	s.Require().NoError(appCtx.Start())
	s.appCtx = appCtx
	// Safety net: a failed Require below skips TearDownSuite; Close is
	// idempotent, so this coexists with TearDownSuite's Close.
	s.T().Cleanup(func() { _ = appCtx.Close() })
	s.volName = appCtx.GetVolumes()[0].Name

	conn, err := appCtx.NewRawConn("test", "test")
	s.Require().NoError(err)
	s.conn = conn
	s.sessions = proto.NewSessionServiceClient(conn)
	s.file = proto.NewRpcFileClient(conn)
}

func (s *SessionReapE2ESuite) TearDownSuite() {
	if s.conn != nil {
		_ = s.conn.Close()
	}
	if s.appCtx != nil {
		s.Require().NoError(s.appCtx.Close())
	}
}

// reapSession marks the session disconnected (the same call the Keepalive
// handler makes when a client's stream drops) and waits until the grace
// period elapses and the reaper removes it.
func (s *SessionReapE2ESuite) reapSession(sid string) {
	s.T().Helper()
	sm := s.appCtx.GetServerApp().SessionManager
	sm.MarkDisconnected(sid)
	s.Require().Eventually(func() bool {
		_, err := sm.Get(sid)
		return err != nil
	}, 10*time.Second, 10*time.Millisecond,
		"server must reap the session once the grace period (%s) elapses", reapGrace)
}

// TestReapPastGraceWireContract walks the full failure half of the session
// contract over the wire: live session works → reap past grace →
// Resume(resumed=false) → old-fd op fails NotFound → fresh Create restores
// service.
func (s *SessionReapE2ESuite) TestReapPastGraceWireContract() {
	ctx := context.Background()

	// Seed a file server-side so Open has a target.
	srcPath := filepath.Join(s.appCtx.GetVolumes()[0].GetSrcPath(), "reap.bin")
	s.Require().NoError(os.WriteFile(srcPath, []byte("reap-me"), 0o644))

	// Create a session and open an fd on it.
	created, err := s.sessions.Create(ctx, &proto.SessionCreateRequest{})
	s.Require().NoError(err)
	sid := created.GetSessionId()
	s.Require().NotEmpty(sid)

	caller := &proto.Caller{Owner: &proto.Owner{Uid: 0, Gid: 0}}
	openReply, err := s.file.Open(ctx, &proto.OpenRequest{
		Volume:    s.volName,
		Path:      "/reap.bin",
		Flags:     uint32(os.O_RDONLY),
		Caller:    caller,
		SessionId: sid,
		RequestId: "reap-open-1",
	})
	s.Require().NoError(err, "Open on a live session must succeed")
	s.Require().Zero(openReply.GetStatus(), "Open must return fuse.OK")
	fd := openReply.GetFd()

	// Sanity: an fd-op on the live session works before the reap.
	_, err = s.file.Flush(ctx, &proto.FlushRequest{Volume: s.volName, Fd: fd, SessionId: sid})
	s.Require().NoError(err, "Flush on a live session must succeed")

	// Disconnect and wait past the grace period: the server reaps the
	// session (fds released, idempotency cache dropped).
	s.reapSession(sid)

	// Contract 1: Resume reports resumed=false — NOT an error. This is the
	// definitive signal that tells a client to fall back to Create.
	resumed, err := s.sessions.Resume(ctx, &proto.SessionResumeRequest{SessionId: sid})
	s.Require().NoError(err, "Resume on a reaped session must not be a transport error")
	s.Require().False(resumed.GetResumed(),
		"Resume on a reaped session must report resumed=false")

	// Contract 2: an fd-op carrying the reaped session id fails with the
	// defined permanent status codes.NotFound ("session not found"), so a
	// client short-circuits cleanly instead of retrying.
	_, err = s.file.Flush(ctx, &proto.FlushRequest{Volume: s.volName, Fd: fd, SessionId: sid})
	s.Require().Error(err, "fd-op on a reaped session must fail")
	st, ok := status.FromError(err)
	s.Require().True(ok, "error must be a gRPC status")
	s.Require().Equal(codes.NotFound, st.Code(),
		"fd-op on a reaped session must surface codes.NotFound, got %s: %s", st.Code(), st.Message())

	// Contract 3: a fresh Create fully restores service — new session id,
	// new usable fds.
	created2, err := s.sessions.Create(ctx, &proto.SessionCreateRequest{})
	s.Require().NoError(err, "fresh Create after reap must succeed")
	sid2 := created2.GetSessionId()
	s.Require().NotEmpty(sid2)
	s.Require().NotEqual(sid, sid2, "fresh Create must mint a new session id")

	openReply2, err := s.file.Open(ctx, &proto.OpenRequest{
		Volume:    s.volName,
		Path:      "/reap.bin",
		Flags:     uint32(os.O_RDONLY),
		Caller:    caller,
		SessionId: sid2,
		RequestId: "reap-open-2",
	})
	s.Require().NoError(err, "Open on the fresh session must succeed")
	s.Require().Zero(openReply2.GetStatus(), "Open on the fresh session must return fuse.OK")
	_, err = s.file.Flush(ctx, &proto.FlushRequest{
		Volume: s.volName, Fd: openReply2.GetFd(), SessionId: sid2,
	})
	s.Require().NoError(err, "fd-op on the fresh session must succeed — service restored")
}

func TestSessionReapE2ESuite(t *testing.T) {
	suite.Run(t, new(SessionReapE2ESuite))
}
