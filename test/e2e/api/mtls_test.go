package api

// mtls_test.go exercises the mTLS authentication + volume ACL end-to-end at
// the gRPC API level (in-process, no FUSE mount). A test CA is generated in
// memory; a server is configured with auth.type:mtls and default_allow:false;
// alice's cert grants the volume, mallory's cert does not.
//
// This is the in-process fallback path chosen over a FUSE-on-VM test because:
//   (a) the mTLS handshake + principal-extraction + ACL enforcement is the
//       property under test; FUSE is not required to prove it;
//   (b) bufconn in-process tests run in the sandbox without root or FUSE.
//
// The test CA helper is in test/e2e/utils/mtls.go (GenerateTestCA, ~120 LOC).
// WithMTLS (in test/e2e/utils/app.go) wires the CA-signed server cert + client
// CA verification on the server, and the primary client cert on the default
// gRPC client. A second raw gRPC client (mallory) is built directly with
// credentials.NewTLS to avoid coupling the helper to multi-client mTLS.

import (
	"context"
	"crypto/tls"
	"testing"

	grpcClient "gmountie/pkg/client/grpc"
	"gmountie/pkg/proto"
	"gmountie/test/e2e/utils"

	"github.com/stretchr/testify/suite"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// MTLSSuite verifies mTLS principal extraction and per-volume ACL denial.
type MTLSSuite struct {
	suite.Suite
	app     *utils.AppTestingContext
	ca      *utils.TestCA
	volName string
}

func (s *MTLSSuite) SetupSuite() {
	s.volName = "data"
	volDir := s.T().TempDir()

	// Generate a test CA with certs for "alice" (granted) and "mallory" (not granted).
	ca, err := utils.GenerateTestCA("alice", "mallory")
	s.Require().NoError(err)
	s.ca = ca

	app, err := utils.NewAppTestingContext(
		utils.WithMTLS(ca, "alice",
			utils.ACLUser{Username: "alice", Volumes: []string{"data"}},
			// mallory has no volume grants; default_allow is false.
		),
		utils.WithExistingVolume("data", volDir),
	)
	s.Require().NoError(err)
	s.Require().NoError(app.Start())
	s.app = app
}

func (s *MTLSSuite) TearDownSuite() {
	if s.app != nil {
		_ = s.app.Close()
	}
}

// buildMalloryClient creates a raw gRPC client that presents mallory's
// client certificate. It connects to the same bufconn listener.
func (s *MTLSSuite) buildMalloryClient() grpcClient.Client {
	malloryLeaf, err := tls.X509KeyPair(
		s.ca.ClientCerts["mallory"],
		s.ca.ClientKeys["mallory"],
	)
	s.Require().NoError(err)

	clientTLS := &tls.Config{
		MinVersion:         tls.VersionTLS13,
		InsecureSkipVerify: true, //nolint:gosec // bufconn target ≠ IP SAN; intentional
		Certificates:       []tls.Certificate{malloryLeaf},
	}
	cl, err := s.app.NewRawClientWithTLS(clientTLS)
	s.Require().NoError(err)
	return cl
}

// --- alice ---

// TestAliceCanAccessData verifies that alice (mTLS principal, volume granted)
// can call GetAttr on the data volume root successfully.
func (s *MTLSSuite) TestAliceCanAccessData() {
	ctx := context.Background()
	reply, err := s.app.GetClient().Fs().GetAttr(ctx, &proto.GetAttrRequest{
		Volume: s.volName,
		Path:   "",
		Caller: &proto.Caller{Owner: &proto.Owner{Uid: 1000, Gid: 1000}},
	})
	s.Require().NoError(err, "alice (mTLS) must be allowed on data volume")
	s.EqualValues(0, reply.Status, "GetAttr root must return status 0 (OK)")
}

// --- mallory ---

// TestMalloryCertDenied verifies that mallory (cert is CA-signed but not
// granted any volume) receives PermissionDenied from the ACL layer.
func (s *MTLSSuite) TestMalloryCertDenied() {
	mallory := s.buildMalloryClient()
	defer func() { _ = mallory.Close() }()

	ctx := context.Background()
	_, err := mallory.Fs().GetAttr(ctx, &proto.GetAttrRequest{
		Volume: s.volName,
		Path:   "",
		Caller: &proto.Caller{Owner: &proto.Owner{Uid: 1000, Gid: 1000}},
	})
	s.Require().Error(err, "mallory must be denied on data volume")
	st, ok := status.FromError(err)
	s.Require().True(ok, "error must be a gRPC status error")
	s.Equal(codes.PermissionDenied, st.Code(),
		"denial must be PermissionDenied, got %s: %s", st.Code(), st.Message())
}

// TestMalloryCannotUseAliceSession is the cross-user security gate:
// mallory presents her own CA-signed cert (so she passes mTLS auth and gets
// principal="mallory"), then forges alice's session_id into the request body.
// The server must return codes.PermissionDenied with the ownership message —
// proving the ownership check at resolveSession fires and not the volume ACL.
//
// Attack flow:
//  1. alice creates a session via the primary client (already done in SetupSuite).
//  2. mallory builds a raw gRPC client with her own cert.
//  3. mallory issues Mkdir with SessionId = alice's session_id in the request body.
//  4. Server: cert present → skip session fast-path → ctx principal = "mallory";
//     resolveSession: sess.Principal()="alice" ≠ "mallory" → PermissionDenied.
func (s *MTLSSuite) TestMalloryCannotUseAliceSession() {
	// Step 1: get alice's session_id from the already-established primary client.
	aliceSessionID := s.app.GetClient().SessionID()
	s.Require().NotEmpty(aliceSessionID, "alice must have an established session")

	// Step 2: build a mallory client (CA-signed cert, CN=mallory).
	mallory := s.buildMalloryClient()
	defer func() { _ = mallory.Close() }()

	ctx := context.Background()
	caller := &proto.Caller{Owner: &proto.Owner{Uid: 1000, Gid: 1000}}

	// Step 3a: metadata op — Mkdir with alice's session_id forged into request body.
	// mallory's own session_id flows in the metadata header (from mallory's session
	// handshake), but the request body carries alice's id — that is what the
	// controller's resolveSession sees.
	_, err := mallory.Fs().Mkdir(ctx, &proto.MkdirRequest{
		Volume:    s.volName,
		Path:      "/evil-mallory-dir",
		RequestId: "mallory-forge-mkdir",
		SessionId: aliceSessionID,
		Caller:    caller,
	})
	s.Require().Error(err, "mallory must not be able to use alice's session_id for Mkdir")
	st, ok := status.FromError(err)
	s.Require().True(ok)
	s.Assert().Equal(codes.PermissionDenied, st.Code(),
		"must be PermissionDenied (ownership check), not ACL denial; got %s: %s", st.Code(), st.Message())
	s.Assert().Equal("session does not belong to the caller", st.Message(),
		"error message must identify the session-ownership chokepoint")

	// Step 3b: truncate op (another session-consuming handler) — same expectation.
	_, err = mallory.Fs().Truncate(ctx, &proto.TruncateRequest{
		Volume:    s.volName,
		Path:      "/any-file",
		RequestId: "mallory-forge-truncate",
		SessionId: aliceSessionID,
		Caller:    caller,
	})
	s.Require().Error(err, "mallory must not be able to use alice's session_id for Truncate")
	st, ok = status.FromError(err)
	s.Require().True(ok)
	s.Assert().Equal(codes.PermissionDenied, st.Code())
	s.Assert().Equal("session does not belong to the caller", st.Message())
}

func TestMTLSSuite(t *testing.T) {
	suite.Run(t, new(MTLSSuite))
}
