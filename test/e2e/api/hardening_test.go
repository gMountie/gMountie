package api

import (
	"context"
	"net"
	"testing"
	"time"

	"gmountie/pkg/common/passhash"
	"gmountie/pkg/server"
	"gmountie/pkg/server/config"
	grpcServer "gmountie/pkg/server/grpc"

	"github.com/stretchr/testify/suite"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	grpcReflection "google.golang.org/grpc/reflection/grpc_reflection_v1"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
)

// mustHash hashes s with argon2id and panics on error.
// Used in hardening test fixtures where a valid PHC hash is needed.
func mustHash(t *testing.T, s string) string {
	t.Helper()
	h, err := passhash.Hash(s)
	if err != nil {
		t.Fatalf("passhash.Hash(%q): %v", s, err)
	}
	return h
}

type HardeningTestSuite struct {
	suite.Suite
}

func TestHardeningTestSuite(t *testing.T) { suite.Run(t, new(HardeningTestSuite)) }

// TestServeRejectsCleartextPassword: covered at unit level by
// pkg/server/config.BasicAuthConfigTestSuite.TestRejectsCleartextPasswordHash
// and wired through LoadConfigFromString in config_test.go.
// The config parse path is the gate — there is no separate wire-level
// startup path that is not already exercised by those unit tests.
// TODO: if a new early-abort path is added between config parse and grpc.Serve,
// add an e2e case here exercising it via server.Start().

// TestServeRejectsOpsAuthNoneOnNonLoopback: the AppOpsSuite unit test already
// covers validateOpsConfig in isolation. The integration version of this
// check would call server.Start, which builds the AppContext (SessionManager
// + EventBus) BEFORE the validation runs — those goroutines outlive the
// Start error return and accumulate across runs, leading to the 18-minute
// CI hang observed on PR #54's first push. Stub at the validator boundary
// instead until Start grows a "validate-first, build-later" reorganization.
//
// Coverage:
//   - pkg/server/app_ops_test.go AppOpsSuite.Test_NonLoopbackWithoutAuthRejected
//     exercises the same validateOpsConfig path.

// TestReflectionDisabledByDefault verifies that a server built through the full
// NewServerAppContext + grpc.NewServer path (the production code path) does NOT
// register the gRPC reflection service when server.grpc.reflection is false
// (the default).  The unit-level ReflectionGateSuite.TestReflectionOffByDefault
// exercises a minimal stub server; this test exercises the full service
// registration path (GetGrpcServices, auth interceptors, real config defaults)
// to confirm no service accidentally calls reflection.Register().
func (s *HardeningTestSuite) TestReflectionDisabledByDefault() {
	// Build a minimal but real server config — reflection defaults to false.
	cfg := &config.Config{
		Server: &config.ServerConfig{
			Address:                    "127.0.0.1",
			Port:                       0,
			Metrics:                    false,
			MetricsAddr:                "127.0.0.1:0",
			FrameSizeBytes:             config.DefaultFrameSizeBytes,
			CompoundMaxParallel:        config.DefaultCompoundMaxParallel,
			MaxMessageBytes:            config.DefaultMaxMessageBytes,
			SubscribeBufferSize:        config.DefaultServerSubscribeBufferSize,
			SubscribeHeartbeatInterval: config.DefaultServerSubscribeHeartbeatInterval,
			Keepalive: config.ServerKeepaliveConfig{
				Time:                config.DefaultKeepaliveTime,
				Timeout:             config.DefaultKeepaliveTimeout,
				MinTime:             config.DefaultKeepaliveMinTime,
				PermitWithoutStream: config.DefaultKeepalivePermitWithoutStream,
			},
			GRPC: config.GRPCConfig{Reflection: false}, // explicit default
		},
		Auth: &config.BasicAuthConfig{
			AuthConfigBase: config.AuthConfigBase{Type: config.AuthConfigTypeBasic},
			Users: []config.BasicAuthConfigUser{
				{Username: "test", PasswordHash: mustHash(s.T(), "test")},
			},
		},
		Volumes: []*config.VolumeConfig{},
	}

	appCtx, err := server.NewServerAppContext(cfg)
	s.Require().NoError(err)
	// Tear down the goroutines AppContext owns (SessionManager + EventBus
	// heartbeat). Otherwise they accumulate across test runs and the test
	// job times out — observed on PR #54's first push.
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = appCtx.SessionManager.Stop(ctx)
		appCtx.Bus.Close()
	}()

	// Wire a bufconn listener so nothing binds on a real port.
	lis := bufconn.Listen(1024 * 1024)

	srv := grpcServer.NewServer(
		cfg,
		appCtx.AuthService,
		appCtx.GetGrpcServices(),
		grpcServer.WithListener(lis),
		// No TLS: plaintext bufconn is fine for a reflection-gate test.
	)
	go func() { _ = srv.Serve() }()
	defer srv.Stop(false)

	// Dial the bufconn without TLS.
	conn, err := grpc.NewClient(
		"passthrough://bufnet",
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
			return lis.Dial()
		}),
	)
	s.Require().NoError(err)
	defer conn.Close() //nolint:errcheck

	rc := grpcReflection.NewServerReflectionClient(conn)
	stream, err := rc.ServerReflectionInfo(context.Background())
	s.Require().NoError(err)

	err = stream.Send(&grpcReflection.ServerReflectionRequest{
		MessageRequest: &grpcReflection.ServerReflectionRequest_ListServices{ListServices: ""},
	})
	s.Require().NoError(err)

	_, err = stream.Recv()
	s.Require().Error(err)
	st, ok := status.FromError(err)
	s.Require().True(ok)
	s.Equal(codes.Unimplemented, st.Code(),
		"gRPC reflection must be Unimplemented when server.grpc.reflection=false (default)")
}
