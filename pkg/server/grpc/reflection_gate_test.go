package grpc

import (
	"context"
	"net"
	"testing"

	"go.gmountie.dev/gmountie/pkg/server/config"
	"go.gmountie.dev/gmountie/pkg/server/service"

	"github.com/stretchr/testify/suite"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/reflection"
	grpcReflection "google.golang.org/grpc/reflection/grpc_reflection_v1"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
)

// allowAllAuth is a minimal AuthService that permits every request.
type allowAllAuth struct{}

func (allowAllAuth) ReloadUsers(config.AuthConfig) {}

func (allowAllAuth) Authorize(_ context.Context, _ string) (*service.UserDetails, error) {
	return &service.UserDetails{Username: "test"}, nil
}

type ReflectionGateSuite struct {
	suite.Suite
}

// buildServer starts a minimal gRPC server with reflection toggled by reflectionOn.
// Returns the raw *grpc.Server (for Stop) and the bufconn listener.
func (s *ReflectionGateSuite) buildServer(reflectionOn bool) (*grpc.Server, *bufconn.Listener) {
	cfg := &config.Config{
		Server: &config.ServerConfig{
			Address: "127.0.0.1",
			Port:    0,
			GRPC:    config.GRPCConfig{Reflection: reflectionOn},
		},
	}

	lis := bufconn.Listen(1024 * 1024)

	// Build the Server struct and use getOptions() so the reflection gate
	// runs through the same code path as production.
	srv := &Server{
		config:        cfg,
		authService:   allowAllAuth{},
		HealthService: NewHealthService(),
		listener:      lis,
	}

	rawGRPC := grpc.NewServer(srv.getOptions()...)
	srv.HealthService.Register(rawGRPC)
	if reflectionOn {
		reflection.Register(rawGRPC)
	}
	go func() { _ = rawGRPC.Serve(lis) }()
	return rawGRPC, lis
}

// dial opens a client connection to the bufconn listener.
func (s *ReflectionGateSuite) dial(lis *bufconn.Listener) *grpc.ClientConn {
	conn, err := grpc.NewClient("passthrough://bufnet",
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
			return lis.Dial()
		}),
	)
	s.Require().NoError(err)
	return conn
}

func (s *ReflectionGateSuite) TestReflectionOffByDefault() {
	srv, lis := s.buildServer(false)
	defer srv.Stop()
	conn := s.dial(lis)
	defer conn.Close()

	rc := grpcReflection.NewServerReflectionClient(conn)
	stream, err := rc.ServerReflectionInfo(context.Background())
	s.Require().NoError(err)

	if err := stream.Send(&grpcReflection.ServerReflectionRequest{
		MessageRequest: &grpcReflection.ServerReflectionRequest_ListServices{ListServices: ""},
	}); err != nil {
		// Send can race the server's Serve goroutine startup under -race;
		// either way the stream is observable via Recv, which is the
		// assertion that matters. Surface the Send error as a Recv error.
		_, err = stream.Recv()
		s.Require().Error(err)
		st, ok := status.FromError(err)
		if ok && st.Code() == codes.Unimplemented {
			return
		}
		s.FailNowf("Send race", "Send failed but Recv didn't surface the expected Unimplemented: %v", err)
	}

	_, err = stream.Recv()
	s.Require().Error(err)
	st, ok := status.FromError(err)
	s.Require().True(ok)
	s.Equal(codes.Unimplemented, st.Code())
}

func (s *ReflectionGateSuite) TestReflectionOnYieldsServices() {
	srv, lis := s.buildServer(true)
	defer srv.Stop()
	conn := s.dial(lis)
	defer conn.Close()

	rc := grpcReflection.NewServerReflectionClient(conn)
	stream, err := rc.ServerReflectionInfo(context.Background())
	s.Require().NoError(err)

	err = stream.Send(&grpcReflection.ServerReflectionRequest{
		MessageRequest: &grpcReflection.ServerReflectionRequest_ListServices{ListServices: ""},
	})
	s.Require().NoError(err)

	resp, err := stream.Recv()
	s.Require().NoError(err)
	s.NotNil(resp.GetListServicesResponse())
	s.NotEmpty(resp.GetListServicesResponse().GetService())
}

func TestReflectionGateSuite(t *testing.T) {
	suite.Run(t, new(ReflectionGateSuite))
}
