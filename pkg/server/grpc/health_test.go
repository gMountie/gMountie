package grpc

import (
	"context"
	"net"
	"testing"

	"github.com/stretchr/testify/suite"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/test/bufconn"
)

type HealthServiceTestSuite struct {
	suite.Suite
	listener *bufconn.Listener
	server   *grpc.Server
	client   healthpb.HealthClient
	conn     *grpc.ClientConn
	health   *HealthService
}

func (s *HealthServiceTestSuite) SetupTest() {
	s.listener = bufconn.Listen(1024 * 1024)
	s.server = grpc.NewServer()
	s.health = NewHealthService()
	s.health.Register(s.server)
	go func() { _ = s.server.Serve(s.listener) }()

	conn, err := grpc.NewClient("passthrough://bufnet",
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
			return s.listener.Dial()
		}),
	)
	s.Require().NoError(err)
	s.conn = conn
	s.client = healthpb.NewHealthClient(conn)
}

func (s *HealthServiceTestSuite) TearDownTest() {
	_ = s.conn.Close()
	s.server.Stop()
}

func (s *HealthServiceTestSuite) TestReportsServingByDefault() {
	resp, err := s.client.Check(context.Background(), &healthpb.HealthCheckRequest{})
	s.Require().NoError(err)
	s.Assert().Equal(healthpb.HealthCheckResponse_SERVING, resp.Status)
}

func (s *HealthServiceTestSuite) TestSetNotServingChangesStatus() {
	s.health.SetNotServing()
	resp, err := s.client.Check(context.Background(), &healthpb.HealthCheckRequest{})
	s.Require().NoError(err)
	s.Assert().Equal(healthpb.HealthCheckResponse_NOT_SERVING, resp.Status)
}

func TestHealthServiceTestSuite(t *testing.T) { suite.Run(t, new(HealthServiceTestSuite)) }
