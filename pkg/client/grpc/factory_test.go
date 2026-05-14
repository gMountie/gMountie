package grpc

import (
	"net"
	"strconv"
	"testing"
	"time"

	"gmountie/pkg/client/config"
	"gmountie/pkg/proto"
	serverConfig "gmountie/pkg/server/config"
	"gmountie/pkg/server/controller"
	"gmountie/pkg/server/service"

	"github.com/stretchr/testify/suite"
	"google.golang.org/grpc"
)

type FactoryTestSuite struct {
	suite.Suite

	// addr is the address of a real in-process gRPC server that has the
	// SessionService registered. Built once in SetupSuite. NewClientFromConfig
	// now invokes Connect() and requires a working handshake, so unit tests
	// can no longer point at a dead address.
	addr   string
	server *grpc.Server
}

func (s *FactoryTestSuite) SetupSuite() {
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	s.Require().NoError(err)

	srv := grpc.NewServer()
	sessMgr := service.NewSessionManager(service.SessionManagerOptions{})
	proto.RegisterSessionServiceServer(srv, controller.NewSessionController(sessMgr))

	go func() {
		_ = srv.Serve(lis)
	}()

	s.addr = lis.Addr().String()
	s.server = srv
}

func (s *FactoryTestSuite) TearDownSuite() {
	if s.server != nil {
		s.server.Stop()
	}
}

// hostPort splits the suite's listener address into host and integer port.
func (s *FactoryTestSuite) hostPort() (string, int) {
	host, portStr, err := net.SplitHostPort(s.addr)
	s.Require().NoError(err)
	port, err := strconv.Atoi(portStr)
	s.Require().NoError(err)
	return host, port
}

func (s *FactoryTestSuite) endpoint() string {
	host, port := s.hostPort()
	return net.JoinHostPort(host, strconv.Itoa(port))
}

func (s *FactoryTestSuite) TestNewClientFromConfig_NilConfig() {
	// Test with nil config
	client, err := NewClientFromConfig(nil)
	s.Error(err)
	s.Nil(client)

	// Test with empty config
	client, err = NewClientFromConfig(&config.Config{})
	s.Error(err)
	s.Nil(client)
}

func (s *FactoryTestSuite) TestNewClientFromConfig_NoneAuth() {
	host, port := s.hostPort()
	cfg := &config.Config{
		Server: &config.ServerConfig{
			Address: host,
			Port:    uint(port),
		},
		Auth: &serverConfig.NoneAuthConfig{},
	}

	client, err := NewClientFromConfig(cfg)
	s.Require().NoError(err)
	s.Require().NotNil(client)
	defer client.Close()
	s.Equal(s.endpoint(), client.GetEndpoint())
	s.NotEmpty(client.SessionID(), "factory must establish a session")
}

func (s *FactoryTestSuite) TestNewClientFromConfig_BasicAuth() {
	host, port := s.hostPort()
	cfg := &config.Config{
		Server: &config.ServerConfig{
			Address: host,
			Port:    uint(port),
		},
		Auth: &config.BasicAuthConfig{
			BasicAuthConfigUser: serverConfig.BasicAuthConfigUser{
				Username: "testuser",
				Password: "testpass",
			},
		},
	}

	client, err := NewClientFromConfig(cfg)
	s.Require().NoError(err)
	s.Require().NotNil(client)
	defer client.Close()
	s.Equal(s.endpoint(), client.GetEndpoint())
	s.NotEmpty(client.SessionID(), "factory must establish a session")
}

func (s *FactoryTestSuite) TestCreateEndpoint() {
	cfg := &config.ServerConfig{
		Address: "localhost",
		Port:    9449,
	}

	endpoint := createEndpoint(cfg)
	s.Equal("localhost:9449", endpoint)
}

// TestNewClientFromConfig_TimeoutsApplied verifies the configured RPC
// timeouts are propagated onto the constructed client.
func (s *FactoryTestSuite) TestNewClientFromConfig_TimeoutsApplied() {
	host, port := s.hostPort()
	cfg := &config.Config{
		Server: &config.ServerConfig{Address: host, Port: uint(port)},
		Auth:   &serverConfig.NoneAuthConfig{},
		Rpc:    &config.RpcConfig{TimeoutMeta: 2 * time.Second, TimeoutIO: 90 * time.Second},
	}

	c, err := NewClientFromConfig(cfg)
	s.Require().NoError(err)
	defer c.Close()

	s.Assert().Equal(2*time.Second, c.MetaTimeout())
	s.Assert().Equal(90*time.Second, c.IOTimeout())
}

// TestNewClientFromConfig_HandshakeFailureReturnsError verifies that when the
// session handshake fails (no server at the address), the factory returns an
// error and a nil client rather than handing back a half-built client.
func (s *FactoryTestSuite) TestNewClientFromConfig_HandshakeFailureReturnsError() {
	// Bind a port then release it — high chance nothing is listening there.
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	s.Require().NoError(err)
	port := lis.Addr().(*net.TCPAddr).Port
	lis.Close()

	cfg := &config.Config{
		Server: &config.ServerConfig{Address: "127.0.0.1", Port: uint(port)},
		Auth:   &serverConfig.NoneAuthConfig{},
	}

	client, err := NewClientFromConfig(cfg)
	s.Error(err)
	s.Nil(client)
}

func TestFactoryTestSuite(t *testing.T) {
	suite.Run(t, new(FactoryTestSuite))
}
