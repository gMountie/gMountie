package grpc

import (
	"gmountie/pkg/client/config"
	serverConfig "gmountie/pkg/server/config"
	"testing"
	"time"

	"github.com/stretchr/testify/suite"
)

type FactoryTestSuite struct {
	suite.Suite
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
	cfg := &config.Config{
		Server: &config.ServerConfig{
			Address: "localhost",
			Port:    9449,
		},
		Auth: &serverConfig.NoneAuthConfig{},
	}

	client, err := NewClientFromConfig(cfg)
	s.NoError(err)
	s.NotNil(client)
	s.Equal("localhost:9449", client.GetEndpoint())
}

func (s *FactoryTestSuite) TestNewClientFromConfig_BasicAuth() {
	cfg := &config.Config{
		Server: &config.ServerConfig{
			Address: "localhost",
			Port:    9449,
		},
		Auth: &config.BasicAuthConfig{
			BasicAuthConfigUser: serverConfig.BasicAuthConfigUser{
				Username: "testuser",
				Password: "testpass",
			},
		},
	}

	client, err := NewClientFromConfig(cfg)
	s.NoError(err)
	s.NotNil(client)
	s.Equal("localhost:9449", client.GetEndpoint())
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
	cfg := &config.Config{
		Server: &config.ServerConfig{Address: "127.0.0.1", Port: 9449},
		Auth:   &serverConfig.NoneAuthConfig{},
		Rpc:    &config.RpcConfig{TimeoutMeta: 2 * time.Second, TimeoutIO: 90 * time.Second},
	}

	c, err := NewClientFromConfig(cfg)
	s.Require().NoError(err)
	defer c.Close()

	s.Assert().Equal(2*time.Second, c.MetaTimeout())
	s.Assert().Equal(90*time.Second, c.IOTimeout())
}

func TestFactoryTestSuite(t *testing.T) {
	suite.Run(t, new(FactoryTestSuite))
}
