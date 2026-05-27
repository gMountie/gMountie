package config

import (
	"testing"

	"github.com/stretchr/testify/suite"
)

type ServerConfigTestSuite struct {
	suite.Suite
}

// Test successful parsing of server configuration
func (s *ServerConfigTestSuite) TestParse_FullServer() {
	conf := `
server:
  address: 0.0.0.0
  port: 9449
  tls: false
auth:
  type: basic
  username: admin
  password: admin
`
	result, err := LoadConfigFromString(conf)
	s.Require().NoError(err)
	s.Assert().Equal("0.0.0.0", result.Server.Address)
	s.Assert().Equal(uint(9449), result.Server.Port)
	s.Assert().False(result.Server.TLS)
}

// Test default values
func (s *ServerConfigTestSuite) TestParse_Defaults() {
	conf := `
server:
  address: 127.0.0.1
auth:
  type: basic
  username: admin
  password: admin
`
	result, err := LoadConfigFromString(conf)
	s.Require().NoError(err)
	s.Assert().Equal("127.0.0.1", result.Server.Address)
	s.Assert().Equal(uint(9449), result.Server.Port) // Should use default port
	s.Assert().False(result.Server.TLS)              // Should default to false
}

// Test validation cases
func (s *ServerConfigTestSuite) TestParse_InvalidServerAddress() {
	conf := `
server:
  address: not-an-ip
  port: 9449
auth:
  type: basic
  username: admin
  password: admin
`
	_, err := LoadConfigFromString(conf)
	s.Require().Error(err)
}

func (s *ServerConfigTestSuite) TestParse_InvalidServerPort() {
	conf := `
server:
  address: 0.0.0.0
  port: -1
auth:
  type: basic
  username: admin
  password: admin
`
	_, err := LoadConfigFromString(conf)
	s.Require().Error(err)
}

func (s *ServerConfigTestSuite) TestParse_MissingServerSection() {
	conf := `
someother:
  key: value
auth:
  type: basic
  username: admin
  password: admin
`
	_, err := LoadConfigFromString(conf)
	s.Require().Error(err)
}

// Test TLS specific cases
func (s *ServerConfigTestSuite) TestParse_WithTLS() {
	conf := `
server:
  address: 0.0.0.0
  port: 9449
  tls: true
auth:
  type: basic
  username: admin
  password: admin
`
	result, err := LoadConfigFromString(conf)
	s.Require().NoError(err)
	s.Assert().True(result.Server.TLS)
}

func TestServerConfigTestSuite(t *testing.T) {
	suite.Run(t, new(ServerConfigTestSuite))
}
