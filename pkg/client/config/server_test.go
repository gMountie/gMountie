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
auth:
  type: basic
  username: admin
  password: admin
`
	result, err := LoadConfigFromString(conf)
	s.Require().NoError(err)
	s.Assert().Equal("0.0.0.0", result.Server.Address)
	s.Assert().Equal(uint(9449), result.Server.Port)
	// TLS defaults: empty Verify is treated as "verify" by client/tls.BuildConfig.
	s.Assert().Empty(result.Server.TLS.Verify)
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
	s.Assert().Empty(result.Server.TLS.Verify)       // empty == "verify" at dial time
}

// A hostname (not just an IP) is a valid server address — the client dials
// host:port via DNS and derives the TLS SNI from the host.
func (s *ServerConfigTestSuite) TestParse_HostnameServerAddress() {
	conf := `
server:
  address: gmountie.home.buluba.net
  port: 443
auth:
  type: basic
  username: admin
  password: admin
`
	result, err := LoadConfigFromString(conf)
	s.Require().NoError(err)
	s.Assert().Equal("gmountie.home.buluba.net", result.Server.Address)
}

// Test validation cases
func (s *ServerConfigTestSuite) TestParse_InvalidServerAddress() {
	// Neither a valid hostname nor an IP (contains a space and '!').
	conf := `
server:
  address: "bad host!"
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

// TestParse_TLSStructFields verifies the TLSConfig struct (Phase 7 PR 1)
// parses each field cleanly. The old `tls: true|false` toggle is gone —
// TLS is always on at the wire layer; the struct controls how the client
// verifies the server.
func (s *ServerConfigTestSuite) TestParse_TLSStructFields() {
	conf := `
server:
  address: 0.0.0.0
  port: 9449
  tls:
    verify: tofu
    expected_fingerprint: SHA256:abcdefghijklmnopqrstuvwxyz0123456789ABCDEFGH
    ca_file: /etc/gmountie/ca.crt
    server_name: gmountie.example.com
auth:
  type: basic
  username: admin
  password: admin
`
	result, err := LoadConfigFromString(conf)
	s.Require().NoError(err)
	s.Assert().Equal("tofu", result.Server.TLS.Verify)
	s.Assert().Equal("SHA256:abcdefghijklmnopqrstuvwxyz0123456789ABCDEFGH", result.Server.TLS.ExpectedFingerprint)
	s.Assert().Equal("/etc/gmountie/ca.crt", result.Server.TLS.CAFile)
	s.Assert().Equal("gmountie.example.com", result.Server.TLS.ServerName)
}

func TestServerConfigTestSuite(t *testing.T) {
	suite.Run(t, new(ServerConfigTestSuite))
}
