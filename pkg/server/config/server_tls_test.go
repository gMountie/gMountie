package config

import (
	"testing"

	"github.com/stretchr/testify/suite"
)

type ServerTLSConfigSuite struct {
	suite.Suite
}

// minimalServerYAML wraps a tls block into a valid full config string.
func minimalServerYAML(tlsBlock string) string {
	return `
server:
  address: "0.0.0.0"
  port: 9449
` + tlsBlock + `
auth:
  type: basic
  users:
  - username: admin
    password_hash: ` + testAdminPHC + `
volumes:
  - name: test
    path: /tmp
`
}

func (s *ServerTLSConfigSuite) TestParsesTLSBlock() {
	cfg, err := LoadConfigFromString(minimalServerYAML(`  tls:
    cert_file: /tmp/c.crt
    key_file: /tmp/c.key
    min_version: "1.3"
`))
	s.Require().NoError(err)
	s.Assert().Equal("/tmp/c.crt", cfg.Server.TLS.CertFile)
	s.Assert().Equal("/tmp/c.key", cfg.Server.TLS.KeyFile)
	s.Assert().Equal("1.3", cfg.Server.TLS.MinVersion)
	s.Assert().False(cfg.Server.TLS.Disabled)
}

func (s *ServerTLSConfigSuite) TestTLSDisabledFlag() {
	cfg, err := LoadConfigFromString(minimalServerYAML(`  tls:
    disabled: true
`))
	s.Require().NoError(err)
	s.Assert().True(cfg.Server.TLS.Disabled)
}

func (s *ServerTLSConfigSuite) TestTLSMinVersion12() {
	cfg, err := LoadConfigFromString(minimalServerYAML(`  tls:
    min_version: "1.2"
`))
	s.Require().NoError(err)
	s.Assert().Equal("1.2", cfg.Server.TLS.MinVersion)
}

func (s *ServerTLSConfigSuite) TestTLSMinVersionDefault() {
	// No tls block at all — default should be "1.3" from SetDefault.
	cfg, err := LoadConfigFromString(minimalServerYAML(""))
	s.Require().NoError(err)
	s.Assert().Equal("1.3", cfg.Server.TLS.MinVersion)
}

func (s *ServerTLSConfigSuite) TestTLSMinVersionInvalid() {
	// "1.1" is not in the oneof=1.2 1.3 set and must fail validation.
	_, err := LoadConfigFromString(minimalServerYAML(`  tls:
    min_version: "1.1"
`))
	s.Require().Error(err)
}

func (s *ServerTLSConfigSuite) TestTLSClientCAFile() {
	cfg, err := LoadConfigFromString(minimalServerYAML(`  tls:
    client_ca_file: /etc/ssl/ca.crt
`))
	s.Require().NoError(err)
	s.Assert().Equal("/etc/ssl/ca.crt", cfg.Server.TLS.ClientCAFile)
}

func (s *ServerTLSConfigSuite) TestTLSEnvOverride() {
	s.T().Setenv("GMOUNTIE_SERVER_TLS_CERT_FILE", "/env/cert.crt")
	s.T().Setenv("GMOUNTIE_SERVER_TLS_KEY_FILE", "/env/cert.key")
	cfg, err := LoadConfigFromString(minimalServerYAML(""))
	s.Require().NoError(err)
	s.Assert().Equal("/env/cert.crt", cfg.Server.TLS.CertFile)
	s.Assert().Equal("/env/cert.key", cfg.Server.TLS.KeyFile)
}

func TestServerTLSConfigSuite(t *testing.T) {
	suite.Run(t, new(ServerTLSConfigSuite))
}
