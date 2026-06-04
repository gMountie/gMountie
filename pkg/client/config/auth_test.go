package config

import (
	"testing"

	serverConfig "go.gmountie.dev/gmountie/pkg/server/config"

	"github.com/stretchr/testify/suite"
)

type AuthConfigTestSuite struct {
	suite.Suite
}

// Test that "none" auth type is rejected now that auth is mandatory
func (s *AuthConfigTestSuite) TestParse_NoneAuth_Rejected() {
	conf := `
server:
  address: 127.0.0.1
auth:
  type: none
`
	_, err := LoadConfigFromString(conf)
	s.Require().Error(err)
}

// Test successful parsing of basic auth configuration
func (s *AuthConfigTestSuite) TestParse_BasicAuth() {
	conf := `
server:
  address: 127.0.0.1
auth:
  type: basic
  username: testuser
  password: testpass
`
	result, err := LoadConfigFromString(conf)
	s.Require().NoError(err)
	s.Assert().Equal(serverConfig.AuthConfigTypeBasic, result.Auth.GetType())

	basicAuth, ok := result.Auth.(*BasicAuthConfig)
	s.Require().True(ok)
	s.Assert().Equal("testuser", basicAuth.Username)
	s.Assert().Equal("testpass", basicAuth.Password)
	test, err := result.String()
	s.Require().NoError(err)
	s.Assert().Contains(test, "testuser")
}

// TestParse_MTLSAuth ensures type:mtls parses with no username/password — the
// client certificate is the identity — and yields the mtls auth type.
func (s *AuthConfigTestSuite) TestParse_MTLSAuth() {
	conf := `
server:
  address: 127.0.0.1
auth:
  type: mtls
`
	result, err := LoadConfigFromString(conf)
	s.Require().NoError(err)
	s.Require().NotNil(result.Auth)
	s.Assert().Equal(serverConfig.AuthConfigTypeMTLS, result.Auth.GetType())
	// mtls auth must NOT be a BasicAuthConfig, so the grpc factory skips
	// attaching per-RPC basic-auth credentials.
	_, ok := result.Auth.(*BasicAuthConfig)
	s.Assert().False(ok)
}

// Test error cases for invalid auth type
func (s *AuthConfigTestSuite) TestParse_InvalidAuthType() {
	conf := `
server:
  address: 127.0.0.1
auth:
  type: invalid
`
	_, err := LoadConfigFromString(conf)
	s.Require().Error(err)
	s.Assert().Contains(err.Error(), "invalid auth type")
}

// TestParse_MissingUsername ensures username remains required at parse time.
func (s *AuthConfigTestSuite) TestParse_MissingUsername() {
	conf := `
server:
  address: 127.0.0.1
auth:
  type: basic
  password: testpass
`
	_, err := LoadConfigFromString(conf)
	s.Require().Error(err)
}

// TestParse_MissingPassword ensures a password-less basic-auth config is valid
// at parse time. The password is resolved at mount time by resolveAuth
// (password_command / password_file / env / interactive prompt), so the inline
// password field is optional in the config file.
func (s *AuthConfigTestSuite) TestParse_MissingPassword() {
	conf := `
server:
  address: 127.0.0.1
auth:
  type: basic
  username: testuser
`
	cfg, err := LoadConfigFromString(conf)
	s.Require().NoError(err)
	basic, ok := cfg.Auth.(*BasicAuthConfig)
	s.Require().True(ok)
	s.Equal("testuser", basic.Username)
	s.Empty(basic.Password) // resolved at mount time by resolveAuth
}

// Test validation of username/password requirements
func (s *AuthConfigTestSuite) TestParse_EmptyCredentials() {
	conf := `
server:
  address: 127.0.0.1
auth:
  type: basic
  username: ""
  password: ""
`
	_, err := LoadConfigFromString(conf)
	s.Require().Error(err)
}

// Test that GetType() returns correct auth type
func (s *AuthConfigTestSuite) TestGetType() {
	basicAuth := &BasicAuthConfig{
		BasicAuthConfigUser: BasicAuthConfigUser{
			Username: "test",
			Password: "test",
		},
	}
	s.Assert().Equal(serverConfig.AuthConfigTypeBasic, basicAuth.GetType())
}

// Test integration with the main config parser
func (s *AuthConfigTestSuite) TestIntegration_WithServerConfig() {
	conf := `
server:
  address: 127.0.0.1
  port: 9449
auth:
  type: basic
  username: testuser
  password: testpass
`
	result, err := LoadConfigFromString(conf)
	s.Require().NoError(err)
	s.Assert().NotNil(result.Server)
	s.Assert().NotNil(result.Auth)
	s.Assert().Equal(serverConfig.AuthConfigTypeBasic, result.Auth.GetType())
}

var minimalServerConfig = `
server:
  address: 127.0.0.1
auth:
  type: basic
  username: admin
  password: admin
`

func TestAuthConfigTestSuite(t *testing.T) {
	suite.Run(t, new(AuthConfigTestSuite))
}
