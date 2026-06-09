package config

import (
	"testing"
	"time"

	"go.gmountie.dev/gmountie/pkg/common/config"

	"github.com/stretchr/testify/suite"
)

// testAdminPHC is a pre-generated argon2id PHC string used in YAML fixtures
// throughout this file. It hashes the string "admin" with a fixed all-zero
// salt so it is deterministic and passes IsHashed without requiring Hash().
const testAdminPHC = "$argon2id$v=19$m=65536,t=3,p=4$AAAAAAAAAAAAAAAAAAAAAA$5u/nk9QnWzMnHQ7LHnzNBwZmJvH2tXG7dBbJGT7C7Ho"

type ConfigTestSuite struct {
	suite.Suite
	fullConf string
}

func (s *ConfigTestSuite) SetupTest() {
	s.fullConf = `
server:
  metrics: true
  address: 0.0.0.0
  port: 8000
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

// Tests
func (s *ConfigTestSuite) TestParse_Full_Server() {
	// Test.
	result, err := LoadConfigFromString(s.fullConf)

	// Verify.
	s.Require().NoError(err)
	s.Assert().Equal("0.0.0.0", result.Server.Address)
	s.Assert().Equal(uint(8000), result.Server.Port)
	s.Assert().Equal(config.AuthConfigTypeBasic, result.Auth.GetType())
	s.Assert().True(result.Server.Metrics)
}

func (s *ConfigTestSuite) TestParse_Full_Volumes() {
	// Test.
	result, err := LoadConfigFromString(s.fullConf)

	// Verify.
	s.Require().NoError(err)
	s.Assert().Len(result.Volumes, 1)
	s.Assert().Equal("test", result.Volumes[0].Name)
	s.Assert().Equal("/tmp", result.Volumes[0].Path)
}

// TestParse_BasicAuthConfig
func (s *ConfigTestSuite) TestParse_BasicAuthConfig() {
	// Pre-generated argon2id PHC for "test" (salt AAAAAAAAAAAAAAAAAAAAAA).
	const testPHC = "$argon2id$v=19$m=65536,t=3,p=4$AAAAAAAAAAAAAAAAAAAAAA$5u/nk9QnWzMnHQ7LHnzNBwZmJvH2tXG7dBbJGT7C7Ho"

	// Setup.
	conf := `
server:
  address: 0.0.0.0
  port: 8000

auth:
  type: basic
  users:
  - username: test
    password_hash: ` + testPHC + `
volumes:
  - name: test
    path: /tmp
`
	// Test.
	result, err := LoadConfigFromString(conf)

	// Verify.
	s.Require().NoError(err)
	s.Assert().Equal(config.AuthConfigTypeBasic, result.Auth.GetType())
	s.Assert().Len(result.Auth.(*BasicAuthConfig).Users, 1)
	s.Assert().Equal("test", result.Auth.(*BasicAuthConfig).Users[0].Username)
	s.Assert().Equal(testPHC, result.Auth.(*BasicAuthConfig).Users[0].PasswordHash)
}

// TestParse_BasicAuthConfig_Invalid
func (s *ConfigTestSuite) TestParse_BasicAuthConfig_Invalid() {
	// Setup.
	conf := `
server:
  address: 0.0.0.0
  port: 8000

auth:
  type: basic
  users:
  - username: nopassword
volumes:
  - name: test
    path: /tmp
`
	// Test.
	_, err := LoadConfigFromString(conf)

	// Verify.
	s.Require().Error(err)
}

// Test validation cases
func (s *ConfigTestSuite) TestParse_EmptyConfig() {
	conf := ``
	_, err := LoadConfigFromString(conf)
	s.Require().Error(err)
}

func (s *ConfigTestSuite) TestParse_InvalidServerAddress() {
	conf := `
server:
	address: not-an-ip
	port: 8000
auth:
	type: none
volumes:
	- name: test
	  path: /tmp
`
	_, err := LoadConfigFromString(conf)
	s.Require().Error(err)
}

// TestParse_FriendlyValidationMessage verifies a validation failure surfaces
// as a human-readable "config error" with the snake_case key path and an
// actionable hint, not raw validator.ValidationErrors noise.
func (s *ConfigTestSuite) TestParse_FriendlyValidationMessage() {
	conf := `
server:
  address: localhost
  port: 8000
auth:
  type: basic
  users:
  - username: admin
    password_hash: ` + testAdminPHC + `
volumes:
  - name: test
    path: /tmp
`
	_, err := LoadConfigFromString(conf)
	s.Require().Error(err)
	s.Contains(err.Error(), "config error")
	s.Contains(err.Error(), "server.address")
	s.Contains(err.Error(), "IP")
	s.NotContains(err.Error(), "Field validation for")
}

func (s *ConfigTestSuite) TestParse_InvalidServerPort() {
	conf := `
server:
	address: 0.0.0.0
	port: -1
auth:
	type: none
volumes:
	- name: test
	  path: /tmp
`
	_, err := LoadConfigFromString(conf)
	s.Require().Error(err)
}

// Test environment variable override
func (s *ConfigTestSuite) TestParse_EnvVarOverride() {
	// Setup environment
	s.T().Setenv("GMOUNTIE_SERVER_PORT", "9000")

	result, err := LoadConfigFromString(s.fullConf)
	s.Require().NoError(err)
	s.Assert().Equal(uint(9000), result.Server.Port)
}

// Test duplicate volume names
func (s *ConfigTestSuite) TestParse_DuplicateVolumeNames() {
	conf := `
server:
	address: 0.0.0.0
	port: 8000
auth:
	type: none
volumes:
	- name: test
	  path: /tmp
	- name: test
	  path: /var
`
	_, err := LoadConfigFromString(conf)
	s.Require().Error(err)
}

// Test auth specific cases
func (s *ConfigTestSuite) TestParse_InvalidAuthType() {
	conf := `
server:
	address: 0.0.0.0
	port: 8000
auth:
	type: invalid
volumes:
	- name: test
	  path: /tmp
`
	_, err := LoadConfigFromString(conf)
	s.Require().Error(err)
}

// Test Runner
func (s *ConfigTestSuite) TestKeepaliveDefaults() {
	cfg, err := LoadConfigFromString(`
server:
  address: "0.0.0.0"
  port: 9449
auth:
  type: basic
  users:
  - username: admin
    password_hash: ` + testAdminPHC + `
volumes:
  - name: test
    path: /tmp
`)
	s.Require().NoError(err)
	s.Assert().Equal(DefaultKeepaliveTime, cfg.Server.Keepalive.Time)
	s.Assert().Equal(DefaultKeepaliveTimeout, cfg.Server.Keepalive.Timeout)
	s.Assert().Equal(DefaultKeepaliveMinTime, cfg.Server.Keepalive.MinTime)
	s.Assert().Equal(DefaultKeepalivePermitWithoutStream, cfg.Server.Keepalive.PermitWithoutStream)
}

func (s *ConfigTestSuite) TestKeepaliveEnvOverride() {
	s.T().Setenv("GMOUNTIE_SERVER_KEEPALIVE_TIME", "45s")
	s.T().Setenv("GMOUNTIE_SERVER_KEEPALIVE_TIMEOUT", "15s")
	cfg, err := LoadConfigFromString(`
server:
  address: "0.0.0.0"
  port: 9449
auth:
  type: basic
  users:
  - username: admin
    password_hash: ` + testAdminPHC + `
volumes:
  - name: test
    path: /tmp
`)
	s.Require().NoError(err)
	s.Assert().Equal(45*time.Second, cfg.Server.Keepalive.Time)
	s.Assert().Equal(15*time.Second, cfg.Server.Keepalive.Timeout)
}

func (s *ConfigTestSuite) TestKeepaliveExplicitYAML() {
	cfg, err := LoadConfigFromString(`
server:
  address: "0.0.0.0"
  port: 9449
  keepalive:
    time: 1m
    timeout: 20s
    min_time: 5s
    permit_without_stream: false
auth:
  type: basic
  users:
  - username: admin
    password_hash: ` + testAdminPHC + `
volumes:
  - name: test
    path: /tmp
`)
	s.Require().NoError(err)
	s.Assert().Equal(1*time.Minute, cfg.Server.Keepalive.Time)
	s.Assert().Equal(20*time.Second, cfg.Server.Keepalive.Timeout)
	s.Assert().Equal(5*time.Second, cfg.Server.Keepalive.MinTime)
	s.Assert().False(cfg.Server.Keepalive.PermitWithoutStream)
}

func (s *ConfigTestSuite) TestSubscribeDefaultsFromConfig() {
	cfg, err := LoadConfigFromString(`
server:
  address: "0.0.0.0"
  port: 9449
auth:
  type: basic
  users:
  - username: admin
    password_hash: ` + testAdminPHC + `
volumes:
  - name: test
    path: /tmp
`)
	s.Require().NoError(err)
	s.Assert().Equal(DefaultServerSubscribeBufferSize, cfg.Server.SubscribeBufferSize)
	s.Assert().Equal(DefaultServerSubscribeHeartbeatInterval, cfg.Server.SubscribeHeartbeatInterval)
}

func (s *ConfigTestSuite) TestSubscribeOverrideYAML() {
	cfg, err := LoadConfigFromString(`
server:
  address: "0.0.0.0"
  port: 9449
  subscribe_buffer_size: 512
  subscribe_heartbeat_interval: 30s
auth:
  type: basic
  users:
  - username: admin
    password_hash: ` + testAdminPHC + `
volumes:
  - name: test
    path: /tmp
`)
	s.Require().NoError(err)
	s.Assert().Equal(512, cfg.Server.SubscribeBufferSize)
	s.Assert().Equal(30*time.Second, cfg.Server.SubscribeHeartbeatInterval)
}

func (s *ConfigTestSuite) TestSubscribeEnvOverride() {
	s.T().Setenv("GMOUNTIE_SERVER_SUBSCRIBE_BUFFER_SIZE", "1024")
	s.T().Setenv("GMOUNTIE_SERVER_SUBSCRIBE_HEARTBEAT_INTERVAL", "20s")
	cfg, err := LoadConfigFromString(`
server:
  address: "0.0.0.0"
  port: 9449
auth:
  type: basic
  users:
  - username: admin
    password_hash: ` + testAdminPHC + `
volumes:
  - name: test
    path: /tmp
`)
	s.Require().NoError(err)
	s.Assert().Equal(1024, cfg.Server.SubscribeBufferSize)
	s.Assert().Equal(20*time.Second, cfg.Server.SubscribeHeartbeatInterval)
}

func (s *ConfigTestSuite) TestSessionGracePeriodDefault() {
	cfg, err := LoadConfigFromString(`
server:
  address: "0.0.0.0"
  port: 9449
auth:
  type: basic
  users:
  - username: admin
    password_hash: ` + testAdminPHC + `
volumes:
  - name: test
    path: /tmp
`)
	s.Require().NoError(err)
	s.Assert().Equal(DefaultSessionGracePeriod, cfg.Server.Session.GracePeriod)
}

func (s *ConfigTestSuite) TestSessionGracePeriodExplicitYAML() {
	cfg, err := LoadConfigFromString(`
server:
  address: "0.0.0.0"
  port: 9449
  session:
    grace_period: 2m
auth:
  type: basic
  users:
  - username: admin
    password_hash: ` + testAdminPHC + `
volumes:
  - name: test
    path: /tmp
`)
	s.Require().NoError(err)
	s.Assert().Equal(2*time.Minute, cfg.Server.Session.GracePeriod)
}

func (s *ConfigTestSuite) TestSessionGracePeriodEnvOverride() {
	s.T().Setenv("GMOUNTIE_SERVER_SESSION_GRACE_PERIOD", "90s")
	cfg, err := LoadConfigFromString(`
server:
  address: "0.0.0.0"
  port: 9449
auth:
  type: basic
  users:
  - username: admin
    password_hash: ` + testAdminPHC + `
volumes:
  - name: test
    path: /tmp
`)
	s.Require().NoError(err)
	s.Assert().Equal(90*time.Second, cfg.Server.Session.GracePeriod)
}

func TestConfigTestSuite(t *testing.T) {
	suite.Run(t, new(ConfigTestSuite))
}
