package config

import (
	"testing"
	"time"

	"github.com/stretchr/testify/suite"
)

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
    password: admin
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
	s.Assert().Equal(AuthConfigTypeBasic, result.Auth.GetType())
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
	// Setup.
	conf := `
server:
  address: 0.0.0.0
  port: 8000

auth:
  type: basic
  users:
  - username: test
    password: test
volumes:
  - name: test
    path: /tmp
`
	// Test.
	result, err := LoadConfigFromString(conf)

	// Verify.
	s.Require().NoError(err)
	s.Assert().Equal(AuthConfigTypeBasic, result.Auth.GetType())
	s.Assert().Len(result.Auth.(*BasicAuthConfig).Users, 1)
	s.Assert().Equal("test", result.Auth.(*BasicAuthConfig).Users[0].Username)
	s.Assert().Equal("test", result.Auth.(*BasicAuthConfig).Users[0].Password)
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

func (s *ConfigTestSuite) TestMetricsAddrDefault() {
	cfg, err := LoadConfigFromString(`
server:
  address: "0.0.0.0"
  port: 9449
auth:
  type: basic
  users:
  - username: admin
    password: admin
volumes:
  - name: test
    path: /tmp
`)
	s.Require().NoError(err)
	s.Assert().Equal(":9090", cfg.Server.MetricsAddr)
}

func (s *ConfigTestSuite) TestMetricsAddrEnvOverride() {
	s.T().Setenv("GMOUNTIE_SERVER_METRICS_ADDR", ":19999")
	cfg, err := LoadConfigFromString(`
server:
  address: "0.0.0.0"
  port: 9449
auth:
  type: basic
  users:
  - username: admin
    password: admin
volumes:
  - name: test
    path: /tmp
`)
	s.Require().NoError(err)
	s.Assert().Equal(":19999", cfg.Server.MetricsAddr)
}

func (s *ConfigTestSuite) TestMetricsAddrExplicitOverride() {
	cfg, err := LoadConfigFromString(`
server:
  address: "0.0.0.0"
  port: 9449
  metrics_addr: "127.0.0.1:9091"
auth:
  type: basic
  users:
  - username: admin
    password: admin
volumes:
  - name: test
    path: /tmp
`)
	s.Require().NoError(err)
	s.Assert().Equal("127.0.0.1:9091", cfg.Server.MetricsAddr)
}

// Test Runner
func (s *ConfigTestSuite) TestMaxMessageBytesAndKeepaliveDefaults() {
	cfg, err := LoadConfigFromString(`
server:
  address: "0.0.0.0"
  port: 9449
auth:
  type: basic
  users:
  - username: admin
    password: admin
volumes:
  - name: test
    path: /tmp
`)
	s.Require().NoError(err)
	s.Assert().Equal(DefaultMaxMessageBytes, cfg.Server.MaxMessageBytes)
	s.Assert().Equal(DefaultKeepaliveTime, cfg.Server.Keepalive.Time)
	s.Assert().Equal(DefaultKeepaliveTimeout, cfg.Server.Keepalive.Timeout)
	s.Assert().Equal(DefaultKeepaliveMinTime, cfg.Server.Keepalive.MinTime)
	s.Assert().Equal(DefaultKeepalivePermitWithoutStream, cfg.Server.Keepalive.PermitWithoutStream)
}

func (s *ConfigTestSuite) TestKeepaliveEnvOverride() {
	s.T().Setenv("GMOUNTIE_SERVER_MAX_MESSAGE_BYTES", "33554432")
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
    password: admin
volumes:
  - name: test
    path: /tmp
`)
	s.Require().NoError(err)
	s.Assert().Equal(33554432, cfg.Server.MaxMessageBytes)
	s.Assert().Equal(45*time.Second, cfg.Server.Keepalive.Time)
	s.Assert().Equal(15*time.Second, cfg.Server.Keepalive.Timeout)
}

func (s *ConfigTestSuite) TestKeepaliveExplicitYAML() {
	cfg, err := LoadConfigFromString(`
server:
  address: "0.0.0.0"
  port: 9449
  max_message_bytes: 1048576
  keepalive:
    time: 1m
    timeout: 20s
    min_time: 5s
    permit_without_stream: false
auth:
  type: basic
  users:
  - username: admin
    password: admin
volumes:
  - name: test
    path: /tmp
`)
	s.Require().NoError(err)
	s.Assert().Equal(1048576, cfg.Server.MaxMessageBytes)
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
    password: admin
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
    password: admin
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
    password: admin
volumes:
  - name: test
    path: /tmp
`)
	s.Require().NoError(err)
	s.Assert().Equal(1024, cfg.Server.SubscribeBufferSize)
	s.Assert().Equal(20*time.Second, cfg.Server.SubscribeHeartbeatInterval)
}

func TestConfigTestSuite(t *testing.T) {
	suite.Run(t, new(ConfigTestSuite))
}
