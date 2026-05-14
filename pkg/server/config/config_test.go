package config

import (
	"testing"

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
  type: none	
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
	s.Assert().Equal(AuthConfigTypeNone, result.Auth.GetType())
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

// Test Runner
func TestConfigTestSuite(t *testing.T) {
	suite.Run(t, new(ConfigTestSuite))
}
