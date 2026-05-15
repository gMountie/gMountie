package config

import (
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/suite"
)

type ConfigTestSuite struct {
	suite.Suite
	fullConf    string
	minimalConf string
}

func (s *ConfigTestSuite) SetupTest() {
	s.fullConf = `
server:
  address: 0.0.0.0
  port: 9449
  tls: false
auth:
  type: none
`
	s.minimalConf = `
server:
  address: 127.0.0.1
auth:
  type: none
`
}

// Test String() function
func (s *ConfigTestSuite) TestString() {
	cfg, err := LoadConfigFromString(s.fullConf)
	s.Require().NoError(err)

	str, err := cfg.String()
	s.Require().NoError(err)
	s.Assert().Contains(str, "address: 0.0.0.0")
	s.Assert().Contains(str, "port: 9449")
	s.Assert().Contains(str, "type: none")
}

// Test Save() function
func (s *ConfigTestSuite) TestSave() {
	// Create a temporary file
	tmpfile, err := os.CreateTemp("", "config_test_*.yaml")
	s.Require().NoError(err)
	s.T().Cleanup(func() {
		os.Remove(tmpfile.Name())
	})

	// Load config and set path
	cfg, err := LoadConfigFromString(s.fullConf)
	s.Require().NoError(err)

	// Test Save
	err = cfg.Save(tmpfile.Name())
	s.Require().NoError(err)

	// Verify file contents
	content, err := os.ReadFile(tmpfile.Name())
	s.Require().NoError(err)
	s.Assert().Contains(string(content), "address: 0.0.0.0")
}

// Test Save() with invalid path
func (s *ConfigTestSuite) TestSaveInvalidPath() {
	path := "/nonexistent/path/config.yaml"
	cfg, err := LoadConfigFromString(s.fullConf)
	s.Require().NoError(err)

	err = cfg.Save(path)
	s.Require().Error(err)
}

// TestParse_RpcDefaults verifies that omitting the rpc: section yields
// the documented default timeouts.
func (s *ConfigTestSuite) TestParse_RpcDefaults() {
	conf := `
server:
  address: 127.0.0.1
  port: 9449
auth:
  type: none
`
	result, err := LoadConfigFromString(conf)
	s.Require().NoError(err)
	s.Require().NotNil(result.Rpc)
	s.Assert().Equal(DefaultRpcTimeoutMeta, result.Rpc.TimeoutMeta)
	s.Assert().Equal(DefaultRpcTimeoutIO, result.Rpc.TimeoutIO)
}

// TestParse_RpcOverride verifies explicit rpc values override the defaults.
func (s *ConfigTestSuite) TestParse_RpcOverride() {
	conf := `
server:
  address: 127.0.0.1
  port: 9449
auth:
  type: none
rpc:
  timeout_meta: 2s
  timeout_io: 1m
`
	result, err := LoadConfigFromString(conf)
	s.Require().NoError(err)
	s.Assert().Equal(2*time.Second, result.Rpc.TimeoutMeta)
	s.Assert().Equal(time.Minute, result.Rpc.TimeoutIO)
}

// TestParse_FUSEDefaults verifies that omitting the fuse: section yields
// the documented default FUSE mount tuning.
func (s *ConfigTestSuite) TestParse_FUSEDefaults() {
	conf := `
server:
  address: 127.0.0.1
  port: 9449
auth:
  type: none
`
	result, err := LoadConfigFromString(conf)
	s.Require().NoError(err)
	s.Require().NotNil(result.FUSE)
	s.Assert().Equal(DefaultFUSEMaxWriteBytes, result.FUSE.MaxWriteBytes)
	s.Assert().Equal(DefaultFUSEMaxBackground, result.FUSE.MaxBackground)
	s.Assert().Equal(DefaultFUSEWritebackCache, result.FUSE.WritebackCache)
}

// TestParse_FUSEOverride verifies explicit fuse values override the defaults.
func (s *ConfigTestSuite) TestParse_FUSEOverride() {
	conf := `
server:
  address: 127.0.0.1
  port: 9449
auth:
  type: none
fuse:
  max_write_bytes: 524288
  max_background: 128
  writeback_cache: true
`
	result, err := LoadConfigFromString(conf)
	s.Require().NoError(err)
	s.Assert().Equal(512<<10, result.FUSE.MaxWriteBytes)
	s.Assert().Equal(128, result.FUSE.MaxBackground)
	s.Assert().True(result.FUSE.WritebackCache)
}

// TestParse_FUSEValidation rejects out-of-range values.
func (s *ConfigTestSuite) TestParse_FUSEValidation() {
	conf := `
server:
  address: 127.0.0.1
  port: 9449
auth:
  type: none
fuse:
  max_write_bytes: 1024
`
	_, err := LoadConfigFromString(conf)
	s.Require().Error(err)
}

// TestParse_LogEnvBindings verifies GMOUNTIE_LOG_LEVEL / GMOUNTIE_LOG_FORMAT
// reach cfg.Log.* on the client side, mirroring the server-side asymmetry.
func (s *ConfigTestSuite) TestParse_LogEnvBindings() {
	s.T().Setenv("GMOUNTIE_LOG_LEVEL", "debug")
	s.T().Setenv("GMOUNTIE_LOG_FORMAT", "json")

	result, err := LoadConfigFromString(s.minimalConf)
	s.Require().NoError(err)
	s.Require().NotNil(result.Log)
	s.Assert().Equal("debug", result.Log.Level)
	s.Assert().Equal("json", result.Log.Format)
}

// Test Runner
func TestConfigTestSuite(t *testing.T) {
	suite.Run(t, new(ConfigTestSuite))
}
