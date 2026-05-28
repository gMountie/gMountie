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
auth:
  type: basic
  username: admin
  password: admin
`
	s.minimalConf = `
server:
  address: 127.0.0.1
auth:
  type: basic
  username: admin
  password: admin
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
	s.Assert().Contains(str, "type: basic")
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
  type: basic
  username: admin
  password: admin
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
  type: basic
  username: admin
  password: admin
rpc:
  timeout_meta: 2s
  timeout_io: 1m
`
	result, err := LoadConfigFromString(conf)
	s.Require().NoError(err)
	s.Assert().Equal(2*time.Second, result.Rpc.TimeoutMeta)
	s.Assert().Equal(time.Minute, result.Rpc.TimeoutIO)
}

// TestParse_CompressionDefaultsOff documents the perf reasoning: a
// loopback profile showed snappy was 53% of client CPU, so compression
// is off by default and must be explicitly opted into.
func (s *ConfigTestSuite) TestParse_CompressionDefaultsOff() {
	conf := `
server:
  address: 127.0.0.1
  port: 9449
auth:
  type: basic
  username: admin
  password: admin
`
	result, err := LoadConfigFromString(conf)
	s.Require().NoError(err)
	s.Assert().Equal(CompressionNone, result.Rpc.Compression)
}

func (s *ConfigTestSuite) TestParse_CompressionSnappyOptIn() {
	conf := `
server:
  address: 127.0.0.1
  port: 9449
auth:
  type: basic
  username: admin
  password: admin
rpc:
  compression: snappy
`
	result, err := LoadConfigFromString(conf)
	s.Require().NoError(err)
	s.Assert().Equal(CompressionSnappy, result.Rpc.Compression)
}

func (s *ConfigTestSuite) TestParse_CompressionRejectsUnknown() {
	conf := `
server:
  address: 127.0.0.1
  port: 9449
auth:
  type: basic
  username: admin
  password: admin
rpc:
  compression: gzip
`
	_, err := LoadConfigFromString(conf)
	s.Require().Error(err)
}

// TestParse_FUSEDefaults verifies that omitting the fuse: section yields
// the documented default FUSE mount tuning.
func (s *ConfigTestSuite) TestParse_FUSEDefaults() {
	conf := `
server:
  address: 127.0.0.1
  port: 9449
auth:
  type: basic
  username: admin
  password: admin
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
  type: basic
  username: admin
  password: admin
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
  type: basic
  username: admin
  password: admin
fuse:
  max_write_bytes: 1024
`
	_, err := LoadConfigFromString(conf)
	s.Require().Error(err)
}

// TestCacheDefaults verifies that omitting the cache: section yields
// the documented Phase 4 Sub-spec C defaults.
func (s *ConfigTestSuite) TestCacheDefaults() {
	conf := `
server:
  address: 127.0.0.1
  port: 9449
auth:
  type: basic
  username: admin
  password: admin
`
	result, err := LoadConfigFromString(conf)
	s.Require().NoError(err)
	s.Require().NotNil(result.Cache)
	s.Assert().Equal(DefaultCacheEnabled, result.Cache.Enabled)
	s.Assert().Equal(DefaultCacheMemoryMaxBytes, result.Cache.MemoryMaxBytes)
	s.Assert().Equal(DefaultCacheChunkSizeBytes, result.Cache.ChunkSizeBytes)
	s.Assert().Equal(DefaultCacheAttrTTL, result.Cache.AttrTTL)
	s.Assert().Equal(DefaultCacheDirTTL, result.Cache.DirTTL)
	s.Assert().Equal(DefaultCacheNegativeTTL, result.Cache.NegativeTTL)
}

// TestCacheEnvOverride verifies GMOUNTIE_CACHE_* env vars reach
// cfg.Cache.* when the YAML omits the cache block. Confirms the nested
// BindEnv wiring works on the client side.
func (s *ConfigTestSuite) TestCacheEnvOverride() {
	s.T().Setenv("GMOUNTIE_CACHE_ENABLED", "true")
	s.T().Setenv("GMOUNTIE_CACHE_MEMORY_MAX_BYTES", "2147483648")
	s.T().Setenv("GMOUNTIE_CACHE_ATTR_TTL", "10s")

	result, err := LoadConfigFromString(s.minimalConf)
	s.Require().NoError(err)
	s.Require().NotNil(result.Cache)
	s.Assert().True(result.Cache.Enabled)
	s.Assert().Equal(1<<31, result.Cache.MemoryMaxBytes)
	s.Assert().Equal(10*time.Second, result.Cache.AttrTTL)
	// Untouched keys keep defaults.
	s.Assert().Equal(DefaultCacheChunkSizeBytes, result.Cache.ChunkSizeBytes)
	s.Assert().Equal(DefaultCacheDirTTL, result.Cache.DirTTL)
	s.Assert().Equal(DefaultCacheNegativeTTL, result.Cache.NegativeTTL)
}

// TestCacheExplicitYAML verifies a fully populated cache: block round-trips
// through the duration decode hook and overrides every default.
func (s *ConfigTestSuite) TestCacheExplicitYAML() {
	conf := `
server:
  address: 127.0.0.1
  port: 9449
auth:
  type: basic
  username: admin
  password: admin
cache:
  enabled: true
  memory_max_bytes: 536870912
  chunk_size_bytes: 524288
  attr_ttl: 1m
  dir_ttl: 30s
  negative_ttl: 500ms
`
	result, err := LoadConfigFromString(conf)
	s.Require().NoError(err)
	s.Require().NotNil(result.Cache)
	s.Assert().True(result.Cache.Enabled)
	s.Assert().Equal(512<<20, result.Cache.MemoryMaxBytes)
	s.Assert().Equal(512<<10, result.Cache.ChunkSizeBytes)
	s.Assert().Equal(time.Minute, result.Cache.AttrTTL)
	s.Assert().Equal(30*time.Second, result.Cache.DirTTL)
	s.Assert().Equal(500*time.Millisecond, result.Cache.NegativeTTL)
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

func (s *ConfigTestSuite) TestParse_ReadaheadWindowDefault() {
	conf := `
server:
  address: 127.0.0.1
  port: 9449
auth:
  type: basic
  username: admin
  password: admin
`
	result, err := LoadConfigFromString(conf)
	s.Require().NoError(err)
	s.Assert().Equal(1<<20, result.Rpc.ReadaheadChunkBytes)
	s.Assert().Equal(4, result.Rpc.ReadaheadWindow)
	s.Assert().Equal(3, result.Rpc.ReadaheadThreshold) // unchanged
	s.Assert().Equal(DefaultReadaheadChunkBytes, result.Rpc.ReadaheadChunkBytes)
	s.Assert().Equal(DefaultReadaheadWindow, result.Rpc.ReadaheadWindow)
	s.Assert().Equal(DefaultReadaheadThreshold, result.Rpc.ReadaheadThreshold)
}

func (s *ConfigTestSuite) TestParse_ReadaheadWindowOverride() {
	conf := `
server:
  address: 127.0.0.1
  port: 9449
auth:
  type: basic
  username: admin
  password: admin
rpc:
  readahead_window: 12
`
	result, err := LoadConfigFromString(conf)
	s.Require().NoError(err)
	s.Assert().Equal(12, result.Rpc.ReadaheadWindow)
}

func (s *ConfigTestSuite) TestParse_ReadaheadWindowRejectsOutOfRange() {
	conf := `
server:
  address: 127.0.0.1
  port: 9449
auth:
  type: basic
  username: admin
  password: admin
rpc:
  readahead_window: 0
`
	_, err := LoadConfigFromString(conf)
	s.Require().Error(err)
}

// Test Runner
func TestConfigTestSuite(t *testing.T) {
	suite.Run(t, new(ConfigTestSuite))
}
