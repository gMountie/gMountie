package config_test

import (
	"path/filepath"
	"testing"

	"gmountie/pkg/client/config"

	"github.com/spf13/viper"
	"github.com/stretchr/testify/suite"
)

type CacheConfigSuite struct{ suite.Suite }

func (s *CacheConfigSuite) TestDefaultsHaveNewKeys() {
	c, err := config.NewCacheConfig(nil)
	s.Require().NoError(err)
	s.Assert().True(c.Enabled, "Sub-spec C flips Enabled true by default")
	s.Assert().Equal(config.DefaultCacheMemoryMaxBytes, c.MemoryMaxBytes)
	s.Assert().Equal(config.DefaultCacheDiskMaxBytes, c.DiskMaxBytes)
	s.Assert().NotEmpty(c.Path)
}

func (s *CacheConfigSuite) TestExplicitOverrides() {
	v := viper.New()
	v.Set("enabled", false)
	v.Set("memory_max_bytes", 12345)
	v.Set("disk_max_bytes", 67890)
	v.Set("path", "/tmp/x")
	c, err := config.NewCacheConfig(v)
	s.Require().NoError(err)
	s.Assert().False(c.Enabled)
	s.Assert().Equal(12345, c.MemoryMaxBytes)
	s.Assert().Equal(67890, c.DiskMaxBytes)
	s.Assert().Equal(filepath.Clean("/tmp/x"), filepath.Clean(c.Path))
}

func TestCacheConfigSuite(t *testing.T) { suite.Run(t, new(CacheConfigSuite)) }
