package config

import (
	"testing"

	"github.com/spf13/viper"
	"github.com/stretchr/testify/suite"
)

type MountConfigTestSuite struct {
	suite.Suite
}

// Test successful parsing of "single" mount configuration
func (s *MountConfigTestSuite) TestParse_SingleMount() {
	conf := `
mount:
  type: single
  path: /mnt/test
  volume: testvol
` + minimalServerConfig
	result, err := LoadConfigFromString(conf)
	s.Require().NoError(err)
	s.Assert().Equal(MountTypeSingle, result.Mount.GetType())

	singleMount, ok := result.Mount.(*SingleMountConfig)
	s.Require().True(ok)
	s.Assert().Equal("/mnt/test", singleMount.Path)
	s.Assert().Equal("testvol", singleMount.Volume)
}

// Test error cases for invalid mount type — includes "vfs" which is no longer a valid type
func (s *MountConfigTestSuite) TestParse_InvalidMountType() {
	for _, typ := range []string{"invalid", "vfs"} {
		conf := `
mount:
  type: ` + typ + `
  path: /mnt/test
` + minimalServerConfig
		_, err := LoadConfigFromString(conf)
		s.Require().Errorf(err, "expected error for mount type %q", typ)
		s.Assert().Contains(err.Error(), "invalid mount type")
	}
}

// TestParse_SingleMount_PathOptional verifies that a single-mount config without
// a `path` now parses: the mountpoint is supplied on the CLI (or a profile's
// mount.path) at mount time, so the config layer no longer requires it. The
// mount command enforces that a mountpoint exists after resolution.
func (s *MountConfigTestSuite) TestParse_SingleMount_PathOptional() {
	conf := `
mount:
  type: single
  volume: testvol
` + minimalServerConfig
	result, err := LoadConfigFromString(conf)
	s.Require().NoError(err)
	sm, ok := result.Mount.(*SingleMountConfig)
	s.Require().True(ok)
	s.Equal("testvol", sm.Volume)
	s.Empty(sm.Path)
}

// TestSingleMountConfig_VolumeOnly verifies that a profile naming only the
// volume (mountpoint supplied on the CLI) passes NewSingleMountConfig and
// Validate without error.
func (s *MountConfigTestSuite) TestSingleMountConfig_VolumeOnly() {
	v := viper.New()
	v.Set("type", string(MountTypeSingle))
	v.Set("volume", "shared")
	cfg, err := NewSingleMountConfig(v)
	s.Require().NoError(err)
	s.Require().NoError(cfg.Validate())
	s.Equal("shared", cfg.Volume)
	s.Empty(cfg.Path)
}

func TestMountConfigTestSuite(t *testing.T) {
	suite.Run(t, new(MountConfigTestSuite))
}
