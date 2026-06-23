package config

import (
	"testing"
	"time"

	"github.com/spf13/viper"
	"github.com/stretchr/testify/suite"
)

// FUSEConfigSuite tests the configurable attr/entry timeout fields added
// alongside the existing FUSE knobs.
type FUSEConfigSuite struct {
	suite.Suite
}

// TestAttrEntryTimeoutDefaults verifies that omitting the fuse: block yields
// the documented 1 s defaults for both fields.
func (s *FUSEConfigSuite) TestAttrEntryTimeoutDefaults() {
	cfg, err := NewFUSEConfig(nil)
	s.Require().NoError(err)
	s.Equal(DefaultFUSEAttrTimeout, cfg.AttrTimeout,
		"AttrTimeout default must equal DefaultFUSEAttrTimeout (1s)")
	s.Equal(DefaultFUSEEntryTimeout, cfg.EntryTimeout,
		"EntryTimeout default must equal DefaultFUSEEntryTimeout (1s)")
	s.Equal(DefaultFUSETBackend, cfg.FuseTBackend,
		"FuseTBackend default must be \"auto\"")
	s.Equal("auto", cfg.FuseTBackend)
}

// TestAttrEntryTimeoutEmptyViper verifies that an empty viper sub-tree
// (the code path taken when the YAML omits the fuse: section) still
// produces 1 s defaults.
func (s *FUSEConfigSuite) TestAttrEntryTimeoutEmptyViper() {
	cfg, err := NewFUSEConfig(viper.New())
	s.Require().NoError(err)
	s.Equal(DefaultFUSEAttrTimeout, cfg.AttrTimeout)
	s.Equal(DefaultFUSEEntryTimeout, cfg.EntryTimeout)
}

// TestAttrEntryTimeoutOverride verifies explicit values round-trip through the
// duration decode hook.
func (s *FUSEConfigSuite) TestAttrEntryTimeoutOverride() {
	v := viper.New()
	v.Set("attr_timeout", "30s")
	v.Set("entry_timeout", "10s")
	cfg, err := NewFUSEConfig(v)
	s.Require().NoError(err)
	s.Equal(30*time.Second, cfg.AttrTimeout)
	s.Equal(10*time.Second, cfg.EntryTimeout)
}

// TestAttrEntryTimeoutZeroAllowed verifies that 0s parses and validates
// (gte=0). Semantics: zero means "use the default" — buildFSOptions maps it
// to the 1s defaults at mount time, never to kernel-cache-off (see
// mount/common.go).
func (s *FUSEConfigSuite) TestAttrEntryTimeoutZeroAllowed() {
	v := viper.New()
	v.Set("attr_timeout", "0s")
	v.Set("entry_timeout", "0s")
	cfg, err := NewFUSEConfig(v)
	s.Require().NoError(err)
	s.Equal(time.Duration(0), cfg.AttrTimeout)
	s.Equal(time.Duration(0), cfg.EntryTimeout)
}

// TestAttrEntryTimeoutFullConfigRoundTrip exercises the full
// LoadConfigFromString path so the mirrorEnvToSub wiring in ParseConfig is
// covered — YAML fuse.attr_timeout / fuse.entry_timeout must land on the
// parsed struct.
func (s *FUSEConfigSuite) TestAttrEntryTimeoutFullConfigRoundTrip() {
	conf := `
server:
  address: 127.0.0.1
  port: 9449
auth:
  type: basic
  username: admin
  password: admin
fuse:
  attr_timeout: 30s
  entry_timeout: 15s
`
	result, err := LoadConfigFromString(conf)
	s.Require().NoError(err)
	s.Require().NotNil(result.FUSE)
	s.Equal(30*time.Second, result.FUSE.AttrTimeout)
	s.Equal(15*time.Second, result.FUSE.EntryTimeout)
}

// TestDirectIODefaultsOff verifies the global direct-IO escape hatch is off
// by default (ordinary files keep the kernel page cache; -shm sidecars get
// direct-IO unconditionally in the node layer regardless of this flag).
func (s *FUSEConfigSuite) TestDirectIODefaultsOff() {
	cfg, err := NewFUSEConfig(nil)
	s.Require().NoError(err)
	s.False(cfg.DirectIO, "direct_io must default off")

	cfg, err = NewFUSEConfig(viper.New())
	s.Require().NoError(err)
	s.False(cfg.DirectIO, "empty viper sub-tree must default direct_io off")
}

// TestDirectIOOverride verifies fuse.direct_io: true round-trips.
func (s *FUSEConfigSuite) TestDirectIOOverride() {
	v := viper.New()
	v.Set("direct_io", true)
	cfg, err := NewFUSEConfig(v)
	s.Require().NoError(err)
	s.True(cfg.DirectIO)
}

// TestHandleKillPrivDefaultsOn verifies the cap defaults ON (nil and empty
// viper sub-trees), so every mount gets the per-write-getxattr fix without
// configuration.
func (s *FUSEConfigSuite) TestHandleKillPrivDefaultsOn() {
	cfg, err := NewFUSEConfig(nil)
	s.Require().NoError(err)
	s.True(cfg.HandleKillPriv, "handle_kill_priv must default on")

	cfg, err = NewFUSEConfig(viper.New())
	s.Require().NoError(err)
	s.True(cfg.HandleKillPriv, "empty viper sub-tree must default handle_kill_priv on")
}

// TestHandleKillPrivOverride verifies fuse.handle_kill_priv: false round-trips
// (an explicit opt-out is honored, not clobbered by the default).
func (s *FUSEConfigSuite) TestHandleKillPrivOverride() {
	v := viper.New()
	v.Set("handle_kill_priv", false)
	cfg, err := NewFUSEConfig(v)
	s.Require().NoError(err)
	s.False(cfg.HandleKillPriv)
}

// TestAutoXAttrDefaultsOff verifies auto_xattr defaults OFF (nil and empty
// viper sub-trees): the clean noappledouble path is the default, and Finder
// copies work there via the SETXATTR-flag translation.
func (s *FUSEConfigSuite) TestAutoXAttrDefaultsOff() {
	cfg, err := NewFUSEConfig(nil)
	s.Require().NoError(err)
	s.False(cfg.AutoXAttr, "auto_xattr must default off")

	cfg, err = NewFUSEConfig(viper.New())
	s.Require().NoError(err)
	s.False(cfg.AutoXAttr, "empty viper sub-tree must default auto_xattr off")
}

// TestAutoXAttrOverride verifies fuse.auto_xattr: true round-trips (the opt-in
// to ._ AppleDouble storage is honored, not clobbered by the default).
func (s *FUSEConfigSuite) TestAutoXAttrOverride() {
	v := viper.New()
	v.Set("auto_xattr", true)
	cfg, err := NewFUSEConfig(v)
	s.Require().NoError(err)
	s.True(cfg.AutoXAttr)
}

func TestFUSEConfigSuite(t *testing.T) {
	suite.Run(t, new(FUSEConfigSuite))
}
