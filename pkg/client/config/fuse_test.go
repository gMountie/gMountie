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

// TestAttrEntryTimeoutZeroAllowed verifies that 0s is accepted (valid per the
// gte=0 constraint).
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

func TestFUSEConfigSuite(t *testing.T) {
	suite.Run(t, new(FUSEConfigSuite))
}
