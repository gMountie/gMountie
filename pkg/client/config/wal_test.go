package config_test

import (
	"testing"

	"go.gmountie.dev/gmountie/pkg/client/config"

	"github.com/spf13/viper"
	"github.com/stretchr/testify/suite"
)

type WALConfigSuite struct{ suite.Suite }

// TestDefaultIsDisabled pins the "default off" guarantee: WAL/delegation is
// opt-in until production-stable, so a nil sub-tree must yield Enabled=false.
func (s *WALConfigSuite) TestDefaultIsDisabled() {
	c, err := config.NewWALConfig(nil)
	s.Require().NoError(err)
	s.Assert().False(c.Enabled, "WAL must default to disabled")
	s.Assert().Equal(config.DefaultWALEnabled, c.Enabled)
}

// TestEmptyTreeIsDisabled proves the SetDefault block agrees with the literal
// default: an empty viper sub-tree yields the same disabled config as nil.
func (s *WALConfigSuite) TestEmptyTreeIsDisabled() {
	c, err := config.NewWALConfig(viper.New())
	s.Require().NoError(err)
	s.Assert().False(c.Enabled)
}

func (s *WALConfigSuite) TestExplicitEnable() {
	v := viper.New()
	v.Set("enabled", true)
	c, err := config.NewWALConfig(v)
	s.Require().NoError(err)
	s.Assert().True(c.Enabled)
}

// TestEmptyTreeEqualsNil mirrors the cache config de-duplication guard: the
// v==nil literal-defaults path and the empty-tree SetDefault path must agree.
func (s *WALConfigSuite) TestEmptyTreeEqualsNil() {
	nilCfg, err := config.NewWALConfig(nil)
	s.Require().NoError(err)
	emptyCfg, err := config.NewWALConfig(viper.New())
	s.Require().NoError(err)
	s.Assert().Equal(nilCfg, emptyCfg)
}

func TestWALConfigSuite(t *testing.T) { suite.Run(t, new(WALConfigSuite)) }
