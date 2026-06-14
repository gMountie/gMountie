package config

import (
	"testing"
	"time"

	"github.com/spf13/viper"
	"github.com/stretchr/testify/suite"
)

type RpcConfigSuite struct {
	suite.Suite
}

func (s *RpcConfigSuite) TestRetryWindowDefault() {
	cfg, err := NewRpcConfig(nil)
	s.Require().NoError(err)
	s.Equal(60*time.Second, cfg.RetryWindow)
}

func (s *RpcConfigSuite) TestRetryWindowOverride() {
	v := viper.New()
	v.Set("retry_window", "5m")
	cfg, err := NewRpcConfig(v)
	s.Require().NoError(err)
	s.Equal(5*time.Minute, cfg.RetryWindow)
}

func (s *RpcConfigSuite) TestRetryWindowZeroAllowed() {
	v := viper.New()
	v.Set("retry_window", "0s")
	cfg, err := NewRpcConfig(v)
	s.Require().NoError(err)
	s.Equal(time.Duration(0), cfg.RetryWindow)
}

// TestNilEqualsDefaultHelper guards the AR-L2 refactor: the v==nil fast path
// must return exactly defaultRpcConfig() (the single literal-defaults source),
// so the literal and the SetDefault block can't silently drift.
func (s *RpcConfigSuite) TestNilEqualsDefaultHelper() {
	cfg, err := NewRpcConfig(nil)
	s.Require().NoError(err)
	s.Equal(defaultRpcConfig(), cfg)
}

// TestEmptyTreeEqualsNil proves the SetDefault block yields the same values as
// the literal defaults: parsing an empty viper sub-tree equals the nil result.
func (s *RpcConfigSuite) TestEmptyTreeEqualsNil() {
	cfg, err := NewRpcConfig(viper.New())
	s.Require().NoError(err)
	s.Equal(defaultRpcConfig(), cfg)
}

func TestRpcConfigSuite(t *testing.T) {
	suite.Run(t, new(RpcConfigSuite))
}
