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

func TestRpcConfigSuite(t *testing.T) {
	suite.Run(t, new(RpcConfigSuite))
}
