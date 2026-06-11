package config

import (
	"testing"
	"time"

	"github.com/spf13/viper"
	"github.com/stretchr/testify/suite"
)

type RenewConfigTestSuite struct{ suite.Suite }

func TestRenewConfigTestSuite(t *testing.T) { suite.Run(t, new(RenewConfigTestSuite)) }

func (s *RenewConfigTestSuite) TestNilViperYieldsDisabledDefaults() {
	cfg, err := NewRenewConfig(nil)
	s.Require().NoError(err)
	s.False(cfg.Enabled())
	s.Equal(DefaultRenewBefore, cfg.Before)
}

func (s *RenewConfigTestSuite) TestEnabledRequiresToken() {
	v := viper.New()
	v.Set("endpoint", "https://cp.example/v1/certs")
	_, err := NewRenewConfig(v)
	s.Require().Error(err)
	s.Contains(err.Error(), "token")
}

func (s *RenewConfigTestSuite) TestFullBlockParses() {
	v := viper.New()
	v.Set("endpoint", "https://cp.example/v1/certs")
	v.Set("token", "tok")
	v.Set("before", "2h")
	cfg, err := NewRenewConfig(v)
	s.Require().NoError(err)
	s.True(cfg.Enabled())
	s.Equal(2*time.Hour, cfg.Before)
}

func (s *RenewConfigTestSuite) TestParseConfigWiresRenewBlock() {
	v := viper.New()
	v.Set("server.address", "127.0.0.1")
	v.Set("server.port", 4444)
	v.Set("auth.type", "basic")
	v.Set("auth.username", "admin")
	v.Set("auth.password", "admin")
	v.Set("renew.endpoint", "https://cp.example/v1/certs")
	v.Set("renew.token", "tok")
	cfg, err := ParseConfig(v)
	s.Require().NoError(err)
	s.Require().NotNil(cfg.Renew)
	s.True(cfg.Renew.Enabled())
}
