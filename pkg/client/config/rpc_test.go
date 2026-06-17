package config

import (
	"testing"
	"time"

	"github.com/go-playground/validator/v10"
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

// TestReadaheadWindowDefault guards the deeper-overlap default: a shallow window
// stutters under WAN RTT (per the throughput investigation, window 4 ≈ 71% of a
// 100Mbit link, window 16 ≈ 82%). The default must keep enough chunks overlapped.
func (s *RpcConfigSuite) TestReadaheadWindowDefault() {
	cfg, err := NewRpcConfig(nil)
	s.Require().NoError(err)
	s.Equal(16, cfg.ReadaheadWindow)
}

// TestInitialWindowDefaults guards the gRPC HTTP/2 flow-control windows: at high
// BDP (1Gbit/50ms) gRPC's auto-tuned window is tighter than the link, so we pin
// generous defaults (≥ worst-case BDP). Harmless on slow links (window is unused
// receive credit), +31% on a 1Gbit single stream.
func (s *RpcConfigSuite) TestInitialWindowDefaults() {
	cfg, err := NewRpcConfig(nil)
	s.Require().NoError(err)
	s.Equal(16<<20, cfg.InitialConnWindowBytes)
	s.Equal(8<<20, cfg.InitialStreamWindowBytes)
}

func (s *RpcConfigSuite) TestInitialWindowOverride() {
	v := viper.New()
	v.Set("initial_conn_window_bytes", 32<<20)
	v.Set("initial_stream_window_bytes", 16<<20)
	cfg, err := NewRpcConfig(v)
	s.Require().NoError(err)
	s.Equal(32<<20, cfg.InitialConnWindowBytes)
	s.Equal(16<<20, cfg.InitialStreamWindowBytes)
}

// TestInitialWindowZeroAllowed: 0 means "leave gRPC BDP autotuning on" (don't pin).
func (s *RpcConfigSuite) TestInitialWindowZeroAllowed() {
	v := viper.New()
	v.Set("initial_conn_window_bytes", 0)
	v.Set("initial_stream_window_bytes", 0)
	cfg, err := NewRpcConfig(v)
	s.Require().NoError(err)
	s.Equal(0, cfg.InitialConnWindowBytes)
	s.Equal(0, cfg.InitialStreamWindowBytes)
}

func (s *RpcConfigSuite) TestConnectionsDefault() {
	cfg, err := NewRpcConfig(nil)
	s.Require().NoError(err)
	s.Equal(DefaultRpcConnections, cfg.Connections)

	cfg, err = NewRpcConfig(viper.New())
	s.Require().NoError(err)
	s.Equal(DefaultRpcConnections, cfg.Connections, "empty viper sub-tree must default connections")
}

func (s *RpcConfigSuite) TestConnectionsOverride() {
	v := viper.New()
	v.Set("connections", 8)
	cfg, err := NewRpcConfig(v)
	s.Require().NoError(err)
	s.Equal(8, cfg.Connections)
}

// TestConnectionsValidationRejectsZeroAndTooMany verifies that connections=0
// and connections=17 fail the validate tags (min=1,max=16).
// NOTE: NewRpcConfig does not run the validator; validation is done at the
// Config level via Config.Validate(). We invoke the validator directly here,
// matching the only validation pattern in this package.
func (s *RpcConfigSuite) TestConnectionsValidationRejectsZeroAndTooMany() {
	validate := validator.New()
	for _, bad := range []int{0, 17} {
		v := viper.New()
		v.Set("connections", bad)
		cfg, err := NewRpcConfig(v)
		s.Require().NoError(err, "connections=%d: parse must succeed", bad)
		err = validate.Struct(cfg)
		s.Require().Error(err, "connections=%d must be rejected by validator", bad)
	}
}

func TestRpcConfigSuite(t *testing.T) {
	suite.Run(t, new(RpcConfigSuite))
}
