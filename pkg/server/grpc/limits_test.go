package grpc

import (
	"testing"
	"time"

	"go.gmountie.dev/gmountie/pkg/server/config"

	"github.com/stretchr/testify/suite"
)

type LimitsSuite struct {
	suite.Suite
}

func (s *LimitsSuite) TestEmptyConfigYieldsNoOptions() {
	s.Empty(limitsServerOptions(config.LimitsConfig{}))
}

func (s *LimitsSuite) TestMaxRecvMessageSizeProducesOption() {
	opts := limitsServerOptions(config.LimitsConfig{MaxRecvMessageSize: 1024})
	s.Len(opts, 1)
}

func (s *LimitsSuite) TestMaxConcurrentStreamsProducesOption() {
	opts := limitsServerOptions(config.LimitsConfig{MaxConcurrentStreams: 100})
	s.Len(opts, 1)
}

// TestIdleAndAgeYieldNoKeepaliveOption pins MN-L1: limitsServerOptions no
// longer emits a KeepaliveParams option for Idle/Age. getOptions is the single
// place keepalive is assembled (it folds Idle/Age into the one
// ServerParameters), so a branch here would be dead by construction.
func (s *LimitsSuite) TestIdleAndAgeYieldNoKeepaliveOption() {
	s.Empty(limitsServerOptions(config.LimitsConfig{MaxConnectionIdle: 30 * time.Second}))
	s.Empty(limitsServerOptions(config.LimitsConfig{MaxConnectionAge: 30 * time.Second}))
	s.Empty(limitsServerOptions(config.LimitsConfig{MaxConnectionIdle: time.Second, MaxConnectionAge: time.Second}))
}

func (s *LimitsSuite) TestAllNonzeroProducesTwoOptions() {
	opts := limitsServerOptions(config.LimitsConfig{
		MaxRecvMessageSize:   1,
		MaxConcurrentStreams: 1,
		MaxConnectionIdle:    time.Second, // no longer contributes an option
	})
	s.Len(opts, 2)
}

func TestLimitsSuite(t *testing.T) {
	suite.Run(t, new(LimitsSuite))
}
