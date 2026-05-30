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

func (s *LimitsSuite) TestKeepaliveOptionAppearsWhenIdleOrAgeNonzero() {
	opts := limitsServerOptions(config.LimitsConfig{MaxConnectionIdle: 30 * time.Second})
	s.Len(opts, 1)

	opts = limitsServerOptions(config.LimitsConfig{MaxConnectionAge: 30 * time.Second})
	s.Len(opts, 1)

	opts = limitsServerOptions(config.LimitsConfig{MaxConnectionIdle: time.Second, MaxConnectionAge: time.Second})
	s.Len(opts, 1)
}

func (s *LimitsSuite) TestAllNonzeroProducesThreeOptions() {
	opts := limitsServerOptions(config.LimitsConfig{
		MaxRecvMessageSize:   1,
		MaxConcurrentStreams: 1,
		MaxConnectionIdle:    time.Second,
	})
	s.Len(opts, 3)
}

func TestLimitsSuite(t *testing.T) {
	suite.Run(t, new(LimitsSuite))
}
