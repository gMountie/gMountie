package metrics

import (
	"testing"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/suite"
)

type ClientMetricsTestSuite struct {
	suite.Suite
	m *Metrics
}

func (s *ClientMetricsTestSuite) SetupTest() { s.m = NewMetrics() }

func (s *ClientMetricsTestSuite) TestRetryInc() {
	s.m.RetryInc("Read", "Unavailable")
	s.m.RetryInc("Read", "Unavailable")
	s.Assert().Equal(2.0, testutil.ToFloat64(s.m.RetryTotal.WithLabelValues("Read", "Unavailable")))
}

func (s *ClientMetricsTestSuite) TestInFlightIncDec() {
	s.m.InFlightInc("Mkdir")
	s.m.InFlightInc("Mkdir")
	s.m.InFlightDec("Mkdir")
	s.Assert().Equal(1.0, testutil.ToFloat64(s.m.InFlight.WithLabelValues("Mkdir")))
}

func (s *ClientMetricsTestSuite) TestRetryHook_FiresWhenSet() {
	var seenOp, seenCode string
	SetRetryHook(func(op, code string) {
		seenOp = op
		seenCode = code
	})
	defer SetRetryHook(nil)

	OnRetry("Read", "Unavailable")
	s.Assert().Equal("Read", seenOp)
	s.Assert().Equal("Unavailable", seenCode)
}

func (s *ClientMetricsTestSuite) TestRetryHook_NoopWhenUnset() {
	SetRetryHook(nil)
	// must not panic
	OnRetry("Read", "Unavailable")
}

func TestClientMetricsTestSuite(t *testing.T) { suite.Run(t, new(ClientMetricsTestSuite)) }
