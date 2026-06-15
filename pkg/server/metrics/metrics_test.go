package metrics

import (
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/suite"
)

type MetricsTestSuite struct {
	suite.Suite
	m *Metrics
}

func (s *MetricsTestSuite) SetupTest() { s.m = NewMetrics() }

func (s *MetricsTestSuite) TestOpenFilesIncDec() {
	s.m.OpenFilesInc("photos")
	s.m.OpenFilesInc("photos")
	s.m.OpenFilesDec("photos")
	s.Assert().Equal(1, int(testutil.ToFloat64(s.m.OpenFiles.WithLabelValues("photos"))))
}

func (s *MetricsTestSuite) TestBytesAccumulate() {
	s.m.BytesAdd("photos", "in", 100)
	s.m.BytesAdd("photos", "in", 50)
	s.m.BytesAdd("photos", "out", 200)
	s.Assert().Equal(150, int(testutil.ToFloat64(s.m.Bytes.WithLabelValues("photos", "in"))))
	s.Assert().Equal(200, int(testutil.ToFloat64(s.m.Bytes.WithLabelValues("photos", "out"))))
}

func (s *MetricsTestSuite) TestRpcErrorsCounter() {
	s.m.RpcErrorInc("photos", "Read", "Unavailable")
	s.Assert().Equal(1, int(testutil.ToFloat64(s.m.RpcErrors.WithLabelValues("photos", "Read", "Unavailable"))))
}

func (s *MetricsTestSuite) TestRequestDurationObserves() {
	s.m.RequestDurationObserve("photos", "Read", 0.123)
	s.Assert().Equal(1, testutil.CollectAndCount(s.m.RequestDuration))
}

func (s *MetricsTestSuite) TestSessionsActive() {
	s.m.SessionsActiveInc()
	s.m.SessionsActiveInc()
	s.m.SessionsActiveDec()
	s.Assert().Equal(1, int(testutil.ToFloat64(s.m.SessionsActive)))
}

func (s *MetricsTestSuite) TestSessionsReaped() {
	s.m.SessionsReapedInc("grace_expired")
	s.m.SessionsReapedInc("grace_expired")
	s.m.SessionsReapedInc("revoked")
	s.Assert().Equal(2, int(testutil.ToFloat64(s.m.SessionsReaped.WithLabelValues("grace_expired"))))
	s.Assert().Equal(1, int(testutil.ToFloat64(s.m.SessionsReaped.WithLabelValues("revoked"))))
}

func (s *MetricsTestSuite) TestAuthFailures() {
	s.m.AuthFailureInc("Read", "revoked")
	s.m.AuthFailureInc("Read", "revoked")
	s.m.AuthFailureInc("Create", "nil_user")
	s.Assert().Equal(2, int(testutil.ToFloat64(s.m.AuthFailures.WithLabelValues("Read", "revoked"))))
	s.Assert().Equal(1, int(testutil.ToFloat64(s.m.AuthFailures.WithLabelValues("Create", "nil_user"))))
}

func (s *MetricsTestSuite) TestTLSReloadFailures() {
	s.m.TLSReloadFailureInc()
	s.m.TLSReloadFailureInc()
	s.Assert().Equal(2, int(testutil.ToFloat64(s.m.TLSReloadFailures)))
}

func (s *MetricsTestSuite) TestTLSReloadSucceededStampsGauge() {
	s.Assert().Equal(0, int(testutil.ToFloat64(s.m.TLSLastReloadUnixtime)))
	s.m.TLSReloadSucceeded()
	s.Assert().Positive(testutil.ToFloat64(s.m.TLSLastReloadUnixtime),
		"TLSReloadSucceeded must stamp the gauge to the current time")
}

// TestRegisterCountTolerant pins the adopt-switch: registering twice against a
// shared registerer must not error and must not mis-adopt one plain
// gauge/counter onto another (OB-M2 adopt-switch hazard).
func (s *MetricsTestSuite) TestRegisterCountTolerant() {
	reg := prometheus.NewRegistry()
	m1 := NewMetrics()
	s.Require().NoError(m1.Register(reg))
	m2 := NewMetrics()
	s.Require().NoError(m2.Register(reg), "re-register against the same registerer must be tolerated")
}

func TestMetricsTestSuite(t *testing.T) { suite.Run(t, new(MetricsTestSuite)) }
