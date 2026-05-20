package metrics

import (
	"testing"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/suite"
)

type MetricsTestSuite struct {
	suite.Suite
	m *Metrics
}

func (s *MetricsTestSuite) SetupTest() { s.m = NewMetrics() }

func (s *MetricsTestSuite) TestOpenFilesIncDec() {
	s.m.OpenFilesInc("photos", "sess-1")
	s.m.OpenFilesInc("photos", "sess-1")
	s.m.OpenFilesDec("photos", "sess-1")
	s.Assert().Equal(1, int(testutil.ToFloat64(s.m.OpenFiles.WithLabelValues("photos", "sess-1"))))
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

func TestMetricsTestSuite(t *testing.T) { suite.Run(t, new(MetricsTestSuite)) }
