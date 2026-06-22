package metrics

import (
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/suite"
)

type RecorderSuite struct {
	suite.Suite
	reg *prometheus.Registry
	m   *Metrics
}

func (s *RecorderSuite) SetupTest() {
	s.reg = prometheus.NewRegistry()
	s.m = NewMetrics()
	s.Require().NoError(s.m.Register(s.reg))
}

func (s *RecorderSuite) TestMetricsSatisfiesRecorder() {
	var r Recorder = s.m
	r.ObserveOp("Read", 0.005, "FS_OK")
	mf, err := s.reg.Gather()
	s.Require().NoError(err)
	var found bool
	for _, f := range mf {
		if f.GetName() == "gmountie_client_op_seconds" {
			found = true
		}
	}
	s.True(found, "op latency histogram should be registered and observed")
}

func TestRecorderSuite(t *testing.T) { suite.Run(t, new(RecorderSuite)) }
