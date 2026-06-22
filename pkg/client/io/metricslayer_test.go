package io

import (
	"context"
	"testing"

	metricsmocks "go.gmountie.dev/gmountie/internal/mocks/pkg/client/metrics"
	"go.gmountie.dev/gmountie/pkg/proto"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"
)

type MetricsLayerSuite struct {
	suite.Suite
	inner *recordingBackend // from passthrough_test.go (same package)
	rec   *metricsmocks.MockRecorder
}

func (s *MetricsLayerSuite) SetupTest() {
	s.inner = &recordingBackend{}
	s.rec = metricsmocks.NewMockRecorder(s.T())
}

func (s *MetricsLayerSuite) TestStatRecordsOpLatency() {
	s.rec.EXPECT().ObserveOp("Stat", mock.AnythingOfType("float64"), "FS_OK").Once()
	layer := NewMetricsLayer(s.inner, s.rec)
	_, st := layer.Stat(context.Background(), "/x")
	s.Equal(proto.FsError_FS_OK, st)
	s.Equal("Stat:/x", s.inner.lastCall) // still forwarded
}

func TestMetricsLayerSuite(t *testing.T) { suite.Run(t, new(MetricsLayerSuite)) }
