package observer

import (
	"context"
	"testing"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"

	iomocks "go.gmountie.dev/gmountie/internal/mocks/pkg/client/io"
	metricsmocks "go.gmountie.dev/gmountie/internal/mocks/pkg/client/metrics"
	"go.gmountie.dev/gmountie/pkg/proto"
)

type MetricsObserverSuite struct {
	suite.Suite
	inner *iomocks.MockFileSystemBackend
	rec   *metricsmocks.MockRecorder
}

func (s *MetricsObserverSuite) SetupTest() {
	s.inner = iomocks.NewMockFileSystemBackend(s.T())
	s.rec = metricsmocks.NewMockRecorder(s.T())
}

func (s *MetricsObserverSuite) TestStatForwardsAndRecordsOp() {
	s.inner.EXPECT().Stat(mock.Anything, "/x").Return(nil, proto.FsError_FS_OK).Once()
	s.rec.EXPECT().ObserveOp("Stat", mock.AnythingOfType("float64"), "FS_OK").Once()

	layer := NewMetricsLayer(s.inner, s.rec)
	_, st := layer.Stat(context.Background(), "/x")
	s.Equal(proto.FsError_FS_OK, st)
}

func (s *MetricsObserverSuite) TestReadForwardsAndRecordsOp() {
	buf := make([]byte, 4)
	s.inner.EXPECT().Read(mock.Anything, mock.Anything, int64(0), buf).Return(4, proto.FsError_FS_OK).Once()
	s.rec.EXPECT().ObserveOp("Read", mock.AnythingOfType("float64"), "FS_OK").Once()

	layer := NewMetricsLayer(s.inner, s.rec)
	n, st := layer.Read(context.Background(), nil, 0, buf)
	s.Equal(4, n)
	s.Equal(proto.FsError_FS_OK, st)
}

func TestMetricsObserverSuite(t *testing.T) { suite.Run(t, new(MetricsObserverSuite)) }
