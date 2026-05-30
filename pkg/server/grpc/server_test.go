package grpc

import (
	"testing"

	"go.gmountie.dev/gmountie/pkg/server/config"

	"github.com/stretchr/testify/suite"
)

type ServerTestSuite struct {
	suite.Suite
}

// TestInitMetricsServer_RegistersCollector verifies that with metrics
// enabled, initMetricsServer wires up the gRPC server-metrics collector
// and attaches its interceptors. The HTTP /metrics endpoint moved to
// pkg/server/ops, so this no longer covers the old listener path.
func (s *ServerTestSuite) TestInitMetricsServer_RegistersCollector() {
	srv := &Server{
		config: &config.Config{
			Server: &config.ServerConfig{Address: "127.0.0.1", Port: 0, Metrics: true},
		},
	}
	srv.initMetricsServer()
	s.Require().NotNil(srv.metricsServer)
	s.Assert().NotEmpty(srv.extraUnaryInterceptors)
	s.Assert().NotEmpty(srv.extraStreamInterceptors)
}

// TestInitMetricsServer_DisabledNoOp verifies that with metrics turned
// off, no collector is created and no interceptors are appended.
func (s *ServerTestSuite) TestInitMetricsServer_DisabledNoOp() {
	srv := &Server{
		config: &config.Config{
			Server: &config.ServerConfig{Address: "127.0.0.1", Port: 0, Metrics: false},
		},
	}
	srv.initMetricsServer()
	s.Assert().Nil(srv.metricsServer)
	s.Assert().Empty(srv.extraUnaryInterceptors)
	s.Assert().Empty(srv.extraStreamInterceptors)
}

func TestServerTestSuite(t *testing.T) {
	suite.Run(t, new(ServerTestSuite))
}
