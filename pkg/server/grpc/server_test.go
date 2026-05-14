package grpc

import (
	"net"
	"testing"
	"time"

	"gmountie/pkg/server/config"

	"github.com/stretchr/testify/suite"
	"google.golang.org/grpc"
)

type ServerTestSuite struct {
	suite.Suite
}

// TestStartMetricsServer_PortInUseDoesNotPanic verifies that when the
// metrics port is already occupied, startMetricsServer logs and returns
// instead of crashing the process via log.Fatal.
func (s *ServerTestSuite) TestStartMetricsServer_PortInUseDoesNotPanic() {
	// Occupy :9090 so the metrics server's ListenAndServe will fail.
	blocker, err := net.Listen("tcp", ":9090")
	s.Require().NoError(err, "if this fails, :9090 was already busy externally")
	defer blocker.Close()

	srv := &Server{
		config: &config.Config{
			Server: &config.ServerConfig{Address: "127.0.0.1", Port: 0, Metrics: true},
		},
		server:        grpc.NewServer(),
		metricsServer: nil,
	}
	// Initialise then start. Both must complete without exiting the process.
	srv.initMetricsServer()
	s.Require().NotNil(srv.metricsServer)

	// Replace the no-op global mux with a fresh one for hygiene.
	// (We just need to confirm the goroutine doesn't crash us.)
	done := make(chan struct{})
	go func() {
		defer close(done)
		srv.startMetricsServer()
		// startMetricsServer launches a goroutine; give it a beat to fail.
		time.Sleep(150 * time.Millisecond)
	}()

	select {
	case <-done:
		// If we got here without the test binary exiting via log.Fatal, we win.
		s.True(true)
	case <-time.After(2 * time.Second):
		s.T().Fatal("startMetricsServer hung")
	}
}

func TestServerTestSuite(t *testing.T) {
	suite.Run(t, new(ServerTestSuite))
}
