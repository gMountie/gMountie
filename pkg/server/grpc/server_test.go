package grpc

import (
	"net"
	"testing"
	"time"

	"gmountie/pkg/server/config"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
)

// TestStartMetricsServer_PortInUseDoesNotPanic verifies that when the
// metrics port is already occupied, startMetricsServer logs and returns
// instead of crashing the process via log.Fatal.
func TestStartMetricsServer_PortInUseDoesNotPanic(t *testing.T) {
	// Occupy :9090 so the metrics server's ListenAndServe will fail.
	blocker, err := net.Listen("tcp", ":9090")
	require.NoError(t, err, "if this fails, :9090 was already busy externally")
	defer blocker.Close()

	s := &Server{
		config: &config.Config{
			Server: &config.ServerConfig{Address: "127.0.0.1", Port: 0, Metrics: true},
		},
		server:        grpc.NewServer(),
		metricsServer: nil,
	}
	// Initialise then start. Both must complete without exiting the process.
	s.initMetricsServer()
	require.NotNil(t, s.metricsServer)

	// Replace the no-op global mux with a fresh one for hygiene.
	// (We just need to confirm the goroutine doesn't crash us.)
	done := make(chan struct{})
	go func() {
		defer close(done)
		s.startMetricsServer()
		// startMetricsServer launches a goroutine; give it a beat to fail.
		time.Sleep(150 * time.Millisecond)
	}()

	select {
	case <-done:
		// If we got here without the test binary exiting via log.Fatal, we win.
		assert.True(t, true)
	case <-time.After(2 * time.Second):
		t.Fatal("startMetricsServer hung")
	}
}
