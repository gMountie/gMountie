package server

import (
	"context"
	"fmt"
	"net"
	"testing"
	"time"

	"gmountie/pkg/server/config"

	"github.com/stretchr/testify/require"
)

// TestStart_ContextCancellationShutsDownGracefully verifies that cancelling
// the context passed to Start triggers a graceful stop and the function
// returns nil within a reasonable bound.
func TestStart_ContextCancellationShutsDownGracefully(t *testing.T) {
	// Find a free port so the test isn't flaky on busy machines.
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	port := uint(lis.Addr().(*net.TCPAddr).Port)
	lis.Close()

	cfg := &config.Config{
		Server:  &config.ServerConfig{Address: "127.0.0.1", Port: port, Metrics: false},
		Auth:    &config.NoneAuthConfig{},
		Volumes: []*config.VolumeConfig{},
	}

	ctx, cancel := context.WithCancel(context.Background())

	startErr := make(chan error, 1)
	go func() {
		startErr <- Start(ctx, cfg)
	}()

	// Give the server a moment to bind.
	require.Eventually(t, func() bool {
		c, err := net.DialTimeout("tcp", fmt.Sprintf("127.0.0.1:%d", port), 10*time.Millisecond)
		if err == nil {
			c.Close()
			return true
		}
		return false
	}, 2*time.Second, 10*time.Millisecond, "server did not bind in time")
	cancel()

	select {
	case err := <-startErr:
		require.NoError(t, err, "graceful shutdown should not return an error")
	case <-time.After(5 * time.Second):
		t.Fatal("Start did not return within 5s of context cancel")
	}
}
