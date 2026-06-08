package grpc

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func TestClientRetryWindowAndLifetime(t *testing.T) {
	c, err := NewClient("127.0.0.1:9",
		WithRetryWindow(42*time.Second),
		WithDialOptions([]grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials())}),
	)
	require.NoError(t, err)
	require.Equal(t, 42*time.Second, c.(*ClientImpl).RetryWindow())
	require.NotNil(t, c.(*ClientImpl).Lifetime())
	select {
	case <-c.(*ClientImpl).Lifetime().Done():
		t.Fatal("lifetime cancelled before Close")
	default:
	}

	// Close must cancel the lifetime context — the property long retries rely
	// on to abort promptly on unmount.
	require.NoError(t, c.Close())
	select {
	case <-c.(*ClientImpl).Lifetime().Done():
	default:
		t.Fatal("lifetime not cancelled after Close")
	}
}
