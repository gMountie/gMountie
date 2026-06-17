package grpc

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	protomocks "go.gmountie.dev/gmountie/internal/mocks/pkg/proto"
	"go.gmountie.dev/gmountie/pkg/proto"
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

type ClientPoolSuite struct{ suite.Suite }

func (s *ClientPoolSuite) TestDataFileClientRoundRobins() {
	m0 := protomocks.NewMockRpcFileClient(s.T())
	m1 := protomocks.NewMockRpcFileClient(s.T())
	m2 := protomocks.NewMockRpcFileClient(s.T())
	c := &ClientImpl{file: m0, dataFileClients: []proto.RpcFileClient{m0, m1, m2}}
	counts := map[proto.RpcFileClient]int{}
	for i := 0; i < 9; i++ {
		counts[c.DataFileClient()]++
	}
	s.Equal(3, counts[m0])
	s.Equal(3, counts[m1])
	s.Equal(3, counts[m2])
}

func (s *ClientPoolSuite) TestDataFileClientSingleConnReturnsPrimary() {
	m0 := protomocks.NewMockRpcFileClient(s.T())
	c := &ClientImpl{file: m0, dataFileClients: []proto.RpcFileClient{m0}}
	for i := 0; i < 4; i++ {
		s.Same(m0, c.DataFileClient())
	}
}

func (s *ClientPoolSuite) TestFilePinsToPrimaryRegardlessOfRotation() {
	m0 := protomocks.NewMockRpcFileClient(s.T())
	m1 := protomocks.NewMockRpcFileClient(s.T())
	c := &ClientImpl{file: m0, dataFileClients: []proto.RpcFileClient{m0, m1}}
	_ = c.DataFileClient()
	s.Same(m0, c.File(), "File() must always return the primary stub")
}

func TestClientPoolSuite(t *testing.T) { suite.Run(t, new(ClientPoolSuite)) }
