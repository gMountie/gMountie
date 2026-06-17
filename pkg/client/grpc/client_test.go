package grpc

import (
	"sync/atomic"
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

// makePool builds a ClientImpl with n mock connections and an initialised inflight slice.
func (s *ClientPoolSuite) makePool(mocks ...proto.RpcFileClient) *ClientImpl {
	c := &ClientImpl{
		file:            mocks[0],
		dataFileClients: mocks,
		inflight:        make([]atomic.Int64, len(mocks)),
	}
	return c
}

// TestSequentialAffinityStaysOnPrimary is the key regression guard: a
// sequential single-stream workload (pick→release→pick→release) must always
// land on conn 0 (the primary/warm connection) because ties resolve to the
// lowest index and each release resets the counter.
func (s *ClientPoolSuite) TestSequentialAffinityStaysOnPrimary() {
	m0 := protomocks.NewMockRpcFileClient(s.T())
	m1 := protomocks.NewMockRpcFileClient(s.T())
	m2 := protomocks.NewMockRpcFileClient(s.T())
	c := s.makePool(m0, m1, m2)
	for i := 0; i < 6; i++ {
		got, release := c.DataFileClient()
		s.Same(m0, got, "sequential pick %d must return the primary (conn 0)", i)
		release()
	}
}

// TestFanOutUnderConcurrentLoad verifies that picking N connections without
// releasing any in between distributes across all connections once (each conn
// gets exactly one stream before any counter is decremented).
func (s *ClientPoolSuite) TestFanOutUnderConcurrentLoad() {
	m0 := protomocks.NewMockRpcFileClient(s.T())
	m1 := protomocks.NewMockRpcFileClient(s.T())
	m2 := protomocks.NewMockRpcFileClient(s.T())
	c := s.makePool(m0, m1, m2)
	var releases []func()
	counts := map[proto.RpcFileClient]int{}
	// Pick 3 times without releasing; each pick finds the next least-loaded conn.
	for i := 0; i < 3; i++ {
		got, release := c.DataFileClient()
		counts[got]++
		releases = append(releases, release)
	}
	s.Equal(1, counts[m0], "each conn should be picked once during fan-out")
	s.Equal(1, counts[m1])
	s.Equal(1, counts[m2])
	// Release all; in-flight counts return to 0.
	for _, r := range releases {
		r()
	}
	// After release the primary wins ties again.
	got, release := c.DataFileClient()
	s.Same(m0, got, "after releasing all, primary should win tie again")
	release()
}

// TestReleaseDecrements verifies that release actually decrements the counter
// so a second pick on a formerly-loaded conn returns to contention.
func (s *ClientPoolSuite) TestReleaseDecrements() {
	m0 := protomocks.NewMockRpcFileClient(s.T())
	m1 := protomocks.NewMockRpcFileClient(s.T())
	c := s.makePool(m0, m1)
	// Pick conn0 (wins tie), don't release → conn0 has 1 in-flight.
	_, release0 := c.DataFileClient()
	// Next pick must choose conn1 (least loaded = 0).
	got, release1 := c.DataFileClient()
	s.Same(m1, got, "conn1 should win when conn0 has an unreleased stream")
	// Release conn0; it drops to 0, tying with conn1 (1). Next pick: conn0.
	release0()
	got2, release2 := c.DataFileClient()
	s.Same(m0, got2, "conn0 should win tie after its release decrements back to 0")
	release1()
	release2()
}

func (s *ClientPoolSuite) TestDataFileClientSingleConnReturnsPrimary() {
	m0 := protomocks.NewMockRpcFileClient(s.T())
	c := &ClientImpl{file: m0, dataFileClients: []proto.RpcFileClient{m0}}
	// n=1 path: no inflight slice needed (returns c.file directly).
	for i := 0; i < 4; i++ {
		got, release := c.DataFileClient()
		s.Same(m0, got)
		release() // no-op; must not panic
	}
}

func (s *ClientPoolSuite) TestFilePinsToPrimaryRegardlessOfPick() {
	m0 := protomocks.NewMockRpcFileClient(s.T())
	m1 := protomocks.NewMockRpcFileClient(s.T())
	c := s.makePool(m0, m1)
	_, release := c.DataFileClient()
	defer release()
	s.Same(m0, c.File(), "File() must always return the primary stub")
}

func TestClientPoolSuite(t *testing.T) { suite.Run(t, new(ClientPoolSuite)) }
