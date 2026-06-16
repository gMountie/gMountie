package grpc

import (
	"testing"

	"go.gmountie.dev/gmountie/pkg/client/config"

	"github.com/stretchr/testify/suite"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// NewClientDefaultsSuite verifies that NewClient with no options produces
// PerFileConfig values identical to the config.Default* constants.  The test
// references the constants — not literals — so a future constant change forces
// a deliberate review rather than silently widening the gap again.
type NewClientDefaultsSuite struct {
	suite.Suite
}

func (s *NewClientDefaultsSuite) TestPerFileConfigMatchesConfigDefaults() {
	c, err := NewClient("127.0.0.1:9",
		WithDialOptions([]grpc.DialOption{
			grpc.WithTransportCredentials(insecure.NewCredentials()),
		}),
	)
	s.Require().NoError(err)
	defer func() { _ = c.Close() }()

	pf := c.(*ClientImpl).PerFileConfig()
	s.Equal(config.DefaultReadaheadChunkBytes, pf.ReadaheadChunkBytes,
		"ReadaheadChunkBytes must match config default (was 16× smaller before the fix)")
	s.Equal(config.DefaultReadaheadThreshold, pf.ReadaheadThreshold,
		"ReadaheadThreshold must match config default")
	s.Equal(config.DefaultReadaheadWindow, pf.ReadaheadWindow,
		"ReadaheadWindow must match config default (was hardcoded 1, not wired to the config default, before the fix)")
	s.Equal(config.DefaultWriteCoalesceBytes, pf.WriteCoalesceBytes,
		"WriteCoalesceBytes must match config default")
}

func (s *NewClientDefaultsSuite) TestTimeoutsMatchConfigDefaults() {
	c, err := NewClient("127.0.0.1:9",
		WithDialOptions([]grpc.DialOption{
			grpc.WithTransportCredentials(insecure.NewCredentials()),
		}),
	)
	s.Require().NoError(err)
	defer func() { _ = c.Close() }()

	s.Equal(config.DefaultRpcTimeoutMeta, c.(*ClientImpl).MetaTimeout())
	s.Equal(config.DefaultRpcTimeoutIO, c.(*ClientImpl).IOTimeout())
	s.Equal(config.DefaultRpcRetryWindow, c.(*ClientImpl).RetryWindow())
}

func TestNewClientDefaultsSuite(t *testing.T) {
	suite.Run(t, new(NewClientDefaultsSuite))
}
