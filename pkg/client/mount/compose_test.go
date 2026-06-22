package mount

import (
	"testing"

	"go.gmountie.dev/gmountie/pkg/client/io"
	"github.com/stretchr/testify/suite"
)

type ComposeSuite struct{ suite.Suite }

func (s *ComposeSuite) TestFoldsOutermostFirstRegardlessOfSliceOrder() {
	var order []string
	mk := func(name string) func(io.FileSystemBackend) io.FileSystemBackend {
		return func(inner io.FileSystemBackend) io.FileSystemBackend {
			order = append(order, name)
			return inner // identity wrapper for the test
		}
	}
	transport := &PassthroughCounter{} // see helper below
	// Deliberately out-of-order slice: cache before observer.
	layers := []backendLayer{
		{pos: posCache, build: mk("cache")},
		{pos: posObserver, build: mk("observer")},
	}
	got := composeBackend(transport, layers)
	s.NotNil(got)
	// Innermost built first: cache (closer to transport) before observer (outermost).
	s.Equal([]string{"cache", "observer"}, order)
}

// PassthroughCounter is a minimal io.FileSystemBackend for composition tests.
type PassthroughCounter struct{ io.PassthroughBackend }

func TestComposeSuite(t *testing.T) { suite.Run(t, new(ComposeSuite)) }
