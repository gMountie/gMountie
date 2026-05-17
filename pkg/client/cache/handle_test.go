package cache

import (
	"testing"

	iomocks "gmountie/internal/mocks/pkg/client/io"
	"gmountie/pkg/client/io"

	"github.com/stretchr/testify/suite"
)

type CachedHandleTestSuite struct {
	suite.Suite
}

func (s *CachedHandleTestSuite) TestPathAndUnwrap() {
	inner := iomocks.NewMockFileHandle(s.T())
	inner.EXPECT().Path().Return("ignored-inner").Maybe()
	h := newCachedHandle(inner, "/wrapped/path")
	s.Assert().Equal("/wrapped/path", h.Path())
	got, ok := h.Unwrap().(io.FileHandle)
	s.Require().True(ok)
	s.Assert().Same(inner, got)
}

func (s *CachedHandleTestSuite) TestUnwrapChainTerminatesAtInner() {
	// Two layers: cachedHandle wraps cachedHandle wraps a leaf.
	leaf := iomocks.NewMockFileHandle(s.T())
	leaf.EXPECT().Unwrap().Return(leaf).Maybe() // leaf is its own unwrap, per Sub-spec A's contract
	mid := newCachedHandle(leaf, "/mid")
	outer := newCachedHandle(mid, "/outer")
	// One Unwrap goes to mid, the next to leaf.
	s.Assert().Same(mid, outer.Unwrap())
	s.Assert().Same(leaf, outer.Unwrap().(*cachedHandle).Unwrap())
}

func TestCachedHandleTestSuite(t *testing.T) {
	suite.Run(t, new(CachedHandleTestSuite))
}
