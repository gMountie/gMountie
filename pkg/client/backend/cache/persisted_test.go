package cache_test

import (
	"context"
	"testing"
	"time"

	iomocks "go.gmountie.dev/gmountie/internal/mocks/pkg/client/backend"
	"go.gmountie.dev/gmountie/pkg/client/backend"
	"go.gmountie.dev/gmountie/pkg/client/backend/cache"
	"go.gmountie.dev/gmountie/pkg/client/backend/cache/persist"
	"go.gmountie.dev/gmountie/pkg/proto"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"
)

type PersistedCacheSuite struct {
	suite.Suite
	dir string
}

func (s *PersistedCacheSuite) SetupTest() { s.dir = s.T().TempDir() }

func (s *PersistedCacheSuite) TestRestartReusesCachedAttr() {
	p1, err := persist.Open(persist.Options{Root: s.dir})
	s.Require().NoError(err)
	inner := iomocks.NewMockFileSystemBackend(s.T())
	cfg := cache.Config{
		SubscribeEnabled: true, // keep tracker unverified so gating path is exercised
		MemoryMaxBytes:   1 << 20,
		ChunkSizeBytes:   1 << 16,
		AttrTTL:          time.Hour,
		DirTTL:           time.Hour,
		NegativeTTL:      time.Minute,
	}
	b1 := cache.NewCachedBackend(inner, cfg, p1, nil, "", nil, nil)

	inner.EXPECT().Stat(mock.Anything, "f").Return(&backend.Attr{Ino: 42, Size: 7}, proto.FsError_FS_OK).Once()
	inner.EXPECT().Close().Return(nil).Once()
	_, st := b1.Stat(context.Background(), "f")
	s.Require().Equal(proto.FsError_FS_OK, st)
	// Close the backend, which owns p1 and closes it.
	s.Require().NoError(b1.Close())

	// Restart with the same dir; Stat must not hit inner.Stat (the attr
	// is loaded from persist). The validity tracker starts unverified, so
	// GetAttrIfChanged is called once to confirm the cached version is still
	// current — this is correct Sub-spec D behaviour.
	p2, err := persist.Open(persist.Options{Root: s.dir})
	s.Require().NoError(err)
	inner2 := iomocks.NewMockFileSystemBackend(s.T())
	inner2.EXPECT().GetAttrIfChanged(mock.Anything, "f", uint64(0)).Return(nil, true, proto.FsError_FS_OK).Once()
	inner2.EXPECT().Close().Return(nil).Once()
	b2 := cache.NewCachedBackend(inner2, cfg, p2, nil, "", nil, nil)
	defer func() { _ = b2.Close() }()
	a, st := b2.Stat(context.Background(), "f")
	s.Require().Equal(proto.FsError_FS_OK, st)
	s.Assert().Equal(uint64(42), a.Ino)
	// inner2.Stat must NOT be called — the attr comes from persist.
}

func TestPersistedCacheSuite(t *testing.T) { suite.Run(t, new(PersistedCacheSuite)) }
