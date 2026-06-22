package io

import (
	"context"
	"testing"

	"go.gmountie.dev/gmountie/pkg/proto"
	"github.com/stretchr/testify/suite"
)

// recordingBackend records the last method called and returns canned values.
type recordingBackend struct {
	FileSystemBackend // embed so unimplemented methods panic loudly if hit unexpectedly
	lastCall string
	closed   bool
}

func (r *recordingBackend) Stat(_ context.Context, path string) (*Attr, proto.FsError) {
	r.lastCall = "Stat:" + path
	return &Attr{Ino: 7}, proto.FsError_FS_OK
}
func (r *recordingBackend) Close() error { r.closed = true; return nil }

type PassthroughSuite struct {
	suite.Suite
	inner *recordingBackend
	pt    *PassthroughBackend
}

func (s *PassthroughSuite) SetupTest() {
	s.inner = &recordingBackend{}
	s.pt = &PassthroughBackend{Inner: s.inner}
}

func (s *PassthroughSuite) TestForwardsStat() {
	attr, st := s.pt.Stat(context.Background(), "/x")
	s.Equal(proto.FsError_FS_OK, st)
	s.Equal(uint64(7), attr.Ino)
	s.Equal("Stat:/x", s.inner.lastCall)
}

func (s *PassthroughSuite) TestForwardsClose() {
	s.NoError(s.pt.Close())
	s.True(s.inner.closed)
}

// Compile-time assertion: PassthroughBackend implements the full interface.
var _ FileSystemBackend = (*PassthroughBackend)(nil)

func TestPassthroughSuite(t *testing.T) { suite.Run(t, new(PassthroughSuite)) }
