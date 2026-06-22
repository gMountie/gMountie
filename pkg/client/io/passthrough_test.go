package io

import (
	"context"
	"reflect"
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

// semanticBackendTypes lists every SEMANTIC layer (changes behavior). Add new
// ones here; the test fails if any embeds PassthroughBackend (silent-forward
// hazard). Observer layers (metrics/trace/audit) are intentionally absent.
func semanticBackendTypes() []reflect.Type {
	return []reflect.Type{
		reflect.TypeOf(BackendClient{}),
	}
}

func (s *PassthroughSuite) TestSemanticLayersDoNotEmbedPassthrough() {
	ptName := reflect.TypeOf(PassthroughBackend{}).Name()
	for _, t := range semanticBackendTypes() {
		for i := 0; i < t.NumField(); i++ {
			f := t.Field(i)
			s.Falsef(f.Anonymous && f.Type.Name() == ptName,
				"%s embeds PassthroughBackend; semantic layers must implement the full interface explicitly", t.Name())
		}
	}
}

func TestPassthroughSuite(t *testing.T) { suite.Run(t, new(PassthroughSuite)) }
