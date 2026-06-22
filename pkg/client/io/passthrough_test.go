package io

import (
	"context"
	"reflect"
	"testing"

	"github.com/stretchr/testify/suite"

	"go.gmountie.dev/gmountie/pkg/proto"
)

// recordingBackend records the last method called and returns canned values.
type recordingBackend struct {
	FileSystemBackend // embed so unimplemented methods panic loudly if hit unexpectedly
	lastCall          string
	closed            bool
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
	s.Require().NoError(s.pt.Close())
	s.True(s.inner.closed)
}

// Compile-time assertion: PassthroughBackend implements the full interface.
var _ FileSystemBackend = (*PassthroughBackend)(nil)

func TestPassthroughSuite(t *testing.T) { suite.Run(t, new(PassthroughSuite)) }

// wantMethods is the pinned method set of FileSystemBackend, in the sorted
// order reflect reports. It is the contract that every embedding layer (the
// cache and identity decorators, which embed PassthroughBackend and override
// only the ops they handle) is reviewed against.
var wantMethods = []string{
	"Access",
	"Allocate",
	"Close",
	"CopyFileRange",
	"Create",
	"Flush",
	"Fsync",
	"GetAttrIfChanged",
	"GetLk",
	"GetXAttr",
	"ListDir",
	"ListXAttr",
	"Lookup",
	"Lseek",
	"Mkdir",
	"Open",
	"Read",
	"Readlink",
	"Release",
	"RemoveXAttr",
	"Rename",
	"Rmdir",
	"SetAttr",
	"SetLk",
	"SetLkw",
	"SetXAttr",
	"Stat",
	"StatFs",
	"Symlink",
	"Unlink",
	"Write",
}

// TestFileSystemBackendMethodSet is the central guard that replaces the old
// "semantic layers must not embed PassthroughBackend" rule. Layers (cache,
// identity, future write-path/WAL) now embed PassthroughBackend and override
// only the ops they handle, so a newly added interface method would forward
// silently and bypass a layer that should have handled it. This test pins the
// interface's method set: adding/removing/renaming a method fails here, forcing
// a deliberate review of every embedding layer before the change lands.
func TestFileSystemBackendMethodSet(t *testing.T) {
	iface := reflect.TypeOf((*FileSystemBackend)(nil)).Elem()
	got := make([]string, 0, iface.NumMethod())
	for i := 0; i < iface.NumMethod(); i++ {
		got = append(got, iface.Method(i).Name)
	}
	// reflect reports interface methods already sorted by name; wantMethods is
	// kept in that same order so a direct compare is sound.
	if !reflect.DeepEqual(got, wantMethods) {
		t.Fatalf("FileSystemBackend changed (method added/removed/renamed).\n"+
			"Review EVERY semantic layer that embeds PassthroughBackend — cache "+
			"(does the new op need invalidation?), identity (does it carry uid/gid "+
			"to rewrite?), and any future write-path/WAL layer — then update "+
			"wantMethods here.\n  got:  %v\n  want: %v", got, wantMethods)
	}
}
