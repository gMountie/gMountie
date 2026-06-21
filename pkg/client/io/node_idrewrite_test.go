package io_test

import (
	"context"
	"syscall"
	"testing"

	iomocks "go.gmountie.dev/gmountie/internal/mocks/pkg/client/io"
	clientio "go.gmountie.dev/gmountie/pkg/client/io"
	"go.gmountie.dev/gmountie/pkg/proto"

	"github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"
)

// NodeIDRewriteSuite verifies that IDRewriter is applied by the node adapter:
//   - Inbound: attrs returned from Getattr have server uids/gids translated to
//     local display ids.
//   - Outbound: Setattr with a local uid/gid calls backend.Chown with the
//     corresponding server ids.
type NodeIDRewriteSuite struct {
	suite.Suite
	backend *iomocks.MockFileSystemBackend
	root    fs.InodeEmbedder
}

func (s *NodeIDRewriteSuite) SetupTest() {
	s.backend = iomocks.NewMockFileSystemBackend(s.T())
	rw := clientio.NewIDRewriter(
		&clientio.Identity{Uid: 1001, Gid: 1001, Gids: []uint32{1001}},
		500, 500,
	)
	s.root = clientio.NewMountieRoot(s.backend, rw, false)
	fs.NewNodeFS(s.root, &fs.Options{})
}

func rootAsIDRW[T any](s *NodeIDRewriteSuite) T {
	v, ok := s.root.(T)
	s.Require().True(ok, "root does not implement %T", *new(T))
	return v
}

// TestGetattr_InboundRewrite verifies that when the backend returns Attr with
// the server uid/gid (1001/1001), the node adapter rewrites them to the local
// display uid/gid (500/500) before returning the fuse.AttrOut to the kernel.
func (s *NodeIDRewriteSuite) TestGetattr_InboundRewrite() {
	s.backend.EXPECT().Stat(mock.Anything, "").Return(
		&clientio.Attr{Ino: 1, Mode: fuse.S_IFREG | 0o644, Uid: 1001, Gid: 1001}, proto.FsError_FS_OK,
	)
	out := &fuse.AttrOut{}
	errno := rootAsIDRW[fs.NodeGetattrer](s).Getattr(context.Background(), nil, out)
	s.Require().Equal(syscall.Errno(0), errno)
	s.Assert().Equal(uint32(500), out.Uid, "uid should be rewritten to local")
	s.Assert().Equal(uint32(500), out.Gid, "gid should be rewritten to local")
}

// TestGetattr_OtherUser_MapsToNobody verifies that attrs belonging to a
// different server user (not 1001) are mapped to nobody (65534).
func (s *NodeIDRewriteSuite) TestGetattr_OtherUser_MapsToNobody() {
	s.backend.EXPECT().Stat(mock.Anything, "").Return(
		&clientio.Attr{Ino: 2, Mode: fuse.S_IFREG | 0o644, Uid: 9999, Gid: 9999}, proto.FsError_FS_OK,
	)
	out := &fuse.AttrOut{}
	errno := rootAsIDRW[fs.NodeGetattrer](s).Getattr(context.Background(), nil, out)
	s.Require().Equal(syscall.Errno(0), errno)
	s.Assert().Equal(uint32(65534), out.Uid, "foreign uid should map to nobody")
	s.Assert().Equal(uint32(65534), out.Gid, "foreign gid should map to nobody")
}

// TestSetattr_OutboundChownRewrite verifies that when the caller sets owner to
// local uid/gid (500/500), the node adapter sends the single-RPC SetAttr with
// the corresponding server ids (1001/1001) — and that the inbound rewrite
// fires on the attrs returned in the reply (no trailing Stat anymore).
func (s *NodeIDRewriteSuite) TestSetattr_OutboundChownRewrite() {
	s.backend.EXPECT().SetAttr(mock.Anything, "", mock.MatchedBy(func(in clientio.SetAttrIn) bool {
		return in.Valid == fuse.FATTR_UID|fuse.FATTR_GID &&
			in.Uid == 1001 && in.Gid == 1001
	})).Return(
		// Reply carries server ids so we can also verify the inbound rewrite
		// fires on the way out.
		&clientio.Attr{Ino: 1, Mode: fuse.S_IFREG | 0o644, Uid: 1001, Gid: 1001}, proto.FsError_FS_OK,
	).Once()

	in := &fuse.SetAttrIn{}
	in.Valid = fuse.FATTR_UID | fuse.FATTR_GID
	in.Owner = fuse.Owner{Uid: 500, Gid: 500}

	out := &fuse.AttrOut{}
	errno := rootAsIDRW[fs.NodeSetattrer](s).Setattr(context.Background(), nil, in, out)
	s.Require().Equal(syscall.Errno(0), errno)
	// Inbound rewrite fires on the reply attrs, confirming the full path.
	s.Assert().Equal(uint32(500), out.Uid, "returned uid should be local after inbound rewrite")
	s.Assert().Equal(uint32(500), out.Gid, "returned gid should be local after inbound rewrite")
}

func TestNodeIDRewriteSuite(t *testing.T) {
	suite.Run(t, new(NodeIDRewriteSuite))
}
