package controller

import (
	"context"
	"fmt"
	"os"
	"path/filepath"

	pathfs2 "go.gmountie.dev/gmountie/internal/mocks/github.com/hanwen/go-fuse/v2/fuse/pathfs"
	"go.gmountie.dev/gmountie/pkg/proto"
	serverio "go.gmountie.dev/gmountie/pkg/server/io"
	"go.gmountie.dev/gmountie/pkg/server/service"

	"github.com/hanwen/go-fuse/v2/fuse"
	"github.com/stretchr/testify/mock"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

// stubReadDirStream implements proto.RpcFs_ReadDirServer for tests, recording
// every batch sent. The ReadDir handler is synchronous (no goroutines), so no
// locking is needed.
type stubReadDirStream struct {
	ctx  context.Context
	sent []*proto.ReadDirBatch
}

func (s *stubReadDirStream) Send(b *proto.ReadDirBatch) error { s.sent = append(s.sent, b); return nil }
func (s *stubReadDirStream) Context() context.Context         { return s.ctx }
func (s *stubReadDirStream) SetHeader(metadata.MD) error      { return nil }
func (s *stubReadDirStream) SendHeader(metadata.MD) error     { return nil }
func (s *stubReadDirStream) SetTrailer(metadata.MD)           {}
func (s *stubReadDirStream) SendMsg(any) error                { return nil }
func (s *stubReadDirStream) RecvMsg(any) error                { return nil }

// bindConfinedVolume points BindIdentity at a real confined loopback FS over
// a fresh tmpdir — the ReadDir handler is exercised against real ReadDirPlus
// mechanics, matching how the production bind chain behaves.
func (s *RpcServerTestSuite) bindConfinedVolume() string {
	dir := s.T().TempDir()
	fs, err := serverio.NewConfinedLoopbackFileSystem(dir)
	s.Require().NoError(err)
	s.fsService.On("BindIdentity", mock.Anything, "testVolume", mock.Anything).
		Return(fs, service.Identity{}, nil)
	return dir
}

// TestReadDirStreamsInBatches: 1200 entries with plus=true stream as
// ceil(1200/512)=3 batches sized 512/512/176, every entry with attrs.
func (s *RpcServerTestSuite) TestReadDirStreamsInBatches() {
	dir := s.bindConfinedVolume()
	const n = 1200
	for i := range n {
		s.Require().NoError(os.WriteFile(filepath.Join(dir, fmt.Sprintf("f%04d", i)), []byte("x"), 0o644))
	}
	stream := &stubReadDirStream{ctx: context.Background()}

	err := s.server.ReadDir(&proto.ReadDirRequest{
		Volume: "testVolume", Path: "/", Plus: true, Caller: CreateCaller(0, 0, 0),
	}, stream)

	s.Require().NoError(err)
	s.Require().Len(stream.sent, 3)
	s.Len(stream.sent[0].Entries, 512)
	s.Len(stream.sent[1].Entries, 512)
	s.Len(stream.sent[2].Entries, 176)
	total := 0
	for _, b := range stream.sent {
		s.Equal(proto.FsError_FS_OK, b.Status)
		for _, e := range b.Entries {
			s.Require().NotNil(e.Entry)
			s.Require().NotNil(e.Attributes, "plus=true must carry attrs for %q", e.Entry.Name)
			s.NotZero(e.Attributes.Ino)
			total++
		}
	}
	s.Equal(n, total)
}

// TestReadDirPlusFalseOmitsAttrs: plus=false takes the OpenDir path — entries
// arrive without attributes.
func (s *RpcServerTestSuite) TestReadDirPlusFalseOmitsAttrs() {
	dir := s.bindConfinedVolume()
	s.Require().NoError(os.WriteFile(filepath.Join(dir, "a.txt"), []byte("a"), 0o644))
	stream := &stubReadDirStream{ctx: context.Background()}

	err := s.server.ReadDir(&proto.ReadDirRequest{
		Volume: "testVolume", Path: "/", Plus: false, Caller: CreateCaller(0, 0, 0),
	}, stream)

	s.Require().NoError(err)
	s.Require().Len(stream.sent, 1)
	s.Require().Len(stream.sent[0].Entries, 1)
	s.Equal("a.txt", stream.sent[0].Entries[0].Entry.Name)
	s.Nil(stream.sent[0].Entries[0].Attributes, "plus=false must not carry attrs")
}

// TestReadDirEmptyDir: an empty directory still produces ONE empty OK batch
// so the client can distinguish success-empty from no reply.
func (s *RpcServerTestSuite) TestReadDirEmptyDir() {
	s.bindConfinedVolume()
	stream := &stubReadDirStream{ctx: context.Background()}

	err := s.server.ReadDir(&proto.ReadDirRequest{
		Volume: "testVolume", Path: "/", Plus: true, Caller: CreateCaller(0, 0, 0),
	}, stream)

	s.Require().NoError(err)
	s.Require().Len(stream.sent, 1)
	s.Empty(stream.sent[0].Entries)
	s.Equal(proto.FsError_FS_OK, stream.sent[0].Status)
}

// TestReadDirBindError: an unknown volume surfaces the BindIdentity gRPC
// error directly and nothing is streamed.
func (s *RpcServerTestSuite) TestReadDirBindError() {
	bindErr := status.Error(codes.NotFound, "volume not found")
	s.fsService.On("BindIdentity", mock.Anything, "ghostVolume", mock.Anything).
		Return(nil, service.Identity{}, bindErr)
	stream := &stubReadDirStream{ctx: context.Background()}

	err := s.server.ReadDir(&proto.ReadDirRequest{
		Volume: "ghostVolume", Path: "/", Caller: CreateCaller(0, 0, 0),
	}, stream)

	s.Require().Error(err)
	s.Equal(codes.NotFound, status.Code(err))
	s.Empty(stream.sent)
}

// TestReadDirFuseFailureTerminalBatch: a filesystem failure travels in-band
// as ONE terminal batch carrying the status; the RPC itself succeeds.
func (s *RpcServerTestSuite) TestReadDirFuseFailureTerminalBatch() {
	s.bindConfinedVolume()
	stream := &stubReadDirStream{ctx: context.Background()}

	err := s.server.ReadDir(&proto.ReadDirRequest{
		Volume: "testVolume", Path: "/no-such-dir", Plus: true, Caller: CreateCaller(0, 0, 0),
	}, stream)

	s.Require().NoError(err)
	s.Require().Len(stream.sent, 1)
	s.Empty(stream.sent[0].Entries)
	s.Equal(proto.FsError_FS_ENOENT, stream.sent[0].Status)
}

// TestReadDirPlusFallbackWithoutCapability: when the bound FS does not
// implement ReadDirPlusser (here: a bare mock), plus=true degrades to the
// OpenDir listing with nil attrs instead of failing.
func (s *RpcServerTestSuite) TestReadDirPlusFallbackWithoutCapability() {
	mockFs := new(pathfs2.MockFileSystem)
	s.fsService.On("BindIdentity", mock.Anything, "testVolume", mock.Anything).
		Return(mockFs, service.Identity{}, nil)
	mockFs.EXPECT().OpenDir("/", mock.Anything).
		Return([]fuse.DirEntry{{Name: "a.txt", Mode: fuse.S_IFREG}}, fuse.OK)
	stream := &stubReadDirStream{ctx: context.Background()}

	err := s.server.ReadDir(&proto.ReadDirRequest{
		Volume: "testVolume", Path: "/", Plus: true, Caller: CreateCaller(0, 0, 0),
	}, stream)

	s.Require().NoError(err)
	s.Require().Len(stream.sent, 1)
	s.Require().Len(stream.sent[0].Entries, 1)
	s.Equal("a.txt", stream.sent[0].Entries[0].Entry.Name)
	s.Nil(stream.sent[0].Entries[0].Attributes)
}
