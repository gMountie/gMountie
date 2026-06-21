package controller

import (
	"context"
	stdio "io"
	"testing"
	"time"

	nodefs2 "go.gmountie.dev/gmountie/internal/mocks/github.com/hanwen/go-fuse/v2/fuse/nodefs"
	pathfs2 "go.gmountie.dev/gmountie/internal/mocks/github.com/hanwen/go-fuse/v2/fuse/pathfs"
	mockservice "go.gmountie.dev/gmountie/internal/mocks/pkg/server/service"
	"go.gmountie.dev/gmountie/pkg/proto"
	serverio "go.gmountie.dev/gmountie/pkg/server/io"
	"go.gmountie.dev/gmountie/pkg/server/metrics"
	"go.gmountie.dev/gmountie/pkg/server/service"

	"github.com/hanwen/go-fuse/v2/fuse"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// fakeWriteStream implements proto.RpcFile_WriteServer for unit tests. The
// generated server interface is grpc.ClientStreamingServer[WriteFrame,
// WriteReply]; we drive the controller via a queued list of frames + a
// terminal io.EOF, and capture the reply written via SendAndClose.
//
// Unlike fakeReadStream there is no buffer-reuse concern: gRPC produces a
// fresh byte slice per Recv in production, and so do we here.
type fakeWriteStream struct {
	grpc.ServerStream
	ctx     context.Context
	frames  []*proto.WriteFrame
	idx     int
	reply   *proto.WriteReply
	recvErr error
}

func newFakeWriteStream(ctx context.Context, frames ...*proto.WriteFrame) *fakeWriteStream {
	if ctx == nil {
		ctx = context.Background()
	}
	return &fakeWriteStream{ctx: ctx, frames: frames}
}

func (f *fakeWriteStream) Recv() (*proto.WriteFrame, error) {
	if f.recvErr != nil {
		return nil, f.recvErr
	}
	if f.idx >= len(f.frames) {
		return nil, stdio.EOF
	}
	frame := f.frames[f.idx]
	f.idx++
	return frame, nil
}

func (f *fakeWriteStream) SendAndClose(reply *proto.WriteReply) error {
	f.reply = reply
	return nil
}

func (f *fakeWriteStream) Context() context.Context {
	return f.ctx
}

// recordingWriter records every Write call. It is wired into the bench's
// MockFile via mockFile.EXPECT().Write(...).RunAndReturn(writer.Write), so
// only the Write hook is exercised during these tests.
type recordingWriter struct {
	writes []writeOp
	status fuse.Status
}

type writeOp struct {
	data   []byte
	offset int64
}

func (r *recordingWriter) Write(data []byte, off int64) (uint32, fuse.Status) {
	// Copy data — the controller is allowed to reuse the underlying buffer
	// across iterations, but unit assertions need a stable snapshot.
	dup := make([]byte, len(data))
	copy(dup, data)
	r.writes = append(r.writes, writeOp{data: dup, offset: off})
	if !r.status.Ok() {
		return 0, r.status
	}
	return uint32(len(data)), fuse.OK
}

func (r *recordingWriter) bytes() []byte {
	out := make([]byte, 0)
	for _, w := range r.writes {
		out = append(out, w.data...)
	}
	return out
}

type StreamingWriteSuite struct {
	suite.Suite
	server     *RpcFileServerImpl
	fsService  *mockservice.MockVolumeService
	sessionMgr service.SessionManager
	sessionID  string
	bus        serverio.EventBus
}

func (s *StreamingWriteSuite) SetupTest() {
	s.fsService = new(mockservice.MockVolumeService)
	s.sessionMgr = service.NewSessionManager(service.SessionManagerOptions{})
	sid, err := s.sessionMgr.Create("test-user", "")
	s.Require().NoError(err)
	s.sessionID = sid
	s.bus = serverio.NewLocalEventBus(serverio.EventBusOptions{BufferSize: 16})
	s.server = NewRpcFileServer(s.fsService, s.sessionMgr, metrics.NewMetrics(), 1<<20, s.bus)
}

func (s *StreamingWriteSuite) TearDownTest() {
	_ = s.sessionMgr.Stop(context.Background())
	s.bus.Close()
}

// registerWriter pre-loads the session with a recording write sink and
// returns the (fd, writer) pair.
func (s *StreamingWriteSuite) registerWriter() (uint64, *recordingWriter) {
	mockFs := new(pathfs2.MockFileSystem)
	mockFile := new(nodefs2.MockFile)
	s.fsService.On("GetVolumeFileSystem", "testVolume").Return(mockFs, nil).Maybe()
	mockFs.EXPECT().GetAttr("/test/path", mock.Anything).Return(&fuse.Attr{}, fuse.OK).Maybe()
	writer := &recordingWriter{status: fuse.OK}
	mockFile.EXPECT().
		Write(mock.Anything, mock.AnythingOfType("int64")).
		RunAndReturn(writer.Write).Maybe()
	mockFile.EXPECT().Release().Return().Maybe()
	sess, _ := s.sessionMgr.Get(s.sessionID)
	return sess.RegisterFile("testVolume", "/test/path", mockFile), writer
}

func (s *StreamingWriteSuite) TestWrite_SingleFrameAppendsAndReturnsByteCount() {
	fd, writer := s.registerWriter()
	data := []byte("hello world")

	stream := newFakeWriteStream(testAuthedCtx("test-user"), &proto.WriteFrame{
		Volume:    "testVolume",
		Fd:        fd,
		SessionId: s.sessionID,
		RequestId: "req-single",
		Offset:    42,
		Data:      data,
	})

	err := s.server.Write(stream)
	s.Require().NoError(err)
	s.Require().NotNil(stream.reply)
	s.Assert().Equal(uint32(len(data)), stream.reply.Written)
	s.Assert().Equal(proto.FsError_FS_OK, stream.reply.Status)
	s.Require().Len(writer.writes, 1)
	s.Assert().Equal(data, writer.writes[0].data)
	s.Assert().Equal(int64(42), writer.writes[0].offset)
}

func (s *StreamingWriteSuite) TestWrite_MultipleFramesContiguousOffsetsAppend() {
	fd, writer := s.registerWriter()
	chunk1 := []byte("chunk-one-")
	chunk2 := []byte("chunk-two")

	stream := newFakeWriteStream(testAuthedCtx("test-user"),
		&proto.WriteFrame{
			Volume:    "testVolume",
			Fd:        fd,
			SessionId: s.sessionID,
			RequestId: "req-multi",
			Offset:    0,
			Data:      chunk1,
		},
		// Second frame: header fields left zero, only data carried.
		&proto.WriteFrame{Data: chunk2},
	)

	err := s.server.Write(stream)
	s.Require().NoError(err)
	s.Require().NotNil(stream.reply)
	s.Assert().Equal(uint32(len(chunk1)+len(chunk2)), stream.reply.Written)
	s.Assert().Equal(proto.FsError_FS_OK, stream.reply.Status)
	s.Require().Len(writer.writes, 2)
	s.Assert().Equal(chunk1, writer.writes[0].data)
	s.Assert().Equal(int64(0), writer.writes[0].offset)
	s.Assert().Equal(chunk2, writer.writes[1].data)
	s.Assert().Equal(int64(len(chunk1)), writer.writes[1].offset)
	s.Assert().Equal(append(append([]byte{}, chunk1...), chunk2...), writer.bytes())
}

func (s *StreamingWriteSuite) TestWrite_DuplicateRequestIDReturnsCachedReply() {
	fd, writer := s.registerWriter()
	original := []byte("first-write-data")
	replay := []byte("totally-different-bytes-that-must-NOT-land")

	first := newFakeWriteStream(testAuthedCtx("test-user"), &proto.WriteFrame{
		Volume:    "testVolume",
		Fd:        fd,
		SessionId: s.sessionID,
		RequestId: "req-dup",
		Offset:    0,
		Data:      original,
	})
	s.Require().NoError(s.server.Write(first))
	s.Require().NotNil(first.reply)
	originalReply := first.reply

	second := newFakeWriteStream(testAuthedCtx("test-user"),
		&proto.WriteFrame{
			Volume:    "testVolume",
			Fd:        fd,
			SessionId: s.sessionID,
			RequestId: "req-dup",
			Offset:    0,
			Data:      replay[:10],
		},
		&proto.WriteFrame{Data: replay[10:]},
	)
	s.Require().NoError(s.server.Write(second))

	s.Require().NotNil(second.reply)
	s.Assert().Equal(originalReply.Written, second.reply.Written, "cached Written must be returned")
	s.Assert().Equal(originalReply.Status, second.reply.Status, "cached Status must be returned")

	// The replay's bytes must NOT have hit the backing file.
	s.Require().Len(writer.writes, 1, "duplicate request must not produce additional Write calls")
	s.Assert().Equal(original, writer.writes[0].data)
}

func (s *StreamingWriteSuite) TestWrite_FrameMissingFirstHeaderFails() {
	fd, _ := s.registerWriter()

	cases := []struct {
		name  string
		frame *proto.WriteFrame
	}{
		{
			name: "empty session_id",
			frame: &proto.WriteFrame{
				Volume:    "testVolume",
				Fd:        fd,
				RequestId: "r",
				Data:      []byte("x"),
			},
		},
		{
			name: "empty request_id",
			frame: &proto.WriteFrame{
				Volume:    "testVolume",
				Fd:        fd,
				SessionId: s.sessionID,
				Data:      []byte("x"),
			},
		},
		{
			name: "empty volume",
			frame: &proto.WriteFrame{
				Fd:        fd,
				SessionId: s.sessionID,
				RequestId: "r2",
				Data:      []byte("x"),
			},
		},
		{
			name: "zero fd",
			frame: &proto.WriteFrame{
				Volume:    "testVolume",
				SessionId: s.sessionID,
				RequestId: "r3",
				Data:      []byte("x"),
			},
		},
	}
	for _, c := range cases {
		s.Run(c.name, func() {
			stream := newFakeWriteStream(testAuthedCtx("test-user"), c.frame)
			err := s.server.Write(stream)
			s.Require().Error(err)
			st, ok := status.FromError(err)
			s.Require().True(ok, "expected gRPC status error, got %v", err)
			s.Assert().Equal(codes.InvalidArgument, st.Code())
		})
	}
}

// TestWrite_ContinuationFrameHeaderMismatchRejected covers
// validateContinuationFrame: a frame 2 with a non-zero header field that
// differs from frame 1 is rejected with InvalidArgument. proto3 zero values
// remain valid (= inherit frame 1).
func (s *StreamingWriteSuite) TestWrite_ContinuationFrameHeaderMismatchRejected() {
	fd, writer := s.registerWriter()

	stream := newFakeWriteStream(testAuthedCtx("test-user"),
		&proto.WriteFrame{
			Volume:    "testVolume",
			Fd:        fd,
			SessionId: s.sessionID,
			RequestId: "req-mismatch",
			Offset:    0,
			Data:      []byte("good-prefix"),
		},
		// Continuation frame with a non-zero, mismatched Volume.
		&proto.WriteFrame{Volume: "otherVolume", Data: []byte("bad-suffix")},
	)

	err := s.server.Write(stream)
	s.Require().Error(err)
	st, ok := status.FromError(err)
	s.Require().True(ok, "expected gRPC status error, got %v", err)
	s.Assert().Equal(codes.InvalidArgument, st.Code())
	// Frame 1's data was applied before the bad frame was seen; that's
	// allowed. The bad frame's data must NOT have landed.
	s.Require().Len(writer.writes, 1)
	s.Assert().Equal([]byte("good-prefix"), writer.writes[0].data)
}

// TestWrite_NoSubscribers_SkipsVersionStatAndEmit: with nobody subscribed to
// the volume, a successful Write must skip versionAfterPath entirely — no
// GetVolumeFileSystem lookup, no GetAttr (each Write otherwise pays an
// openat2+fstatat+close on the path) — and emit nothing. The fd is registered
// by hand instead of via registerWriter so the GetVolumeFileSystem/GetAttr
// stubs are ABSENT: the strict mocks fail on any unexpected call.
func (s *StreamingWriteSuite) TestWrite_NoSubscribers_SkipsVersionStatAndEmit() {
	spy := &spyBus{EventBus: s.bus}
	server := NewRpcFileServer(s.fsService, s.sessionMgr, metrics.NewMetrics(), 1<<20, spy)

	mockFile := new(nodefs2.MockFile)
	writer := &recordingWriter{status: fuse.OK}
	mockFile.EXPECT().
		Write(mock.Anything, mock.AnythingOfType("int64")).
		RunAndReturn(writer.Write).Once()
	mockFile.EXPECT().Release().Return().Maybe()
	sess, _ := s.sessionMgr.Get(s.sessionID)
	fd := sess.RegisterFile("testVolume", "/test/path", mockFile)

	stream := newFakeWriteStream(testAuthedCtx("test-user"), &proto.WriteFrame{
		Volume:    "testVolume",
		Fd:        fd,
		SessionId: s.sessionID,
		RequestId: "req-nosub",
		Offset:    0,
		Data:      []byte("payload"),
	})

	s.Require().NoError(server.Write(stream))
	s.Require().NotNil(stream.reply)
	s.Assert().Equal(proto.FsError_FS_OK, stream.reply.Status)
	s.Require().Len(writer.writes, 1, "the write itself must still land")
	s.EqualValues(0, spy.emits.Load(), "no subscribers → no emit")
	s.fsService.AssertNotCalled(s.T(), "GetVolumeFileSystem", mock.Anything)
}

// TestWrite_WithSubscriber_EmitsVersionFromStat pins the subscribed path: the
// post-write versionAfterPath stat happens exactly once and the event carries
// the non-zero version it produced.
func (s *StreamingWriteSuite) TestWrite_WithSubscriber_EmitsVersionFromStat() {
	events, _, cancel := s.bus.Subscribe("testVolume", "")
	defer cancel()

	mockFs := new(pathfs2.MockFileSystem)
	mockFile := new(nodefs2.MockFile)
	attr := &fuse.Attr{Ino: 9, Size: 7, Mtime: 1700000000}
	s.fsService.On("GetVolumeFileSystem", "testVolume").Return(mockFs, nil).Once()
	mockFs.EXPECT().GetAttr("/test/path", mock.Anything).Return(attr, fuse.OK).Once()
	writer := &recordingWriter{status: fuse.OK}
	mockFile.EXPECT().
		Write(mock.Anything, mock.AnythingOfType("int64")).
		RunAndReturn(writer.Write).Once()
	mockFile.EXPECT().Release().Return().Maybe()
	sess, _ := s.sessionMgr.Get(s.sessionID)
	fd := sess.RegisterFile("testVolume", "/test/path", mockFile)

	stream := newFakeWriteStream(testAuthedCtx("test-user"), &proto.WriteFrame{
		Volume:    "testVolume",
		Fd:        fd,
		SessionId: s.sessionID,
		RequestId: "req-sub",
		Offset:    0,
		Data:      []byte("payload"),
	})

	s.Require().NoError(s.server.Write(stream))
	s.Require().NotNil(stream.reply)
	s.Assert().Equal(proto.FsError_FS_OK, stream.reply.Status)

	select {
	case ev := <-events:
		s.Equal(serverio.KindMutated, ev.Kind)
		s.Equal("/test/path", ev.Path)
		s.NotZero(ev.NewVersion, "subscribed path must seed the event from a fresh stat")
		s.Equal(serverio.VersionFromAttr(attr), ev.NewVersion)
	case <-time.After(time.Second):
		s.FailNow("Write with a subscriber did not emit a KindMutated event within 1s")
	}
	mockFs.AssertExpectations(s.T()) // GetAttr .Once(): the stat still happens when someone listens
}

func TestStreamingWriteSuite(t *testing.T) {
	suite.Run(t, new(StreamingWriteSuite))
}
