package controller

import (
	"context"
	"testing"

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
)

// fakeReadStream implements proto.RpcFile_ReadServer for unit tests. The
// generated server interface is grpc.ServerStreamingServer[ReadFrame]; we
// only need Send + Context to drive the controller, so the remaining
// ServerStream methods are stubbed via the embedded interface.
type fakeReadStream struct {
	grpc.ServerStream
	ctx    context.Context
	frames []*proto.ReadFrame
}

func newFakeReadStream(ctx context.Context) *fakeReadStream {
	if ctx == nil {
		ctx = context.Background()
	}
	return &fakeReadStream{ctx: ctx}
}

// Send copies the frame's Data so that capture survives the streamer reusing
// the underlying buffer across iterations — mirrors how gRPC serialises the
// payload synchronously before returning from Send in production.
func (f *fakeReadStream) Send(frame *proto.ReadFrame) error {
	if frame != nil && frame.Data != nil {
		dup := make([]byte, len(frame.Data))
		copy(dup, frame.Data)
		frame = &proto.ReadFrame{Data: dup, Status: frame.Status}
	}
	f.frames = append(f.frames, frame)
	return nil
}

func (f *fakeReadStream) Context() context.Context {
	return f.ctx
}

// chunkedReader simulates a backing file that returns frame-sized reads
// against an in-memory payload. EOF is signalled by a 0-byte read once the
// offset has reached len(data).
type chunkedReader struct {
	data []byte
}

func (r *chunkedReader) Read(buf []byte, off int64) (fuse.ReadResult, fuse.Status) {
	if off >= int64(len(r.data)) {
		return fuse.ReadResultData(nil), fuse.OK
	}
	end := off + int64(len(buf))
	if end > int64(len(r.data)) {
		end = int64(len(r.data))
	}
	return fuse.ReadResultData(r.data[off:end]), fuse.OK
}

// failingReader returns failAfterBytes worth of payload across one or more
// reads, then returns failStatus on the next call. Used to exercise the
// mid-stream error path in ReadStreamer.
type failingReader struct {
	data           []byte
	failAfterBytes int64
	failStatus     fuse.Status
	served         int64
}

func (r *failingReader) Read(buf []byte, off int64) (fuse.ReadResult, fuse.Status) {
	if r.served >= r.failAfterBytes {
		return nil, r.failStatus
	}
	end := off + int64(len(buf))
	if end > r.failAfterBytes {
		end = r.failAfterBytes
	}
	if end > int64(len(r.data)) {
		end = int64(len(r.data))
	}
	chunk := r.data[off:end]
	r.served += int64(len(chunk))
	return fuse.ReadResultData(chunk), fuse.OK
}

type StreamingReadSuite struct {
	suite.Suite
	server     *RpcFileServerImpl
	fsService  *mockservice.MockVolumeService
	sessionMgr service.SessionManager
	sessionID  string
	frameSize  int
	bus        serverio.EventBus
}

func (s *StreamingReadSuite) SetupTest() {
	s.frameSize = 1 << 20 // 1 MiB
	s.fsService = new(mockservice.MockVolumeService)
	s.sessionMgr = service.NewSessionManager(service.SessionManagerOptions{})
	sid, err := s.sessionMgr.Create("test-user")
	s.Require().NoError(err)
	s.sessionID = sid
	s.bus = serverio.NewLocalEventBus(serverio.EventBusOptions{BufferSize: 16})
	s.server = NewRpcFileServer(s.fsService, s.sessionMgr, metrics.NewMetrics(), s.frameSize, s.bus)
}

func (s *StreamingReadSuite) TearDownTest() {
	_ = s.sessionMgr.Stop(context.Background())
	s.bus.Close()
}

// registerFile pre-loads the session with a backing chunkedReader and returns
// the fd. The chunkedReader is wrapped in a MockFile so we can rely on the
// existing nodefs mock infrastructure.
func (s *StreamingReadSuite) registerFile(payload []byte) uint64 {
	mockFs := new(pathfs2.MockFileSystem)
	mockFile := new(nodefs2.MockFile)
	s.fsService.On("GetVolumeFileSystem", "testVolume").Return(mockFs, nil).Maybe()
	reader := &chunkedReader{data: payload}
	mockFile.EXPECT().
		Read(mock.Anything, mock.AnythingOfType("int64")).
		RunAndReturn(reader.Read).Maybe()
	mockFile.EXPECT().Release().Return().Maybe()
	sess, _ := s.sessionMgr.Get(s.sessionID)
	return sess.RegisterFile("/test/path", mockFile)
}

func (s *StreamingReadSuite) TestRead_DeliversFullPayloadInMultipleFrames() {
	const totalSize = 5 << 20 // 5 MiB
	payload := make([]byte, totalSize)
	for i := range payload {
		payload[i] = byte(i % 251)
	}
	fd := s.registerFile(payload)

	stream := newFakeReadStream(testAuthedCtx("test-user"))
	err := s.server.Read(&proto.ReadRequest{
		Volume:    "testVolume",
		Fd:        fd,
		Offset:    0,
		Size:      uint32(totalSize),
		SessionId: s.sessionID,
	}, stream)

	s.Require().NoError(err)
	// 5 data frames + 1 terminal OK frame = 6 total.
	s.Require().Len(stream.frames, 6, "expected 5 data frames + 1 terminal status frame")

	got := make([]byte, 0, totalSize)
	for i, frame := range stream.frames[:5] {
		s.Assert().Equal(int32(fuse.OK), frame.Status, "data frame %d should carry OK status", i)
		s.Assert().Len(frame.Data, s.frameSize, "data frame %d should be exactly frame size", i)
		got = append(got, frame.Data...)
	}
	terminal := stream.frames[5]
	s.Assert().Equal(int32(fuse.OK), terminal.Status, "terminal frame should be OK")
	s.Assert().Empty(terminal.Data, "terminal frame should carry no payload")
	s.Assert().Equal(payload, got, "concatenated frames must match payload byte-for-byte")
}

func (s *StreamingReadSuite) TestRead_EOFReturnsShortFinalFrame() {
	const fileSize = (3 << 19)  // 1.5 MiB
	const requestSize = 5 << 20 // 5 MiB requested
	payload := make([]byte, fileSize)
	for i := range payload {
		payload[i] = byte(i % 251)
	}
	fd := s.registerFile(payload)

	stream := newFakeReadStream(testAuthedCtx("test-user"))
	err := s.server.Read(&proto.ReadRequest{
		Volume:    "testVolume",
		Fd:        fd,
		Offset:    0,
		Size:      uint32(requestSize),
		SessionId: s.sessionID,
	}, stream)

	s.Require().NoError(err)
	// First frame: 1 MiB; second frame: 0.5 MiB; terminal OK frame.
	s.Require().Len(stream.frames, 3)
	s.Assert().Len(stream.frames[0].Data, s.frameSize)
	s.Assert().Len(stream.frames[1].Data, fileSize-s.frameSize)
	s.Assert().Equal(int32(fuse.OK), stream.frames[2].Status)
	s.Assert().Empty(stream.frames[2].Data)

	got := append(stream.frames[0].Data, stream.frames[1].Data...)
	s.Assert().Equal(payload, got)
}

func (s *StreamingReadSuite) TestRead_MidStreamErrorEmitsTerminalErrnoFrame() {
	// Backing reader serves 1 MiB cleanly, then fails with EIO. We request
	// 3 MiB so the failure occurs after the first frame has been emitted —
	// the streamer must follow up with a single terminal frame carrying the
	// errno status, not silently truncate.
	const fileSize = 5 << 20
	payload := make([]byte, fileSize)
	for i := range payload {
		payload[i] = byte(i % 251)
	}

	mockFs := new(pathfs2.MockFileSystem)
	mockFile := new(nodefs2.MockFile)
	s.fsService.On("GetVolumeFileSystem", "testVolume").Return(mockFs, nil).Maybe()
	reader := &failingReader{data: payload, failAfterBytes: int64(s.frameSize), failStatus: fuse.EIO}
	mockFile.EXPECT().
		Read(mock.Anything, mock.AnythingOfType("int64")).
		RunAndReturn(reader.Read).Maybe()
	mockFile.EXPECT().Release().Return().Maybe()
	sess, _ := s.sessionMgr.Get(s.sessionID)
	fd := sess.RegisterFile("/test/path", mockFile)

	stream := newFakeReadStream(testAuthedCtx("test-user"))
	err := s.server.Read(&proto.ReadRequest{
		Volume:    "testVolume",
		Fd:        fd,
		Offset:    0,
		Size:      uint32(3 << 20),
		SessionId: s.sessionID,
	}, stream)

	s.Require().NoError(err, "mid-stream errno should surface as a terminal frame, not a transport error")
	s.Require().Len(stream.frames, 2, "expected one data frame + one terminal errno frame")
	s.Assert().Equal(int32(fuse.OK), stream.frames[0].Status, "first frame carries the served chunk")
	s.Assert().Len(stream.frames[0].Data, s.frameSize)
	s.Assert().Equal(int32(fuse.EIO), stream.frames[1].Status, "terminal frame carries the errno")
	s.Assert().Empty(stream.frames[1].Data, "terminal errno frame must not carry stale data")
}

func (s *StreamingReadSuite) TestRead_ReturnsErrnoOnBadFd() {
	stream := newFakeReadStream(testAuthedCtx("test-user"))
	err := s.server.Read(&proto.ReadRequest{
		Volume:    "testVolume",
		Fd:        99999, // unknown to session
		Offset:    0,
		Size:      4096,
		SessionId: s.sessionID,
	}, stream)

	s.Require().NoError(err, "bad fd should surface as an errno frame, not a transport error")
	s.Require().Len(stream.frames, 1)
	s.Assert().Equal(int32(fuse.EBADF), stream.frames[0].Status)
	s.Assert().Empty(stream.frames[0].Data)
}

func TestStreamingReadSuite(t *testing.T) {
	suite.Run(t, new(StreamingReadSuite))
}
