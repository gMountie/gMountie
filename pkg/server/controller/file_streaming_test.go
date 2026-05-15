package controller

import (
	"context"
	"testing"

	nodefs2 "gmountie/internal/mocks/github.com/hanwen/go-fuse/v2/fuse/nodefs"
	pathfs2 "gmountie/internal/mocks/github.com/hanwen/go-fuse/v2/fuse/pathfs"
	mockservice "gmountie/internal/mocks/pkg/server/service"
	"gmountie/pkg/proto"
	"gmountie/pkg/server/metrics"
	"gmountie/pkg/server/service"

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

func (f *fakeReadStream) Send(frame *proto.ReadFrame) error {
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

type StreamingReadSuite struct {
	suite.Suite
	server     *RpcFileServerImpl
	fsService  *mockservice.MockVolumeService
	sessionMgr service.SessionManager
	sessionID  string
	frameSize  int
}

func (s *StreamingReadSuite) SetupTest() {
	s.frameSize = 1 << 20 // 1 MiB
	s.fsService = new(mockservice.MockVolumeService)
	s.sessionMgr = service.NewSessionManager(service.SessionManagerOptions{})
	sid, err := s.sessionMgr.Create()
	s.Require().NoError(err)
	s.sessionID = sid
	s.server = NewRpcFileServer(s.fsService, s.sessionMgr, metrics.NewMetrics(), s.frameSize)
}

func (s *StreamingReadSuite) TearDownTest() {
	_ = s.sessionMgr.Stop(context.Background())
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

	stream := newFakeReadStream(context.Background())
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
		s.Assert().Equal(s.frameSize, len(frame.Data), "data frame %d should be exactly frame size", i)
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

	stream := newFakeReadStream(context.Background())
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
	s.Assert().Equal(s.frameSize, len(stream.frames[0].Data))
	s.Assert().Equal(fileSize-s.frameSize, len(stream.frames[1].Data))
	s.Assert().Equal(int32(fuse.OK), stream.frames[2].Status)
	s.Assert().Empty(stream.frames[2].Data)

	got := append(stream.frames[0].Data, stream.frames[1].Data...)
	s.Assert().Equal(payload, got)
}

func (s *StreamingReadSuite) TestRead_ReturnsErrnoOnBadFd() {
	stream := newFakeReadStream(context.Background())
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
