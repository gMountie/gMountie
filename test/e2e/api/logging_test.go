package api

import (
	"bytes"
	"context"
	"os"
	"regexp"
	"strings"
	"sync"
	"testing"
	"time"

	"go.gmountie.dev/gmountie/pkg/proto"
	"go.gmountie.dev/gmountie/pkg/utils/log"
	"go.gmountie.dev/gmountie/test/e2e/utils"

	"github.com/stretchr/testify/suite"
)

// syncBuffer is a goroutine-safe bytes.Buffer suitable for use as a zap
// sink while concurrent gRPC handlers and the test goroutine read/write.
type syncBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *syncBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *syncBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

func (b *syncBuffer) Reset() {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.buf.Reset()
}

type LoggingE2ETestSuite struct {
	suite.Suite
	testAppCtx *utils.AppTestingContext
	logBuf     *syncBuffer
}

func (s *LoggingE2ETestSuite) SetupSuite() {
	s.logBuf = &syncBuffer{}
	s.Require().NoError(log.Reconfigure(log.LogConfig{Format: "json", Level: "debug"}, s.logBuf))

	testAppCtx, err := utils.NewAppTestingContext(
		utils.WithBasicAuth("test", "test"),
		utils.WithRandomTestVolume(false),
	)
	s.Require().NoError(err)
	// Start performs the client session handshake (Connect) itself.
	s.Require().NoError(testAppCtx.Start())
	s.testAppCtx = testAppCtx
}

func (s *LoggingE2ETestSuite) TearDownSuite() {
	s.Require().NoError(s.testAppCtx.Close())
	// Restore the default logger so other suites running in the same `go test`
	// invocation aren't stuck with our buffer.
	_ = log.Reconfigure(log.LogConfig{}, os.Stderr)
}

func (s *LoggingE2ETestSuite) TestRequestIDOnBothSides() {
	s.logBuf.Reset()
	_, err := s.testAppCtx.GetClient().Volume().List(context.Background(), &proto.VolumeListRequest{})
	s.Require().NoError(err)

	// The buffer is the shared global logger, so it can also pick up
	// unrelated request_id lines from background session keepalive
	// recovery (Resume/Create are unary and carry a request_id too). Scope
	// the assertion to *this* call by keying on the VolumeService/List
	// finish-call lines, one per side, rather than blindly comparing the
	// first two request_id matches in the buffer.
	reqIDRe := regexp.MustCompile(`"request_id":"([0-9a-f-]+)"`)
	componentRe := regexp.MustCompile(`"grpc\.component":"(client|server)"`)

	var clientID, serverID string
	s.Require().Eventually(func() bool {
		clientID, serverID = "", ""
		for _, line := range strings.Split(s.logBuf.String(), "\n") {
			if !strings.Contains(line, `"grpc.method":"List"`) {
				continue
			}
			id := reqIDRe.FindStringSubmatch(line)
			comp := componentRe.FindStringSubmatch(line)
			if id == nil || comp == nil {
				continue
			}
			switch comp[1] {
			case "client":
				clientID = id[1]
			case "server":
				serverID = id[1]
			}
		}
		// Require both sides — ordering between the client and server
		// finish-call lines is not guaranteed, so a count check would race.
		return clientID != "" && serverID != ""
	}, time.Second, 20*time.Millisecond,
		"both client and server must emit a List finish-call line with request_id, got log:\n%s", s.logBuf.String())

	s.Assert().Equal(clientID, serverID,
		"both sides must use the same request_id")
}

func TestLoggingE2ETestSuite(t *testing.T) {
	suite.Run(t, new(LoggingE2ETestSuite))
}
