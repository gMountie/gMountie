package api

import (
	"bytes"
	"context"
	"os"
	"regexp"
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
	s.Require().NoError(testAppCtx.Start())
	testAppCtx.GetClient().Connect()
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

	re := regexp.MustCompile(`"request_id":"([0-9a-f-]+)"`)
	var matches [][]string
	s.Require().Eventually(func() bool {
		matches = re.FindAllStringSubmatch(s.logBuf.String(), -1)
		return len(matches) >= 2
	}, time.Second, 20*time.Millisecond,
		"both client and server must emit a finish-call line with request_id, got log:\n%s", s.logBuf.String())

	s.Assert().Equal(matches[0][1], matches[1][1],
		"both sides must use the same request_id")
}

func TestLoggingE2ETestSuite(t *testing.T) {
	suite.Run(t, new(LoggingE2ETestSuite))
}
