package log

import (
	"bytes"
	"os"
	"testing"

	"github.com/stretchr/testify/suite"
	"go.uber.org/zap"
)

type LogTestSuite struct {
	suite.Suite
}

func (s *LogTestSuite) TestDefaultIsConsoleWhenStderrTTY() {
	s.Assert().Equal("console", chooseFormat("", true))
	s.Assert().Equal("json", chooseFormat("", false))
}

func (s *LogTestSuite) TestExplicitFormatOverrides() {
	s.Assert().Equal("json", chooseFormat("json", true))
	s.Assert().Equal("console", chooseFormat("console", false))
}

func (s *LogTestSuite) TestReconfigureSwitchesEncoder() {
	var buf bytes.Buffer
	err := Reconfigure(LogConfig{Format: "json", Level: "info"}, &buf)
	s.Require().NoError(err)
	Log.Info("hello", zap.String("k", "v"))
	s.Assert().Contains(buf.String(), `"msg":"hello"`, "JSON encoder must produce structured output")
	s.Assert().Contains(buf.String(), `"k":"v"`, "fields should be present in JSON")
}

func (s *LogTestSuite) TestReconfigureRejectsUnknownLevel() {
	err := Reconfigure(LogConfig{Format: "json", Level: "shouty"}, os.Stderr)
	s.Require().Error(err)
}

func TestLogTestSuite(t *testing.T) {
	suite.Run(t, new(LogTestSuite))
}
