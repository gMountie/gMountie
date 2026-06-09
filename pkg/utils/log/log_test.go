package log

import (
	"bytes"
	stdlog "log"
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

// TestReconfigureDoesNotHijackGlobals pins the importable-library contract:
// without the explicit HijackGlobals opt-in, neither import-time init nor a
// Reconfigure call may replace zap's global loggers.
func (s *LogTestSuite) TestReconfigureDoesNotHijackGlobals() {
	prev := zap.L()
	var buf bytes.Buffer
	s.Require().NoError(Reconfigure(LogConfig{Format: "json"}, &buf))
	s.Assert().Same(prev, zap.L(),
		"Reconfigure must not replace zap globals without HijackGlobals")
}

// TestHijackGlobalsIsExplicitAndSticky verifies the CLI opt-in: HijackGlobals
// routes zap's globals and the stdlib default logger through Log, and a later
// Reconfigure keeps them pointed at the rebuilt logger.
func (s *LogTestSuite) TestHijackGlobalsIsExplicitAndSticky() {
	prevGlobal := zap.L()
	prevWriter := stdlog.Default().Writer()
	defer func() {
		globalsHijacked = false
		zap.ReplaceGlobals(prevGlobal)
		stdlog.Default().SetOutput(prevWriter)
	}()

	var buf bytes.Buffer
	s.Require().NoError(Reconfigure(LogConfig{Format: "json", Level: "debug"}, &buf))
	s.Require().NoError(HijackGlobals())
	s.Assert().Same(Log, zap.L(), "HijackGlobals must replace zap globals")
	stdlog.Print("via-stdlib")
	s.Assert().Contains(buf.String(), "via-stdlib", "stdlib log must be redirected")

	// Sticky: a later Reconfigure re-points the globals at the new logger.
	var buf2 bytes.Buffer
	s.Require().NoError(Reconfigure(LogConfig{Format: "json", Level: "debug"}, &buf2))
	s.Assert().Same(Log, zap.L(), "hijack must survive Reconfigure")
	stdlog.Print("after-reconfigure")
	s.Assert().Contains(buf2.String(), "after-reconfigure")
}

func TestLogTestSuite(t *testing.T) {
	suite.Run(t, new(LogTestSuite))
}
