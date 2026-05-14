package log

import (
	"io"
	"log"
	"os"

	"github.com/pkg/errors"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"golang.org/x/term"
)

// LogConfig controls runtime logger behaviour. Both fields are optional;
// zero values trigger auto-detect / defaults.
type LogConfig struct {
	// Format selects the encoder. "console" (human-friendly), "json"
	// (machine-friendly), or "" to auto-detect: console if stderr is a
	// TTY, json otherwise.
	Format string `mapstructure:"format"`
	// Level is "debug" | "info" | "warn" | "error". Empty -> info.
	Level string `mapstructure:"level"`
}

var Log *zap.Logger

func init() {
	if err := Reconfigure(LogConfig{}, os.Stderr); err != nil {
		Log = zap.NewNop()
		return
	}
}

// chooseFormat resolves the encoder name from config + TTY state.
func chooseFormat(configured string, isTTY bool) string {
	if configured != "" {
		return configured
	}
	if isTTY {
		return "console"
	}
	return "json"
}

func parseLevel(s string) (zapcore.Level, error) {
	if s == "" {
		return zapcore.InfoLevel, nil
	}
	var lvl zapcore.Level
	if err := lvl.UnmarshalText([]byte(s)); err != nil {
		return 0, errors.Wrapf(err, "parse log level %q", s)
	}
	return lvl, nil
}

// Reconfigure rebuilds the package logger from cfg. `sink` is where
// output goes; in production callers pass os.Stderr. Tests can pass
// a *bytes.Buffer.
func Reconfigure(cfg LogConfig, sink io.Writer) error {
	lvl, err := parseLevel(cfg.Level)
	if err != nil {
		return err
	}
	isTTY := false
	if f, ok := sink.(*os.File); ok {
		isTTY = term.IsTerminal(int(f.Fd()))
	}
	format := chooseFormat(cfg.Format, isTTY)

	var enc zapcore.Encoder
	if format == "console" {
		encCfg := zap.NewDevelopmentEncoderConfig()
		enc = zapcore.NewConsoleEncoder(encCfg)
	} else {
		encCfg := zap.NewProductionEncoderConfig()
		encCfg.TimeKey = "ts"
		encCfg.MessageKey = "msg"
		encCfg.EncodeTime = zapcore.ISO8601TimeEncoder
		enc = zapcore.NewJSONEncoder(encCfg)
	}
	core := zapcore.NewCore(enc, zapcore.AddSync(sink), lvl)
	Log = zap.New(core, zap.AddCaller()).Named("gMountie")
	zap.ReplaceGlobals(Log)

	stdLogger, err := zap.NewStdLogAt(Log.Named("std"), zapcore.DebugLevel)
	if err != nil {
		return errors.Wrap(err, "wire stdlib log")
	}
	log.Default().SetOutput(stdLogger.Writer())
	return nil
}
