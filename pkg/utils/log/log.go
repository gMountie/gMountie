// Package log owns the project logger (Log, a named *zap.Logger).
//
// IMPORTANT — importable-library contract: importing this package has no
// process-global side effects. The package logger is built lazily at init
// (stderr, auto-detected encoder) but zap's globals and the stdlib `log`
// default logger are NOT touched unless the host binary explicitly opts in
// via HijackGlobals. The gmountie CLI opts in (cmd/commands.Execute); a
// third-party program importing pkg/client or pkg/server keeps its own
// logging configuration.
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

// globalsHijacked records the HijackGlobals opt-in so subsequent Reconfigure
// calls keep the process globals pointing at the rebuilt logger.
var globalsHijacked bool

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
//
// Reconfigure only swaps the package-level Log. Process globals (zap's
// global loggers, the stdlib `log` output) are left alone unless the binary
// previously opted in via HijackGlobals, in which case they are re-pointed
// at the new logger so the opt-in stays in effect across reconfigurations.
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

	if globalsHijacked {
		return applyGlobalHijack()
	}
	return nil
}

// HijackGlobals routes process-global logging through the package logger:
// zap.ReplaceGlobals plus redirecting the stdlib `log` default logger (which
// third-party code such as go-fuse writes to) into Log. This is the explicit
// opt-in for binaries that own the process (the gmountie CLI calls it once
// from cmd/commands.Execute); it must never run as an import side effect, so
// library consumers of pkg/... keep their own global loggers. The opt-in is
// sticky: later Reconfigure calls re-apply it to the rebuilt logger.
func HijackGlobals() error {
	globalsHijacked = true
	return applyGlobalHijack()
}

// applyGlobalHijack points zap's globals and the stdlib default logger at
// the current package logger.
func applyGlobalHijack() error {
	zap.ReplaceGlobals(Log)
	stdLogger, err := zap.NewStdLogAt(Log.Named("std"), zapcore.DebugLevel)
	if err != nil {
		return errors.Wrap(err, "wire stdlib log")
	}
	log.Default().SetOutput(stdLogger.Writer())
	return nil
}
