package log

import (
	"log"

	"go.uber.org/zap"
)

var Log *zap.Logger

func init() {
	// Create a logger

	// Set logger to warn level for tests
	zapConfig := zap.Config{
		Level:       zap.NewAtomicLevelAt(zap.InfoLevel),
		Development: true,
		Sampling: &zap.SamplingConfig{
			Initial:    100,
			Thereafter: 100,
		},
		Encoding:         "console",
		EncoderConfig:    zap.NewDevelopmentEncoderConfig(),
		OutputPaths:      []string{"stderr"},
		ErrorOutputPaths: []string{"stderr"},
	}
	Log = zap.Must(zapConfig.Build())
	Log = Log.Named("gMountie")
	zap.ReplaceGlobals(Log)
	// Redirect standard library log to zap
	logger, err := zap.NewStdLogAt(Log.Named("std"), zap.DebugLevel)
	if err != nil {
		Log.Fatal("failed to create logger", zap.Error(err))
	}
	log.Default().SetOutput(logger.Writer())
}
