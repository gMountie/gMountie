// Package perf contains Go-bench-shaped performance harness for gMountie.
//
// Each Benchmark* builds its own in-process server + FUSE mount via
// test/e2e/utils and tears it down via b.Cleanup. The harness is intentionally
// flat (no testify suite) because benchstat expects vanilla Benchmark* funcs.
//
// To execute:
//
//	task perf:bench OUT=docs/perf/<name>.txt   # COUNT=5 BENCHTIME=10s by default
//	task perf:diff  BEFORE=<old> AFTER=<new>
//
// See docs/perf/README.md for the full workflow.
package perf

import (
	"io"
	"os"
	"testing"

	"gmountie/pkg/utils/log"
	"gmountie/test/e2e/utils"
)

// TestMain silences the package-level zap logger before any benchmark runs.
// Server/client setup is chatty (one block per benchmark, multiplied by COUNT)
// and pollutes the bench output stream that benchstat parses. Set the env var
// GMOUNTIE_BENCH_VERBOSE=1 to keep logs (useful when debugging a failing
// benchmark).
func TestMain(m *testing.M) {
	if os.Getenv("GMOUNTIE_BENCH_VERBOSE") == "" {
		if err := log.Reconfigure(log.LogConfig{Level: "error"}, io.Discard); err != nil {
			// Reconfigure with io.Discard should never fail; fall through if
			// it does — extra log lines are an annoyance, not a correctness
			// problem.
			_ = err
		}
	}
	os.Exit(m.Run())
}

// benchEnv holds an in-process gMountie server + FUSE mount for a single
// benchmark. The dataDir is the server-side directory backing the volume; the
// mountPoint is the client-side FUSE mount that the benchmark reads/writes
// through.
type benchEnv struct {
	ctx        *utils.AppTestingContext
	volume     *utils.TestVolume
	mountPoint string
	dataDir    string
}

// setupBenchEnv spins up a server + mount and registers cleanup. The volume is
// created empty (no random files) so benchmarks can seed deterministic data.
func setupBenchEnv(b *testing.B) *benchEnv {
	b.Helper()

	ctx, err := utils.NewAppTestingContext(
		utils.WithBasicAuth("test", "test"),
		utils.WithRandomTestVolume(false),
	)
	if err != nil {
		b.Fatalf("NewAppTestingContext: %v", err)
	}
	if err := ctx.Start(); err != nil {
		b.Fatalf("ctx.Start: %v", err)
	}

	volume := ctx.GetVolumes()[0]
	if volume == nil {
		b.Fatal("no test volume registered")
	}
	ctx.MountVolume(volume)

	b.Cleanup(func() {
		// If ctx.Close fails the FUSE mount may still be live; removing the
		// mountpoint dir via volume.Close in that state corrupts the next
		// benchmark's setup. Skip volume.Close on a failed ctx.Close.
		if err := ctx.Close(); err != nil {
			b.Logf("ctx.Close (mount may still be active, skipping volume removal): %v", err)
			return
		}
		if err := volume.Close(); err != nil {
			b.Logf("volume.Close: %v", err)
		}
	})

	return &benchEnv{
		ctx:        ctx,
		volume:     volume,
		mountPoint: volume.GetMountPath(),
		dataDir:    volume.GetSrcPath(),
	}
}
