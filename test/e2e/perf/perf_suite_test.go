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
	"time"

	clientconfig "gmountie/pkg/client/config"
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
// The transport is bufconn (in-process) by default. Set GMOUNTIE_BENCH_TCP=1
// to swap to a real loopback TCP listener so tc netem shaping on lo
// affects the gRPC path — used by `task perf:bench:tcp` for the F4 harness
// that the Phase 3 final-review flagged as a follow-up.
func setupBenchEnv(b *testing.B) *benchEnv {
	b.Helper()

	opts := []utils.TestOptions{
		utils.WithBasicAuth("test", "test"),
		utils.WithRandomTestVolume(false),
	}
	if os.Getenv("GMOUNTIE_BENCH_TCP") != "" {
		opts = append(opts, utils.WithTCPTransport())
	}
	// GMOUNTIE_BENCH_CACHE=1 turns on the Sub-spec B client-side cache
	// decorator with a moderate sizing profile (256 MiB cap, 1 MiB chunks,
	// 5s/5s/2s TTLs). Mirrors the cache-on e2e suite configuration so the
	// bench/api numbers are reading the same backend chain. The default
	// (env unset) leaves the cache disabled, matching production.
	if os.Getenv("GMOUNTIE_BENCH_CACHE") != "" {
		opts = append(opts, utils.WithCache(clientconfig.CacheConfig{
			Enabled:        true,
			MaxSizeBytes:   1 << 28, // 256 MiB
			ChunkSizeBytes: 1 << 20, // 1 MiB
			AttrTTL:        5 * time.Second,
			DirTTL:         5 * time.Second,
			NegativeTTL:    2 * time.Second,
		}))
	}
	ctx, err := utils.NewAppTestingContext(opts...)
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

	// FUSE WaitMount returns once the kernel accepts the mount, but in
	// rapid mount/unmount cycles the very first op against the new mount
	// sometimes comes back with a pre-cancelled context (kernel teardown
	// of the previous mount's FUSE session bleeds into the new one). Poke
	// the mount until a Stat succeeds before handing control to the
	// bench loop so the b.N timer isn't measuring a startup glitch.
	if err := waitMountReady(volume.GetMountPath(), readyBudget()); err != nil {
		b.Fatalf("mount not ready: %v", err)
	}

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

// AssertReady re-probes the mount with the same Stat+Create+Remove cycle
// waitMountReady runs at setup. Benches that seed data via the
// server-side dataDir between setupBenchEnv and the timed loop should
// call this right before b.ResetTimer — heavy seeding can introduce a
// gap during which the kernel side of the new mount briefly stalls and
// the first bench-loop FUSE op surfaces EIO. The re-probe absorbs that
// without contaminating the bench measurement.
func (e *benchEnv) AssertReady(b *testing.B) {
	b.Helper()
	if err := waitMountReady(e.mountPoint, readyBudget()); err != nil {
		b.Fatalf("mount not ready post-setup: %v", err)
	}
}

// readyBudget returns how long the probe is willing to wait. The TCP
// transport variant runs under tc netem shaping that can add tens of
// milliseconds per RPC, so the probe (which issues several FUSE round-
// trips) needs more headroom than the bufconn path.
func readyBudget() time.Duration {
	if os.Getenv("GMOUNTIE_BENCH_TCP") != "" {
		return 10 * time.Second
	}
	return 2 * time.Second
}

// waitMountReady probes the mount with a Stat + Create + Remove cycle
// until all three succeed in a single pass, retrying with backoff up to
// budget. The first FUSE op after a rapid mount/unmount cycle
// occasionally surfaces an EIO from a pre-cancelled op context; running
// the probe out-of-band keeps that startup glitch off the bench timer.
// Probing Create + Remove (not just Stat) catches benches whose first
// timed op writes through different FUSE code paths than a bare Stat.
func waitMountReady(path string, budget time.Duration) error {
	deadline := time.Now().Add(budget)
	delay := 5 * time.Millisecond
	var lastErr error
	for {
		lastErr = probeMountOnce(path)
		if lastErr == nil {
			return nil
		}
		if time.Now().After(deadline) {
			return lastErr
		}
		time.Sleep(delay)
		if delay < 100*time.Millisecond {
			delay *= 2
		}
	}
}

func probeMountOnce(mountPath string) error {
	if _, err := os.Stat(mountPath); err != nil {
		return err
	}
	probe := mountPath + "/.gmountie-probe"
	if err := os.WriteFile(probe, []byte{0}, 0o600); err != nil {
		return err
	}
	if err := os.Remove(probe); err != nil {
		return err
	}
	return nil
}
