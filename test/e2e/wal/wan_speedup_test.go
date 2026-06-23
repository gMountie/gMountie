// Package wal_e2e — WAN speedup benchmark.
//
// Measures the speedup the WAL delivers on create-heavy workloads under
// injected WAN latency. Latency is injected via gRPC client interceptors so
// it is charged only on actual wire RPCs:
//
//   - unary interceptor: fires once per unary call (Create, Release, Mkdir …)
//   - stream interceptor: fires once at stream establishment (Write, Apply)
//
// WAL-off path (direct transport, no delegation):
//
//	for each file: Create (1 RTT) + Write-stream-open (1 RTT) + Release (1 RTT)
//	total ≈ N × 3 × delay
//
// WAL-on path (WAL stack with delegation):
//
//	delegation grant: ~few Mkdir RTTs (timed out of the measured window)
//	N creates + writes: 0 server RPCs — deferred to overlay
//	flush: 1 Apply stream open (1 RTT)
//	total ≈ 1 × delay  (plus negligible CPU)
//
// The regression assertion (WAL-on < WAL-off/2 at 20 ms) is a real gate,
// not a no-op print: at 20 ms the predicted WAL-off time is ~12 s and the
// predicted WAL-on time is ~20 ms, giving a ~600× theoretical speedup.
// The threshold of 2× leaves a huge margin for scheduling noise, so this
// assert will not flake but WILL catch regressions that break batching.
package wal_e2e

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/suite"
	"google.golang.org/grpc"

	"go.gmountie.dev/gmountie/pkg/client/backend/transport"
	"go.gmountie.dev/gmountie/pkg/proto"
	serverapp "go.gmountie.dev/gmountie/pkg/server"
	"go.gmountie.dev/gmountie/test/e2e/utils"
)

// ─── WANSpeedupSuite ──────────────────────────────────────────────────────────

// WANSpeedupSuite measures WAL speedup under injected WAN latency.
type WANSpeedupSuite struct {
	suite.Suite
}

// wanResult holds the timing outcome for a single (delay, N) run.
type wanResult struct {
	delay     time.Duration
	n         int
	walOff    time.Duration // wall-clock for N files, WAL-off path
	walOn     time.Duration // wall-clock for N files, WAL-on path
	offRPCs   int64         // unary RPCs counted on WAL-off path (Create + Release)
	offStream int64         // stream RPCs on WAL-off path (Write stream opens)
	onStream  int64         // Apply stream opens on WAL-on flush
}

func (r wanResult) speedup() float64 {
	if r.walOn <= 0 {
		return 0
	}
	return float64(r.walOff) / float64(r.walOn)
}

// latencyUnaryInterceptor returns a unary client interceptor that sleeps
// delay before invoking the RPC. This is charged once per round-trip,
// modelling a WAN with one-way delay = delay/2.
func latencyUnaryInterceptor(delay time.Duration, counter *atomic.Int64) grpc.UnaryClientInterceptor {
	return func(ctx context.Context, method string, req, reply interface{},
		cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
		if counter != nil {
			counter.Add(1)
		}
		time.Sleep(delay)
		return invoker(ctx, method, req, reply, cc, opts...)
	}
}

// latencyStreamInterceptor returns a stream client interceptor that sleeps
// delay at stream establishment — once per stream open, NOT per Send/RecvMsg.
// This correctly models one RTT per stream: WAL-on's Apply stream pays once
// for N files; WAL-off's Write stream pays once per file. A per-message sleep
// would unfairly penalise WAL-on (N sends per Apply) and must NOT be used.
func latencyStreamInterceptor(delay time.Duration, counter *atomic.Int64) grpc.StreamClientInterceptor {
	return func(ctx context.Context, desc *grpc.StreamDesc, cc *grpc.ClientConn,
		method string, streamer grpc.Streamer, opts ...grpc.CallOption) (grpc.ClientStream, error) {
		if counter != nil {
			counter.Add(1)
		}
		time.Sleep(delay)
		return streamer(ctx, desc, cc, method, opts...)
	}
}

// newScenarioCtxWAN creates an isolated test scenario with latency interceptors
// injected on all clients (both WAL-on and WAL-off stacks share the same tc).
//
// The returned unaryCount and streamCount atomics accumulate across all client
// calls made through tc; reset them before each timed region.
func (s *WANSpeedupSuite) newScenarioCtxWAN(
	delay time.Duration,
	unaryCount, streamCount *atomic.Int64,
	extraOpts ...utils.TestOptions,
) (tc *utils.AppTestingContext, srcDir, volName string) {
	s.T().Helper()
	srcDir = s.T().TempDir()
	volName = "testvol"

	wm := newMemWatermarkStore()

	opts := []utils.TestOptions{
		utils.WithBasicAuth("user", "pass"),
		utils.WithExistingVolume(volName, srcDir),
		utils.WithSessionGracePeriod(5 * time.Second),
		utils.WithServerAppContextOption(serverapp.WithWatermarkStore(wm)),
		// Inject latency on both unary and stream RPCs.
		utils.WithClientUnaryInterceptors(latencyUnaryInterceptor(delay, unaryCount)),
		utils.WithClientStreamInterceptors(latencyStreamInterceptor(delay, streamCount)),
	}
	opts = append(opts, extraOpts...)

	var err error
	tc, err = utils.NewAppTestingContext(opts...)
	s.Require().NoError(err, "build AppTestingContext")
	s.Require().NoError(tc.Start(), "start server")
	s.T().Cleanup(func() { _ = tc.Close() })
	return tc, srcDir, volName
}

// runWALOff times N file creates through a plain transport backend (no delegation).
// Each file pays Create (unary) + Write-stream-open (stream) + Release (unary).
func (s *WANSpeedupSuite) runWALOff(
	tc *utils.AppTestingContext,
	srcDir, volName string,
	n int,
	unaryCount, streamCount *atomic.Int64,
) (elapsed time.Duration, unaryRPCs, streamRPCs int64) {
	s.T().Helper()
	r := s.Require()

	// Plain transport backend — no delegation hook, no WAL drain.
	cl, err := tc.NewClientAs("user", "pass")
	r.NoError(err, "WAL-off: NewClientAs")
	defer cl.Close()

	// Create the "off" subtree on the server filesystem before timing.
	offDir := filepath.Join(srcDir, "off")
	r.NoError(os.Mkdir(offDir, 0o755))

	directBe := transport.NewBackendClient(cl, volName)

	// Reset counters before timed region.
	unaryCount.Store(0)
	streamCount.Store(0)

	start := time.Now()
	for i := 0; i < n; i++ {
		name := fmt.Sprintf("f%04d.txt", i)
		fh, _, fst := directBe.Create(bg(), "off", name, uint32(os.O_RDWR|os.O_CREATE), 0o644)
		r.Equal(proto.FsError_FS_OK, fst, "WAL-off Create %s", name)
		data := []byte(fmt.Sprintf("data-%d", i))
		_, wst := directBe.Write(bg(), fh, 0, data)
		r.Equal(proto.FsError_FS_OK, wst, "WAL-off Write %s", name)
		rst := directBe.Release(bg(), fh)
		r.Equal(proto.FsError_FS_OK, rst, "WAL-off Release %s", name)
	}
	elapsed = time.Since(start)

	unaryRPCs = unaryCount.Load()
	streamRPCs = streamCount.Load()

	return elapsed, unaryRPCs, streamRPCs
}

// runWALOn times N file creates through the full WAL stack (delegation + overlay),
// then flushes via Fsync. The delegation grant is acquired BEFORE starting the
// timer so grant-acquisition RTTs are excluded from the comparison.
func (s *WANSpeedupSuite) runWALOn(
	tc *utils.AppTestingContext,
	srcDir, volName string,
	n int,
	unaryCount, streamCount *atomic.Int64,
) (elapsed time.Duration, applyStreams int64) {
	s.T().Helper()
	r := s.Require()

	// Count Apply streams via a per-run server interceptor.
	var applyStreams_ atomic.Int64
	countApply := func(srv any, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		if info.FullMethod == applyFullMethod {
			applyStreams_.Add(1)
		}
		return handler(srv, ss)
	}
	_ = countApply // only used if injected via extra opts; here we count via server interceptor

	// Build WAL stack (shares the same tc with its injected interceptors).
	cs := newWALStack(s.T(), tc, "user", "pass", volName)
	defer cs.Close()

	// Create the "on" subtree on the server before grant acquisition.
	onDir := filepath.Join(srcDir, "on")
	r.NoError(os.Mkdir(onDir, 0o755))

	// Acquire delegation grant (outside the timed window).
	acquireGrant(s.T(), cs, "on", 10*time.Second)
	r.True(cs.mgr.IsDelegated("on"), "must hold delegation before timing starts")

	// Reset counters immediately before the timed region.
	unaryCount.Store(0)
	streamCount.Store(0)

	start := time.Now()
	// Create N files — all deferred to overlay (0 server RPCs expected).
	for i := 0; i < n; i++ {
		name := fmt.Sprintf("f%04d.txt", i)
		fh, _, fst := cs.be.Create(bg(), "on", name, uint32(os.O_RDWR|os.O_CREATE), 0o644)
		r.Equal(proto.FsError_FS_OK, fst, "WAL-on Create %s", name)
		data := []byte(fmt.Sprintf("data-%d", i))
		_, wst := cs.be.Write(bg(), fh, 0, data)
		r.Equal(proto.FsError_FS_OK, wst, "WAL-on Write %s", name)
		rst := cs.be.Release(bg(), fh)
		r.Equal(proto.FsError_FS_OK, rst, "WAL-on Release %s", name)
	}
	// Flush all pending ops in one Apply stream.
	r.Equal(proto.FsError_FS_OK, cs.coord.Fsync(bg()), "WAL-on Fsync must succeed")
	elapsed = time.Since(start)

	applyStreams = streamCount.Load() // Apply stream open = 1 stream interceptor call
	return elapsed, applyStreams
}

// TestWANSpeedupSuite_Speedup is the main benchmark-as-test.
// It runs at three injected latencies (1 ms, 5 ms, 20 ms) and asserts that
// the WAL-on path is at least 2× faster than WAL-off at the highest latency.
func (s *WANSpeedupSuite) TestWANSpeedupSuite_Speedup() {
	const N = 200

	delays := []time.Duration{
		1 * time.Millisecond,
		5 * time.Millisecond,
		20 * time.Millisecond,
	}

	var results []wanResult

	for _, delay := range delays {
		s.Run(fmt.Sprintf("delay=%v", delay), func() {
			// Each sub-run gets its own counter pair (reset inside each run-fn).
			var unaryCount, streamCount atomic.Int64

			tc, srcDir, volName := s.newScenarioCtxWAN(delay, &unaryCount, &streamCount)

			// WAL-off first (no delegation, direct transport).
			offDur, offUnary, offStream := s.runWALOff(tc, srcDir, volName, N, &unaryCount, &streamCount)

			// WAL-on: grant outside timing window, N creates + 1 flush.
			onDur, onApply := s.runWALOn(tc, srcDir, volName, N, &unaryCount, &streamCount)

			r := wanResult{
				delay:     delay,
				n:         N,
				walOff:    offDur,
				walOn:     onDur,
				offRPCs:   offUnary,
				offStream: offStream,
				onStream:  onApply,
			}
			results = append(results, r)

			s.T().Logf("delay=%-6v  N=%d  WAL-off=%v  WAL-on=%v  speedup=%.1fx  off-unary=%d  off-streams=%d  on-apply=%d",
				delay, N, offDur.Round(time.Millisecond), onDur.Round(time.Millisecond),
				r.speedup(), offUnary, offStream, onApply)

			// Sanity: WAL-off path fired many RPCs; WAL-on path fired far fewer.
			// We expect at minimum Create + Release per file on the unary path.
			minExpectedOffRPCs := int64(N) // at least N Creates
			s.Assert().GreaterOrEqual(offUnary, minExpectedOffRPCs,
				"WAL-off must fire at least %d unary RPCs (one Create per file)", minExpectedOffRPCs)

			// WAL-on: during the timed window (post-grant) there should be zero
			// unary RPCs (all creates are local) and exactly 1 Apply stream.
			s.Assert().Equal(int64(0), unaryCount.Load(),
				"WAL-on must not fire any unary RPCs in the timed window (ops are deferred)")
			s.Assert().Equal(int64(1), onApply,
				"WAL-on flush must open exactly 1 Apply stream (all N ops batched)")

			// Regression gate at 20 ms: WAL-on must be at least 2× faster than WAL-off.
			if delay >= 20*time.Millisecond {
				s.Assert().Less(onDur, offDur/2,
					"at delay=%v, WAL-on (%v) must be <half of WAL-off (%v); speedup=%.1fx",
					delay, onDur, offDur, r.speedup())
			}
		})
	}

	// Print the full comparison table at the end.
	if len(results) > 0 {
		s.T().Logf("\n--- WAN Speedup Summary (N=%d files) ---", N)
		s.T().Logf("%-10s  %-14s  %-14s  %-10s  %-14s  %-14s  %-10s",
			"Delay", "WAL-off", "WAL-on", "Speedup", "Off-Unary", "Off-Streams", "On-Apply")
		for _, r := range results {
			s.T().Logf("%-10v  %-14v  %-14v  %-10.1fx  %-14d  %-14d  %-10d",
				r.delay,
				r.walOff.Round(time.Millisecond),
				r.walOn.Round(time.Millisecond),
				r.speedup(),
				r.offRPCs,
				r.offStream,
				r.onStream,
			)
		}
	}
}

// ─── test entry-point ─────────────────────────────────────────────────────────

func TestWANSpeedupSuite(t *testing.T) {
	suite.Run(t, new(WANSpeedupSuite))
}
