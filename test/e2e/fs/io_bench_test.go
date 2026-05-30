package fs

import (
	"go.gmountie.dev/gmountie/test/e2e/utils"
	"runtime"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/typomedia/diskspeed/bench"
	"github.com/typomedia/diskspeed/mem"
)

func setup(b *testing.B) (*utils.AppTestingContext, *utils.TestVolume) {
	// Create a new app testing context.
	testAppCtx, err := utils.NewAppTestingContext(
		utils.WithBasicAuth("test", "test"),
		utils.WithRandomTestVolume(false),
	)
	require.NoError(b, err)
	// Start the app testing context.
	err = testAppCtx.Start()
	require.NoError(b, err)
	// Mount the volume.
	volume := testAppCtx.GetVolumes()[0]
	require.NotNil(b, volume)
	testAppCtx.MountVolume(volume)
	require.NoError(b, err)
	// Cleanup.
	b.Cleanup(func() {
		// Unmount the volume.
		utils.Must0(b, testAppCtx.Close())
	})
	return testAppCtx, volume
}

func benchmarkWrite(bm *bench.Mark, b *testing.B) {
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		b.StartTimer()
		err := bm.RunSequentialWriteTest()
		b.StopTimer()
		require.NoError(b, err)
		r := bm.Results[len(bm.Results)-1]
		b.ReportMetric(float64(r.WrittenBytes)/1000000, "MB")
		b.ReportMetric(r.WrittenDuration.Seconds(), "seconds")
		b.ReportMetric(bench.MegaBytesPerSecond(r.WrittenBytes, r.WrittenDuration), "MB/s")
	}
}

func benchmarkRead(bm *bench.Mark, b *testing.B) {
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		b.StartTimer()
		err := bm.RunSequentialReadTest()
		b.StopTimer()
		require.NoError(b, err)
		r := bm.Results[len(bm.Results)-1]
		b.ReportMetric(float64(r.ReadBytes)/1000000, "MB")
		b.ReportMetric(r.ReadDuration.Seconds(), "seconds")
		b.ReportMetric(bench.MegaBytesPerSecond(r.ReadBytes, r.ReadDuration), "MB/s")
	}
}

func benchmarkIops(bm *bench.Mark, b *testing.B) {
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		b.StartTimer()
		err := bm.RunIOPSTest()
		b.StopTimer()
		require.NoError(b, err)
		r := bm.Results[len(bm.Results)-1]
		b.ReportMetric(float64(r.IOOperations), "operations")
		b.ReportMetric(r.IODuration.Seconds(), "seconds")
		b.ReportMetric(bench.IOPS(r.IOOperations, r.IODuration), "IOPS")
	}
}

func BenchmarkWriteRead(b *testing.B) {
	_, volume := setup(b)

	// run the Fib function b.N times
	bm := bench.Mark{
		Start: time.Now(),
	}

	err := bm.SetTempDir(volume.GetMountPath())
	require.NoError(b, err)

	bm.PhysicalMemory, err = mem.Get()
	require.NoError(b, err)

	bm.NumReadersWriters = runtime.NumCPU()

	bm.AggregateTestFilesSizeInGiB = 1

	bm.IODuration = 15

	err = bm.CreateRandomBlock()
	require.NoError(b, err)

	b.Run("Write", func(b *testing.B) {
		benchmarkWrite(&bm, b)
	})
	b.Run("Read", func(b *testing.B) {
		benchmarkRead(&bm, b)
	})
	b.Run("IOPS", func(b *testing.B) {
		benchmarkIops(&bm, b)
	})
}
