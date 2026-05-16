package perf

import (
	"crypto/rand"
	"io"
	mrand "math/rand"
	"os"
	"path/filepath"
	"testing"
)

const (
	randomFileSize = 64 << 20 // 64 MiB — large enough to defeat any reasonable RAM cache
	randomBlock    = 4096
)

// BenchmarkRandomRead4KiB issues 4 KiB ReadAt at deterministic random offsets
// against a pre-filled file. We hold the file open across iterations so the
// open cost isn't measured.
func BenchmarkRandomRead4KiB(b *testing.B) {
	env := setupBenchEnv(b)
	seedPath := filepath.Join(env.dataDir, "rand_read.bin")
	seedFile(b, seedPath, randomFileSize)
	env.AssertReady(b)

	f, err := os.Open(filepath.Join(env.mountPoint, "rand_read.bin"))
	if err != nil {
		b.Fatalf("open: %v", err)
	}
	defer f.Close()

	// Deterministic seed so reads exercise the same offset pattern across
	// benchstat runs (helps reduce iter-to-iter variance).
	r := mrand.New(mrand.NewSource(1)) //nolint:gosec // non-crypto benchmark
	buf := make([]byte, randomBlock)
	maxOff := int64(randomFileSize - randomBlock)

	b.SetBytes(int64(randomBlock))
	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		off := r.Int63n(maxOff)
		off &^= int64(randomBlock - 1) // 4 KiB-align to match real workloads
		n, err := f.ReadAt(buf, off)
		if err != nil && err != io.EOF {
			b.Fatalf("ReadAt: %v", err)
		}
		if n != randomBlock {
			b.Fatalf("short ReadAt: %d at off %d", n, off)
		}
	}
}

// BenchmarkRandomWrite4KiB issues 4 KiB WriteAt at deterministic random
// offsets against a pre-truncated file. We deliberately do *not* fsync per
// iter — that would measure the host disk, not the gMountie protocol.
func BenchmarkRandomWrite4KiB(b *testing.B) {
	env := setupBenchEnv(b)
	// Pre-truncate the file server-side so the random writes have a stable
	// address space. crypto/rand is overkill but matches the read path.
	seedPath := filepath.Join(env.dataDir, "rand_write.bin")
	f0, err := os.Create(seedPath)
	if err != nil {
		b.Fatalf("create: %v", err)
	}
	if _, err := io.CopyN(f0, rand.Reader, randomFileSize); err != nil {
		b.Fatalf("seed: %v", err)
	}
	_ = f0.Close()
	env.AssertReady(b)

	f, err := os.OpenFile(filepath.Join(env.mountPoint, "rand_write.bin"), os.O_RDWR, 0o600)
	if err != nil {
		b.Fatalf("open: %v", err)
	}
	defer f.Close()

	r := mrand.New(mrand.NewSource(2)) //nolint:gosec // non-crypto benchmark
	buf := make([]byte, randomBlock)
	if _, err := io.ReadFull(rand.Reader, buf); err != nil {
		b.Fatalf("rand buf: %v", err)
	}
	maxOff := int64(randomFileSize - randomBlock)

	b.SetBytes(int64(randomBlock))
	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		off := r.Int63n(maxOff)
		off &^= int64(randomBlock - 1)
		n, err := f.WriteAt(buf, off)
		if err != nil {
			b.Fatalf("WriteAt: %v", err)
		}
		if n != randomBlock {
			b.Fatalf("short WriteAt: %d at off %d", n, off)
		}
	}
}
