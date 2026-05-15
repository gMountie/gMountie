package perf

import (
	"crypto/rand"
	"io"
	"os"
	"path/filepath"
	"testing"
)

const (
	sizeMiB1  = 1 << 20
	sizeMiB16 = 16 << 20
	sizeMiB64 = 64 << 20
)

// seedFile writes a file of size bytes filled with pseudo-random content. Used
// to pre-fill the server-side dataDir directly (no FUSE) so the read path can
// be measured in isolation.
func seedFile(b *testing.B, path string, size int) {
	b.Helper()
	f, err := os.Create(path)
	if err != nil {
		b.Fatalf("create seed: %v", err)
	}
	defer f.Close()
	// rand.Reader is fine — we don't need cryptographic quality, just bytes
	// that won't get heavily compressed by Snappy.
	if _, err := io.CopyN(f, rand.Reader, int64(size)); err != nil {
		b.Fatalf("seed write: %v", err)
	}
	if err := f.Sync(); err != nil {
		b.Fatalf("seed sync: %v", err)
	}
}

func benchmarkSeqRead(b *testing.B, size int) {
	env := setupBenchEnv(b)
	seedPath := filepath.Join(env.dataDir, "seq_read.bin")
	seedFile(b, seedPath, size)

	clientPath := filepath.Join(env.mountPoint, "seq_read.bin")
	buf := make([]byte, 256<<10)

	b.SetBytes(int64(size))
	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		f, err := os.Open(clientPath)
		if err != nil {
			b.Fatalf("open: %v", err)
		}
		n, err := io.CopyBuffer(io.Discard, f, buf)
		if err != nil {
			b.Fatalf("copy: %v", err)
		}
		if n != int64(size) {
			b.Fatalf("short read: got %d want %d", n, size)
		}
		if err := f.Close(); err != nil {
			b.Fatalf("close: %v", err)
		}
	}
}

func BenchmarkSeqRead1MiB(b *testing.B)  { benchmarkSeqRead(b, sizeMiB1) }
func BenchmarkSeqRead16MiB(b *testing.B) { benchmarkSeqRead(b, sizeMiB16) }

// SeqRead64MiB stands in for the originally-planned 1GiB case. We capped it
// for VM tractability: a 1GiB sequential read on the slow30ms profile would
// exceed the per-bench budget by an order of magnitude, and 64 MiB already
// flushes per-connection windows / buffers comfortably while staying inside
// the VM's 3.8 GiB of RAM headroom.
func BenchmarkSeqRead64MiB(b *testing.B) { benchmarkSeqRead(b, sizeMiB64) }

func benchmarkSeqWrite(b *testing.B, size int) {
	env := setupBenchEnv(b)
	clientPath := filepath.Join(env.mountPoint, "seq_write.bin")

	// Reuse a single random payload across iterations — generating fresh
	// crypto/rand bytes for every loop would dominate the measurement.
	payload := make([]byte, size)
	if _, err := io.ReadFull(rand.Reader, payload); err != nil {
		b.Fatalf("rand payload: %v", err)
	}

	b.SetBytes(int64(size))
	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		f, err := os.Create(clientPath)
		if err != nil {
			b.Fatalf("create: %v", err)
		}
		n, err := f.Write(payload)
		if err != nil {
			b.Fatalf("write: %v", err)
		}
		if n != size {
			b.Fatalf("short write: %d", n)
		}
		if err := f.Close(); err != nil {
			b.Fatalf("close: %v", err)
		}
		// Remove between iterations so each write starts from a fresh inode.
		// The measurement boundary intentionally includes the Create+Close
		// round trips; that's representative of real workloads.
		if err := os.Remove(clientPath); err != nil {
			b.Fatalf("remove: %v", err)
		}
	}
}

func BenchmarkSeqWrite1MiB(b *testing.B)  { benchmarkSeqWrite(b, sizeMiB1) }
func BenchmarkSeqWrite16MiB(b *testing.B) { benchmarkSeqWrite(b, sizeMiB16) }
func BenchmarkSeqWrite64MiB(b *testing.B) { benchmarkSeqWrite(b, sizeMiB64) }
