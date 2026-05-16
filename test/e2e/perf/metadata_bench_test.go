package perf

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"
)

// BenchmarkOpenStatClose measures the cost of os.Stat against a path that
// already exists. Each iteration is a Lookup+Getattr round-trip on the gMountie
// wire.
func BenchmarkOpenStatClose(b *testing.B) {
	env := setupBenchEnv(b)
	path := filepath.Join(env.dataDir, "stat.target")
	if err := os.WriteFile(path, []byte("x"), 0o600); err != nil {
		b.Fatalf("seed: %v", err)
	}
	clientPath := filepath.Join(env.mountPoint, "stat.target")
	env.AssertReady(b)

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		fi, err := os.Stat(clientPath)
		if err != nil {
			b.Fatalf("stat: %v", err)
		}
		if fi.Size() != 1 {
			b.Fatalf("unexpected size: %d", fi.Size())
		}
	}
}

// BenchmarkReaddir100 measures listing a directory of 100 files. Each
// iteration is a fresh OpenDir + ReadDir + Close cycle.
func BenchmarkReaddir100(b *testing.B) {
	env := setupBenchEnv(b)
	dir := filepath.Join(env.dataDir, "ls100")
	if err := os.Mkdir(dir, 0o755); err != nil {
		b.Fatalf("mkdir: %v", err)
	}
	for i := 0; i < 100; i++ {
		if err := os.WriteFile(filepath.Join(dir, fmt.Sprintf("f%03d", i)), nil, 0o600); err != nil {
			b.Fatalf("seed file: %v", err)
		}
	}
	clientDir := filepath.Join(env.mountPoint, "ls100")
	env.AssertReady(b)

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		ents, err := os.ReadDir(clientDir)
		if err != nil {
			b.Fatalf("readdir: %v", err)
		}
		if len(ents) != 100 {
			b.Fatalf("unexpected count: %d", len(ents))
		}
	}
}

// BenchmarkLookup measures the lighter os.Lstat path — no symlink traversal,
// just the parent-directory lookup of a name. Useful as a proxy for the cost
// of a single FUSE Lookup op.
func BenchmarkLookup(b *testing.B) {
	env := setupBenchEnv(b)
	path := filepath.Join(env.dataDir, "lookup.target")
	if err := os.WriteFile(path, nil, 0o600); err != nil {
		b.Fatalf("seed: %v", err)
	}
	clientPath := filepath.Join(env.mountPoint, "lookup.target")
	env.AssertReady(b)

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		if _, err := os.Lstat(clientPath); err != nil {
			b.Fatalf("lstat: %v", err)
		}
	}
}
