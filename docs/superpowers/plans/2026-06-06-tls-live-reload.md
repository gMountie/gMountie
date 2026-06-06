# Server TLS Leaf Live-Reload Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Both server listeners (gRPC + ops) pick up a renewed TLS cert+key from disk at the next handshake — no restart — via a stat-stamped `tls.Config.GetCertificate` callback that fails open to the last good pair.

**Architecture:** A new `Reloader` type in `pkg/server/tls` (the package that owns the server cert lifecycle). Lock-free fast path: stat the cert file, compare an `(mtime, size, inode)` stamp held in an `atomic.Pointer`, return the cached `*tls.Certificate`. On stamp change, a `TryLock`-guarded slow path reloads the pair; losers and any reload failure serve the cached pair. Wire it into `app.go` and `ops.ApplyTLS` in place of the static `Certificates` slice.

**Tech Stack:** Go stdlib (`crypto/tls`, `os`, `sync/atomic`), `go.uber.org/zap` via `pkg/utils/log`, testify suites. No new dependencies.

**Spec:** `docs/superpowers/specs/2026-06-06-tls-live-reload-design.md` (read it first).

**Worktree:** all work in `/home/john/git/gMountie/gMountie/.claude/worktrees/tls-reload` (branch `worktree-tls-reload`). Run all commands from that directory.

---

### Task 1: stamp + statStamp (file-identity check)

**Files:**
- Create: `pkg/server/tls/reloader.go`
- Create: `pkg/server/tls/reloader_stat_unix.go`
- Create: `pkg/server/tls/reloader_stat_other.go`
- Create: `pkg/server/tls/reloader_test.go`

- [ ] **Step 1: Write the failing test**

`pkg/server/tls/reloader_test.go`:

```go
package tls

import (
	"crypto/tls"
	"crypto/x509"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/suite"
)
```

(`net` and `sync` are added to this import block later, by Tasks 6 and 5
respectively — Go rejects unused imports, so don't add them now.)

```go

type ReloaderSuite struct{ suite.Suite }

func TestReloaderSuite(t *testing.T) { suite.Run(t, new(ReloaderSuite)) }

// writeAtomic writes data via tmp-file + rename — the atomic-swap pattern
// rotation tooling (kubelet, cert-manager) uses. The rename also guarantees
// a new inode, so every rotation produces a new stamp even within the same
// mtime second.
func (s *ReloaderSuite) writeAtomic(path string, data []byte) {
	s.T().Helper()
	tmp := path + ".tmp"
	s.Require().NoError(os.WriteFile(tmp, data, 0o600))
	s.Require().NoError(os.Rename(tmp, path))
}

// writePair generates a fresh self-signed pair for host and writes it
// atomically to dir as tls.crt / tls.key.
func (s *ReloaderSuite) writePair(dir, host string) (certPath, keyPath string) {
	s.T().Helper()
	certPEM, keyPEM, err := Generate(host)
	s.Require().NoError(err)
	certPath = filepath.Join(dir, "tls.crt")
	keyPath = filepath.Join(dir, "tls.key")
	s.writeAtomic(certPath, certPEM)
	s.writeAtomic(keyPath, keyPEM)
	return certPath, keyPath
}

// serialOf returns the leaf certificate's serial as a string.
func (s *ReloaderSuite) serialOf(c *tls.Certificate) string {
	s.T().Helper()
	s.Require().NotNil(c)
	s.Require().NotEmpty(c.Certificate)
	leaf, err := x509.ParseCertificate(c.Certificate[0])
	s.Require().NoError(err)
	return leaf.SerialNumber.String()
}

func (s *ReloaderSuite) TestStampChangesOnAtomicRotation() {
	dir := s.T().TempDir()
	certPath, _ := s.writePair(dir, "stamp.example.com")

	before, err := statStamp(certPath)
	s.Require().NoError(err)

	s.writePair(dir, "stamp.example.com") // rotate in place

	after, err := statStamp(certPath)
	s.Require().NoError(err)
	s.False(before.equal(after), "atomic rotation must produce a new stamp")
	s.True(after.equal(after), "a stamp must equal itself")
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd /home/john/git/gMountie/gMountie/.claude/worktrees/tls-reload && go test ./pkg/server/tls/ -run TestReloaderSuite -v`
Expected: FAIL — `undefined: statStamp`

- [ ] **Step 3: Write minimal implementation**

`pkg/server/tls/reloader.go`:

```go
package tls

import (
	"os"
	"time"
)

// stamp identifies a cert file's on-disk version. The inode catches the
// kubelet projected-volume rotation (an atomic ..data symlink swap = a new
// inode) and same-second mtime granularity; os.Stat follows symlinks so the
// projected layout is transparent.
type stamp struct {
	modTime time.Time
	size    int64
	inode   uint64
}

// equal compares stamps. modTime is compared with Equal (never ==) so a
// monotonic-clock component can't poison the comparison.
func (a stamp) equal(b stamp) bool {
	return a.modTime.Equal(b.modTime) && a.size == b.size && a.inode == b.inode
}

// statStamp reads the current stamp of path.
func statStamp(path string) (stamp, error) {
	fi, err := os.Stat(path)
	if err != nil {
		return stamp{}, err
	}
	return stamp{modTime: fi.ModTime(), size: fi.Size(), inode: inodeOf(fi)}, nil
}
```

`pkg/server/tls/reloader_stat_unix.go`:

```go
//go:build unix

package tls

import (
	"os"
	"syscall"
)

// inodeOf extracts the file's inode where the platform exposes one.
func inodeOf(fi os.FileInfo) uint64 {
	if st, ok := fi.Sys().(*syscall.Stat_t); ok {
		return st.Ino
	}
	return 0
}
```

`pkg/server/tls/reloader_stat_other.go`:

```go
//go:build !unix

package tls

import "os"

// inodeOf is unavailable off unix; the (mtime, size) pair still detects
// rotation in practice there.
func inodeOf(os.FileInfo) uint64 { return 0 }
```

- [ ] **Step 4: Run test to verify it passes**

Run: `go test ./pkg/server/tls/ -run TestReloaderSuite -v`
Expected: PASS (TestStampChangesOnAtomicRotation)

- [ ] **Step 5: Commit**

```bash
git add pkg/server/tls/reloader.go pkg/server/tls/reloader_stat_unix.go pkg/server/tls/reloader_stat_other.go pkg/server/tls/reloader_test.go
git commit -m "feat(server/tls): file-identity stamp for cert rotation detection

(mtime, size, inode) — inode catches the kubelet atomic symlink swap and
same-second mtime granularity. First slice of the TLS leaf live-reload
(spec: docs/superpowers/specs/2026-06-06-tls-live-reload-design.md)."
```

---

### Task 2: Reloader — initial load + stable serving

**Files:**
- Modify: `pkg/server/tls/reloader.go`
- Modify: `pkg/server/tls/reloader_test.go`

- [ ] **Step 1: Write the failing tests**

Append to `pkg/server/tls/reloader_test.go`:

```go
func (s *ReloaderSuite) TestServesInitialPair() {
	dir := s.T().TempDir()
	certPath, keyPath := s.writePair(dir, "initial.example.com")
	wantCert, _, err := Load(certPath, keyPath)
	s.Require().NoError(err)

	r, err := NewReloader(certPath, keyPath)
	s.Require().NoError(err)

	got, err := r.GetCertificate(nil)
	s.Require().NoError(err)
	s.Equal(s.serialOf(&wantCert), s.serialOf(got))
}

func (s *ReloaderSuite) TestPointerStableWhenUnchanged() {
	dir := s.T().TempDir()
	certPath, keyPath := s.writePair(dir, "stable.example.com")
	r, err := NewReloader(certPath, keyPath)
	s.Require().NoError(err)

	first, err := r.GetCertificate(nil)
	s.Require().NoError(err)
	second, err := r.GetCertificate(nil)
	s.Require().NoError(err)
	s.Same(first, second, "unchanged stamp must not re-load the pair")
}

func (s *ReloaderSuite) TestNewReloaderFailsFastOnBadPaths() {
	_, err := NewReloader("/nonexistent/tls.crt", "/nonexistent/tls.key")
	s.Error(err)
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `go test ./pkg/server/tls/ -run TestReloaderSuite -v`
Expected: FAIL — `undefined: NewReloader`

- [ ] **Step 3: Write minimal implementation**

Append to `pkg/server/tls/reloader.go` (add the new imports to the existing block):

```go
import (
	"crypto/tls"
	"os"
	"sync"
	"sync/atomic"
	"time"
)

// Reloader serves a TLS cert+key pair from disk, transparently reloading
// it when the cert file changes (Task 3). Safe for concurrent use as a
// tls.Config.GetCertificate callback.
type Reloader struct {
	certPath string
	keyPath  string

	current atomic.Pointer[tls.Certificate]
	loaded  atomic.Pointer[stamp]

	// mu serializes reloads and guards fingerprint. failing is atomic so
	// the lock-free stat-failure path can warn-once too.
	mu          sync.Mutex
	fingerprint string
	failing     atomic.Bool
}

// NewReloader loads the initial pair — failing fast on a bad cert exactly
// like the static startup path — and caches the cert file's stamp.
func NewReloader(certPath, keyPath string) (*Reloader, error) {
	cert, certPEM, err := Load(certPath, keyPath)
	if err != nil {
		return nil, err
	}
	st, err := statStamp(certPath)
	if err != nil {
		return nil, err
	}
	fp, err := Fingerprint(certPEM)
	if err != nil {
		return nil, err
	}
	r := &Reloader{certPath: certPath, keyPath: keyPath, fingerprint: fp}
	r.current.Store(&cert)
	r.loaded.Store(&st)
	return r, nil
}

// GetCertificate is a tls.Config.GetCertificate callback: it serves the
// cached pair. (Rotation handling lands in the next slice.)
func (r *Reloader) GetCertificate(*tls.ClientHelloInfo) (*tls.Certificate, error) {
	return r.current.Load(), nil
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `go test ./pkg/server/tls/ -run TestReloaderSuite -v`
Expected: PASS (all four tests)

- [ ] **Step 5: Commit**

```bash
git add pkg/server/tls/reloader.go pkg/server/tls/reloader_test.go
git commit -m "feat(server/tls): Reloader skeleton — fail-fast initial load, cached serving"
```

---

### Task 3: rotation reload on stamp change

**Files:**
- Modify: `pkg/server/tls/reloader.go`
- Modify: `pkg/server/tls/reloader_test.go`

- [ ] **Step 1: Write the failing test**

Append to `pkg/server/tls/reloader_test.go`:

```go
func (s *ReloaderSuite) TestReloadsOnRotation() {
	dir := s.T().TempDir()
	certPath, keyPath := s.writePair(dir, "rotate.example.com")
	r, err := NewReloader(certPath, keyPath)
	s.Require().NoError(err)

	before, err := r.GetCertificate(nil)
	s.Require().NoError(err)

	s.writePair(dir, "rotate.example.com") // rotate: fresh pair, same paths

	after, err := r.GetCertificate(nil)
	s.Require().NoError(err)
	s.NotEqual(s.serialOf(before), s.serialOf(after),
		"handshake after rotation must serve the new leaf")

	// And the new pair is now the stable cached one.
	again, err := r.GetCertificate(nil)
	s.Require().NoError(err)
	s.Same(after, again)
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./pkg/server/tls/ -run TestReloaderSuite -v`
Expected: FAIL — TestReloadsOnRotation: serials equal (no reload happens yet)

- [ ] **Step 3: Implement the reload path**

Replace `GetCertificate` in `pkg/server/tls/reloader.go` with (and add the imports `go.gmountie.dev/gmountie/pkg/utils/log` + `go.uber.org/zap`):

```go
// GetCertificate is a tls.Config.GetCertificate callback: it serves the
// cached pair, reloading it first when the cert file's stamp has changed.
//
// Fail-open contract: any stat or reload failure (file briefly missing
// mid-rotation, cert/key mismatch from a non-atomic swap, unparsable PEM)
// keeps the last good pair — a handshake the old cert could serve is never
// failed by the reloader. Failures warn once per streak, not per handshake.
func (r *Reloader) GetCertificate(*tls.ClientHelloInfo) (*tls.Certificate, error) {
	st, err := statStamp(r.certPath)
	if err != nil {
		r.warnOnce("server cert stat failed; serving previous cert", err)
		return r.current.Load(), nil
	}
	if st.equal(*r.loaded.Load()) {
		return r.current.Load(), nil
	}

	// Stamp changed: one handshake reloads, concurrent ones keep serving
	// the cached pair without blocking on disk I/O.
	if !r.mu.TryLock() {
		return r.current.Load(), nil
	}
	defer r.mu.Unlock()
	if st.equal(*r.loaded.Load()) { // raced with a completed reload
		return r.current.Load(), nil
	}

	cert, certPEM, err := Load(r.certPath, r.keyPath)
	if err != nil {
		r.warnOnce("server cert changed on disk but reload failed; serving previous cert", err)
		return r.current.Load(), nil
	}
	fp, err := Fingerprint(certPEM)
	if err != nil {
		r.warnOnce("server cert changed on disk but reload failed; serving previous cert", err)
		return r.current.Load(), nil
	}

	if r.failing.Swap(false) {
		log.Log.Info("server cert reload recovered", zap.String("cert_path", r.certPath))
	}
	log.Log.Info("server cert reloaded",
		zap.String("cert_path", r.certPath),
		zap.String("old_fingerprint", r.fingerprint),
		zap.String("fingerprint", fp))
	r.fingerprint = fp
	r.current.Store(&cert)
	r.loaded.Store(&st)
	return r.current.Load(), nil
}

// warnOnce logs the first failure of a streak; subsequent failures are
// silent until a reload succeeds (no per-handshake log spam).
func (r *Reloader) warnOnce(msg string, err error) {
	if r.failing.CompareAndSwap(false, true) {
		log.Log.Warn(msg, zap.String("cert_path", r.certPath), zap.Error(err))
	}
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `go test ./pkg/server/tls/ -run TestReloaderSuite -v`
Expected: PASS (all five tests)

- [ ] **Step 5: Commit**

```bash
git add pkg/server/tls/reloader.go pkg/server/tls/reloader_test.go
git commit -m "feat(server/tls): reload the pair when the cert file's stamp changes

TryLock keeps concurrent handshakes serving the cached pair instead of
queueing on disk I/O; old->new fingerprint logged on each swap."
```

---

### Task 4: fail-open — mismatch, missing file

**Files:**
- Modify: `pkg/server/tls/reloader_test.go` (implementation should already satisfy these; they pin the contract)

- [ ] **Step 1: Write the tests**

Append to `pkg/server/tls/reloader_test.go`:

```go
func (s *ReloaderSuite) TestKeepsServingOnMismatchedPair() {
	dir := s.T().TempDir()
	certPath, keyPath := s.writePair(dir, "mismatch.example.com")
	r, err := NewReloader(certPath, keyPath)
	s.Require().NoError(err)
	before, err := r.GetCertificate(nil)
	s.Require().NoError(err)

	// Rotate ONLY the cert (a torn, non-atomic swap): pair now mismatched.
	certPEM, _, err := Generate("mismatch.example.com")
	s.Require().NoError(err)
	s.writeAtomic(certPath, certPEM)

	got, err := r.GetCertificate(nil)
	s.Require().NoError(err, "fail-open: mismatch must not fail the handshake")
	s.Equal(s.serialOf(before), s.serialOf(got), "must keep the last good pair")

	// Completing the rotation (matching key) recovers on the next handshake.
	s.writePair(dir, "mismatch.example.com")
	after, err := r.GetCertificate(nil)
	s.Require().NoError(err)
	s.NotEqual(s.serialOf(before), s.serialOf(after))
}

func (s *ReloaderSuite) TestKeepsServingOnMissingFile() {
	dir := s.T().TempDir()
	certPath, keyPath := s.writePair(dir, "missing.example.com")
	r, err := NewReloader(certPath, keyPath)
	s.Require().NoError(err)
	before, err := r.GetCertificate(nil)
	s.Require().NoError(err)

	s.Require().NoError(os.Remove(certPath))

	got, err := r.GetCertificate(nil)
	s.Require().NoError(err, "fail-open: missing file must not fail the handshake")
	s.Equal(s.serialOf(before), s.serialOf(got))
	_ = keyPath
}
```

- [ ] **Step 2: Run tests — expected to pass against Task 3's implementation**

Run: `go test ./pkg/server/tls/ -run TestReloaderSuite -v`
Expected: PASS. If TestKeepsServingOnMismatchedPair fails at the recovery assertion, the bug is the `failing` flag short-circuiting reloads — re-read Task 3's `GetCertificate`: the failing flag must never gate the reload attempt, only the logging.

- [ ] **Step 3: Commit**

```bash
git add pkg/server/tls/reloader_test.go
git commit -m "test(server/tls): pin the fail-open contract — torn swap and missing file keep the last good pair"
```

---

### Task 5: concurrency under rotation (race)

**Files:**
- Modify: `pkg/server/tls/reloader_test.go`

- [ ] **Step 1: Write the test**

Add `"sync"` to `reloader_test.go`'s import block, then append:

```go
func (s *ReloaderSuite) TestConcurrentHandshakesDuringRotation() {
	dir := s.T().TempDir()
	certPath, keyPath := s.writePair(dir, "race.example.com")
	r, err := NewReloader(certPath, keyPath)
	s.Require().NoError(err)
	_ = keyPath

	const goroutines = 8
	const iterations = 200
	var wg sync.WaitGroup
	rotate := make(chan struct{})

	for range goroutines {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range iterations {
				c, err := r.GetCertificate(nil)
				// assert.* (not Require) — testify forbids FailNow off
				// the test goroutine.
				s.NoError(err)
				s.NotNil(c)
			}
		}()
	}
	go func() {
		<-rotate
		s.writePair(dir, "race.example.com")
	}()
	close(rotate)
	wg.Wait()

	// After the dust settles, the new pair is served.
	want, _, err := Load(certPath, filepath.Join(dir, "tls.key"))
	s.Require().NoError(err)
	got, err := r.GetCertificate(nil)
	s.Require().NoError(err)
	s.Equal(s.serialOf(&want), s.serialOf(got))
}
```

- [ ] **Step 2: Run with the race detector**

Run: `go test ./pkg/server/tls/ -run TestReloaderSuite -race -count=1 -v`
Expected: PASS, no race reports.

- [ ] **Step 3: Commit**

```bash
git add pkg/server/tls/reloader_test.go
git commit -m "test(server/tls): concurrent handshakes racing a rotation, under -race"
```

---

### Task 6: listener integration — real handshakes across a rotation

**Files:**
- Modify: `pkg/server/tls/reloader_test.go`

- [ ] **Step 1: Write the test**

Add `"net"` to `reloader_test.go`'s import block, then append:

```go
func (s *ReloaderSuite) TestListenerServesRotatedCert() {
	dir := s.T().TempDir()
	certPath, keyPath := s.writePair(dir, "127.0.0.1")
	r, err := NewReloader(certPath, keyPath)
	s.Require().NoError(err)

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	s.Require().NoError(err)
	defer ln.Close()
	tlsLn := tls.NewListener(ln, &tls.Config{
		MinVersion:     tls.VersionTLS12,
		GetCertificate: r.GetCertificate,
	})
	go func() {
		for {
			conn, err := tlsLn.Accept()
			if err != nil {
				return
			}
			go func() {
				_ = conn.(*tls.Conn).Handshake()
				_ = conn.Close()
			}()
		}
	}()

	dialSerial := func() string {
		conn, err := tls.Dial("tcp", ln.Addr().String(),
			&tls.Config{InsecureSkipVerify: true}) //nolint:gosec // test dials a self-signed cert
		s.Require().NoError(err)
		defer conn.Close()
		return conn.ConnectionState().PeerCertificates[0].SerialNumber.String()
	}

	first := dialSerial()
	s.writePair(dir, "127.0.0.1") // rotate on disk; no restart
	second := dialSerial()
	s.NotEqual(first, second, "post-rotation handshake must present the new leaf")
}
```

- [ ] **Step 2: Run the suite**

Run: `go test ./pkg/server/tls/ -race -count=1 -v`
Expected: PASS (whole package, including the pre-existing CertSuite).

- [ ] **Step 3: Commit**

```bash
git add pkg/server/tls/reloader_test.go
git commit -m "test(server/tls): real TLS listener presents the rotated leaf without restart"
```

---

### Task 7: wire the main gRPC listener (app.go)

**Files:**
- Modify: `pkg/server/app.go` (the TLS block, currently ~lines 180-188)

- [ ] **Step 1: Make the change**

In `pkg/server/app.go`, the current block:

```go
		cert, _, fp, err := servertls.LoadOrGenerate(certPath, keyPath, host)
		if err != nil {
			return errors.Wrap(err, "load/generate server cert")
		}
		tlsCfg := &tls.Config{
			MinVersion:   minTLSVersion(cfg.Server.TLS.MinVersion),
			NextProtos:   []string{"h2"},
			Certificates: []tls.Certificate{cert},
		}
```

becomes:

```go
		// LoadOrGenerate keeps the generate-on-first-boot + fail-fast
		// behavior; the Reloader then owns the pair so a rotated cert is
		// picked up at the next handshake without a restart.
		_, _, fp, err := servertls.LoadOrGenerate(certPath, keyPath, host)
		if err != nil {
			return errors.Wrap(err, "load/generate server cert")
		}
		reloader, err := servertls.NewReloader(certPath, keyPath)
		if err != nil {
			return errors.Wrap(err, "init server cert reloader")
		}
		tlsCfg := &tls.Config{
			MinVersion:     minTLSVersion(cfg.Server.TLS.MinVersion),
			NextProtos:     []string{"h2"},
			GetCertificate: reloader.GetCertificate,
		}
```

Everything below (the mTLS `ClientCAs` block, `VerifyPeerCertificate`, the fingerprint log) is untouched.

- [ ] **Step 2: Build + run the server package tests**

Run: `go build ./... && go test ./pkg/server/ -count=1`
Expected: build OK, tests PASS. If a server test asserts `tlsCfg.Certificates` is non-empty, update that assertion to `GetCertificate != nil` — the config shape changed deliberately.

- [ ] **Step 3: Commit**

```bash
git add pkg/server/app.go
git commit -m "feat(server): gRPC listener serves certs through the reloader

Rotation (cert-manager-style file replacement) now reaches new handshakes
without a restart; startup behavior (generate-on-first-boot, fail-fast,
fingerprint log) is unchanged."
```

---

### Task 8: wire the ops listener (ApplyTLS)

**Files:**
- Modify: `pkg/server/ops/server.go:75-86` (`ApplyTLS`)
- Modify: `pkg/server/ops/server_test.go`

- [ ] **Step 1: Write the failing test**

Append to `pkg/server/ops/server_test.go` (the file already imports `servertls` and has `writeOpsCertKeyCA`):

```go
func (s *ServerTestSuite) TestApplyTLSServesRotatedCert() {
	dir := s.T().TempDir()
	certFile, keyFile, _ := writeOpsCertKeyCA(s.T(), dir)

	srv, err := NewServerWithTLS("127.0.0.1:0", serverconfig.OpsTLSConfig{
		CertFile: certFile, KeyFile: keyFile,
	})
	s.Require().NoError(err)

	tc := srv.tlsConfig()
	s.Require().NotNil(tc)
	s.NotNil(tc.GetCertificate, "ops TLS must serve through the reloader")
	s.Empty(tc.Certificates, "no static cert — rotation would never reach it")

	// The callback picks up an on-disk rotation.
	before, err := tc.GetCertificate(nil)
	s.Require().NoError(err)
	certPEM, keyPEM, err := servertls.Generate("ops-rotated.example.com")
	s.Require().NoError(err)
	s.Require().NoError(os.WriteFile(certFile+".tmp", certPEM, 0o600))
	s.Require().NoError(os.Rename(certFile+".tmp", certFile))
	s.Require().NoError(os.WriteFile(keyFile+".tmp", keyPEM, 0o600))
	s.Require().NoError(os.Rename(keyFile+".tmp", keyFile))
	after, err := tc.GetCertificate(nil)
	s.Require().NoError(err)
	s.NotEqual(before, after)
}
```

- [ ] **Step 2: Run to verify it fails**

Run: `go test ./pkg/server/ops/ -run TestServerTestSuite -v`
Expected: FAIL — `GetCertificate` is nil (ApplyTLS still sets static `Certificates`).

- [ ] **Step 3: Implement**

In `pkg/server/ops/server.go`, `ApplyTLS`, replace:

```go
	cert, err := tls.LoadX509KeyPair(cfg.CertFile, cfg.KeyFile)
	if err != nil {
		return pkgerrors.Wrap(err, "load ops TLS keypair")
	}
	tc := &tls.Config{
		MinVersion:   tls.VersionTLS12,
		Certificates: []tls.Certificate{cert},
	}
```

with:

```go
	// Serve through the reloader so a rotated cert (cert-manager-style
	// file replacement) reaches new handshakes without a restart — the
	// operator's revocation reloads handshake against this listener.
	reloader, err := servertls.NewReloader(cfg.CertFile, cfg.KeyFile)
	if err != nil {
		return pkgerrors.Wrap(err, "load ops TLS keypair")
	}
	tc := &tls.Config{
		MinVersion:     tls.VersionTLS12,
		GetCertificate: reloader.GetCertificate,
	}
```

and add the import `servertls "go.gmountie.dev/gmountie/pkg/server/tls"` to `server.go`.

- [ ] **Step 4: Run the ops tests**

Run: `go test ./pkg/server/ops/ -race -count=1 -v`
Expected: PASS. Existing tests that asserted on `tc.Certificates` (if any — check `server_test.go` and `reload_test.go`) must be updated to assert `GetCertificate != nil` instead; the error-path test for a bad keypair still passes because `NewReloader` wraps the same `Load` failure.

- [ ] **Step 5: Commit**

```bash
git add pkg/server/ops/server.go pkg/server/ops/server_test.go
git commit -m "feat(server/ops): ops listener serves certs through the reloader

Load-bearing for hosted deployments: the operator's revocation reloads
handshake against this listener, so a stale in-memory leaf would break
revocation exactly when the cert ages out."
```

---

### Task 9: changelog + full verification

**Files:**
- Modify: `CHANGELOG.md` (the `## Unreleased` → `### Headline features` list)

- [ ] **Step 1: Add the changelog entry**

Append this bullet to the `### Headline features` list under `## Unreleased` in `CHANGELOG.md`:

```markdown
- **Server TLS leaf live-reload.** Both the gRPC and ops listeners pick up a renewed cert+key from disk at the next handshake (stat-stamped `GetCertificate`, fail-open to the last good pair) — cert-manager-style rotation no longer needs a restart, and existing sessions are never disturbed. Note for fingerprint-pinning setups: replacing the cert files changes the fingerprint clients must pin; nothing changes unless you replace the files.
```

- [ ] **Step 2: Full verification**

Run, from the worktree root:

```bash
gofmt -l ./pkg/ | (! grep .)        # no unformatted files
go build ./...
go test ./pkg/server/... -race -count=1
```

Expected: all green. (Repo-wide `task lint` is noisy locally per project memory — CI's lint job is the arbiter; rely on gofmt + build + the package tests here.)

- [ ] **Step 3: Commit**

```bash
git add CHANGELOG.md
git commit -m "docs: changelog for the server TLS leaf live-reload"
```

---

## Done criteria

- `go test ./pkg/server/... -race` green; no race reports.
- Rotating `tls.crt`/`tls.key` on disk changes the leaf served to the *next* connection on both listeners, with zero restarts and zero impact on established sessions (proven by TestListenerServesRotatedCert + TestApplyTLSServesRotatedCert).
- A torn/partial rotation never breaks handshakes the old cert could serve (TestKeepsServingOnMismatchedPair / TestKeepsServingOnMissingFile).
- Push branch + open the PR (conventional title `feat(server): TLS leaf live-reload — rotation without restarts`, body explains the deterministic-outage problem and the fail-open contract; no AI attribution).
