package tls

import (
	"crypto/tls"
	"crypto/x509"
	"io"
	"net"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/stretchr/testify/suite"
)

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
	s.True(r.failing.Load(), "torn swap must arm the warn-once flag")
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
	s.True(r.failing.Load(), "missing cert must arm the warn-once flag")
	s.Equal(s.serialOf(before), s.serialOf(got))
}

func (s *ReloaderSuite) TestConcurrentHandshakesDuringRotation() {
	dir := s.T().TempDir()
	certPath, keyPath := s.writePair(dir, "race.example.com")
	r, err := NewReloader(certPath, keyPath)
	s.Require().NoError(err)
	_ = keyPath

	const goroutines = 8
	const iterations = 200
	var wg sync.WaitGroup

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
	s.writePair(dir, "race.example.com")
	wg.Wait()

	// After the dust settles, the new pair is served.
	want, _, err := Load(certPath, filepath.Join(dir, "tls.key"))
	s.Require().NoError(err)
	got, err := r.GetCertificate(nil)
	s.Require().NoError(err)
	s.Equal(s.serialOf(&want), s.serialOf(got))
}

func (s *ReloaderSuite) TestWarnOnceRearmsAfterTransientStatBlip() {
	dir := s.T().TempDir()
	certPath, keyPath := s.writePair(dir, "blip.example.com")
	r, err := NewReloader(certPath, keyPath)
	s.Require().NoError(err)
	_ = keyPath

	// Transient blip: cert briefly missing, then restored unchanged.
	original, err := os.ReadFile(certPath)
	s.Require().NoError(err)
	s.Require().NoError(os.Remove(certPath))
	_, err = r.GetCertificate(nil) // stat fails -> warns once, arms failing
	s.Require().NoError(err)
	s.True(r.failing.Load(), "stat failure must arm the warn-once flag")

	s.writeAtomic(certPath, original) // blip over; stamp differs (new inode)
	// The restored file has a NEW stamp (rename = new inode), so this passes
	// through the reload path and must clear the flag there...
	_, err = r.GetCertificate(nil)
	s.Require().NoError(err)
	s.False(r.failing.Load(), "recovered serve must re-arm warn-once")

	// ...and the pure fast path must also clear a stale flag: arm it
	// artificially and take the stamp-unchanged path.
	r.failing.Store(true)
	_, err = r.GetCertificate(nil)
	s.Require().NoError(err)
	s.False(r.failing.Load(),
		"stamp-unchanged fast path must clear a stale failing flag")
}

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
				tconn := conn.(*tls.Conn)
				if err := tconn.Handshake(); err != nil {
					_ = conn.Close()
					return
				}
				_, _ = io.Copy(conn, conn) // echo until the client closes
				_ = conn.Close()
			}()
		}
	}()

	dial := func() *tls.Conn {
		conn, err := tls.Dial("tcp", ln.Addr().String(),
			&tls.Config{InsecureSkipVerify: true}) //nolint:gosec // test dials a self-signed cert
		s.Require().NoError(err)
		return conn
	}
	serialOfConn := func(c *tls.Conn) string {
		return c.ConnectionState().PeerCertificates[0].SerialNumber.String()
	}

	first := dial()
	defer first.Close()
	firstSerial := serialOfConn(first)

	s.writePair(dir, "127.0.0.1") // rotate on disk; no restart

	second := dial()
	defer second.Close()
	s.NotEqual(firstSerial, serialOfConn(second),
		"post-rotation handshake must present the new leaf")

	// The pre-rotation session is never disturbed: it still round-trips.
	_, err = first.Write([]byte("ping"))
	s.Require().NoError(err)
	buf := make([]byte, 4)
	_, err = io.ReadFull(first, buf)
	s.Require().NoError(err)
	s.Equal("ping", string(buf))
}

// fakeReloadMetrics is a minimal ReloadMetrics for asserting reload accounting.
type fakeReloadMetrics struct {
	failures  int
	successes int
}

func (f *fakeReloadMetrics) TLSReloadFailureInc() { f.failures++ }
func (f *fakeReloadMetrics) TLSReloadSucceeded()  { f.successes++ }

// TestMetricsCountReloadFailureAndSuccess covers OB-M2: a failed reload bumps
// the failure counter (every attempt, not just the first of a streak) and a
// successful reload stamps the success sink.
func (s *ReloaderSuite) TestMetricsCountReloadFailureAndSuccess() {
	dir := s.T().TempDir()
	certPath, keyPath := s.writePair(dir, "metrics.example.com")
	fm := &fakeReloadMetrics{}
	r, err := NewReloader(certPath, keyPath, WithMetrics(fm))
	s.Require().NoError(err)

	// Torn swap: cert rotated, key stale ⇒ load fails ⇒ failure counted.
	certPEM, _, err := Generate("metrics.example.com")
	s.Require().NoError(err)
	s.writeAtomic(certPath, certPEM)
	_, err = r.GetCertificate(nil)
	s.Require().NoError(err) // fail-open
	s.Equal(1, fm.failures, "a failed reload must bump the failure counter")
	s.Equal(0, fm.successes)

	// Complete the rotation: a matching pair ⇒ success stamp.
	s.writePair(dir, "metrics.example.com")
	_, err = r.GetCertificate(nil)
	s.Require().NoError(err)
	s.Equal(1, fm.successes, "a successful reload must stamp the success sink")
}
