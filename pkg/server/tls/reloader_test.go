package tls

import (
	"crypto/tls"
	"crypto/x509"
	"os"
	"path/filepath"
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
