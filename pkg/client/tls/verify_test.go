package tls

import (
	"crypto/tls"
	"encoding/pem"
	"os"
	"path/filepath"
	"testing"

	servertls "go.gmountie.dev/gmountie/pkg/server/tls"

	"github.com/stretchr/testify/suite"
)

// writeTempKeypair writes certPEM and keyPEM to temp files in dir and returns
// the cert path and key path.
func writeTempKeypair(t interface {
	Helper()
	TempDir() string
	Fatalf(format string, args ...interface{})
}, certPEM, keyPEM []byte) (certPath, keyPath string) {
	t.Helper()
	dir := t.TempDir()
	certPath = filepath.Join(dir, "client.crt")
	keyPath = filepath.Join(dir, "client.key")
	if err := os.WriteFile(certPath, certPEM, 0o600); err != nil {
		t.Fatalf("write cert: %v", err)
	}
	if err := os.WriteFile(keyPath, keyPEM, 0o600); err != nil {
		t.Fatalf("write key: %v", err)
	}
	return certPath, keyPath
}

// VerifyTestSuite tests BuildConfig and pinVerifier using forged rawCerts.
// No real network connection is needed — we call VerifyPeerCertificate
// directly with DER bytes produced by servertls.Generate.
type VerifyTestSuite struct {
	suite.Suite

	// certDER is a DER-encoded self-signed cert generated once per suite run.
	certDER []byte
	// certFP is the SHA256 fingerprint of certDER.
	certFP string
	// altCertDER is a second cert whose fingerprint differs from certFP.
	altCertDER []byte
}

func (s *VerifyTestSuite) SetupSuite() {
	certPEM, _, err := servertls.Generate("testhost")
	s.Require().NoError(err)
	block, _ := pem.Decode(certPEM)
	s.Require().NotNil(block)
	s.certDER = block.Bytes

	fp, err := servertls.Fingerprint(certPEM)
	s.Require().NoError(err)
	s.certFP = fp

	altPEM, _, err := servertls.Generate("althost")
	s.Require().NoError(err)
	altBlock, _ := pem.Decode(altPEM)
	s.Require().NotNil(altBlock)
	s.altCertDER = altBlock.Bytes
}

// TestVerifyModeNoFingerprintNoCallback — verify mode without a fingerprint
// must not install a VerifyPeerCertificate callback and must not skip chain
// validation.
func (s *VerifyTestSuite) TestVerifyModeNoFingerprintNoCallback() {
	cfg, err := BuildConfig(Config{Mode: ModeVerify, Endpoint: "x:1"})
	s.Require().NoError(err)
	s.Nil(cfg.VerifyPeerCertificate)
	s.False(cfg.InsecureSkipVerify)
}

// TestVerifyModeWithFingerprintInstallsCallback — when ExpectedFingerprint is
// set in verify mode the callback is installed and enforces the pin.
func (s *VerifyTestSuite) TestVerifyModeWithFingerprintInstallsCallback() {
	cfg, err := BuildConfig(Config{
		Mode:                ModeVerify,
		Endpoint:            "x:1",
		ExpectedFingerprint: s.certFP,
	})
	s.Require().NoError(err)
	s.NotNil(cfg.VerifyPeerCertificate)

	// Correct cert → no error.
	err = cfg.VerifyPeerCertificate([][]byte{s.certDER}, nil)
	s.Require().NoError(err)

	// Wrong cert → error containing "fingerprint changed".
	err = cfg.VerifyPeerCertificate([][]byte{s.altCertDER}, nil)
	s.Require().Error(err)
	s.Contains(err.Error(), "fingerprint changed")
}

// TestTofuModeFirstConnectPins — TOFU with no prior pin: first call pins,
// second call with same cert succeeds, call with different cert fails.
func (s *VerifyTestSuite) TestTofuModeFirstConnectPins() {
	dir := s.T().TempDir()
	khPath := filepath.Join(dir, "known_hosts")

	cfg, err := BuildConfig(Config{
		Mode:           ModeTOFU,
		Endpoint:       "server.example:9449",
		KnownHostsPath: khPath,
	})
	s.Require().NoError(err)
	s.True(cfg.InsecureSkipVerify, "TOFU sets InsecureSkipVerify; chain check replaced by pin")
	s.NotNil(cfg.VerifyPeerCertificate)

	// First call: pins the cert.
	err = cfg.VerifyPeerCertificate([][]byte{s.certDER}, nil)
	s.Require().NoError(err)

	// Confirm the known_hosts file has the line.
	raw, readErr := os.ReadFile(khPath)
	s.Require().NoError(readErr)
	s.Contains(string(raw), "server.example:9449")
	s.Contains(string(raw), s.certFP)

	// Second call with same cert: idempotent.
	err = cfg.VerifyPeerCertificate([][]byte{s.certDER}, nil)
	s.Require().NoError(err)

	// Call with different cert: fingerprint changed → error.
	err = cfg.VerifyPeerCertificate([][]byte{s.altCertDER}, nil)
	s.Require().Error(err)
	s.Contains(err.Error(), "fingerprint changed")
}

// TestExpectedFingerprintPin_RoundTrip — TOFU with ExpectedFingerprint
// pre-set: correct cert succeeds, wrong cert fails even on first call.
func (s *VerifyTestSuite) TestExpectedFingerprintPin_RoundTrip() {
	cfg, err := BuildConfig(Config{
		Mode:                ModeTOFU,
		Endpoint:            "pinned.example:9449",
		ExpectedFingerprint: s.certFP,
	})
	s.Require().NoError(err)
	s.NotNil(cfg.VerifyPeerCertificate)

	// Correct cert → no error.
	err = cfg.VerifyPeerCertificate([][]byte{s.certDER}, nil)
	s.Require().NoError(err)

	// Wrong cert → error.
	err = cfg.VerifyPeerCertificate([][]byte{s.altCertDER}, nil)
	s.Require().Error(err)
	s.Contains(err.Error(), "fingerprint changed")
}

// TestInsecureMode — InsecureSkipVerify true, no callback.
func (s *VerifyTestSuite) TestInsecureMode() {
	cfg, err := BuildConfig(Config{Mode: ModeInsecure, Endpoint: "x:1"})
	s.Require().NoError(err)
	s.True(cfg.InsecureSkipVerify)
	s.Nil(cfg.VerifyPeerCertificate)
}

// TestUnknownModeRejected — unknown mode returns an error, no config.
func (s *VerifyTestSuite) TestUnknownModeRejected() {
	cfg, err := BuildConfig(Config{Mode: "yolo", Endpoint: "x:1"})
	s.Require().Error(err)
	s.Nil(cfg)
}

// TestEmptyModeDefaultsToVerify — empty Mode is treated as "verify".
func (s *VerifyTestSuite) TestEmptyModeDefaultsToVerify() {
	cfg, err := BuildConfig(Config{Endpoint: "x:1"})
	s.Require().NoError(err)
	s.False(cfg.InsecureSkipVerify)
	s.Nil(cfg.VerifyPeerCertificate)
}

// TestTLSVersionIsAtLeast13 — all modes enforce TLS 1.3 minimum.
func (s *VerifyTestSuite) TestTLSVersionIsAtLeast13() {
	for _, mode := range []string{ModeVerify, ModeTOFU, ModeInsecure} {
		cfg, err := BuildConfig(Config{Mode: mode, Endpoint: "x:1"})
		s.Require().NoError(err)
		s.Equal(uint16(tls.VersionTLS13), cfg.MinVersion, "mode=%s", mode)
	}
}

// TestClientCertPresentedWhenConfigured — when CertFile and KeyFile are set,
// BuildConfig populates Certificates with exactly one entry.
func (s *VerifyTestSuite) TestClientCertPresentedWhenConfigured() {
	certPEM, keyPEM, err := servertls.Generate("client.example")
	s.Require().NoError(err)

	certPath, keyPath := writeTempKeypair(s.T(), certPEM, keyPEM)

	cfg, err := BuildConfig(Config{Mode: ModeInsecure, Endpoint: "x:1", CertFile: certPath, KeyFile: keyPath})
	s.Require().NoError(err)
	s.Len(cfg.Certificates, 1, "expected exactly one client certificate")
}

// TestNoClientCertByDefault — when CertFile/KeyFile are not set, Certificates
// must be empty.
func (s *VerifyTestSuite) TestNoClientCertByDefault() {
	cfg, err := BuildConfig(Config{Mode: ModeInsecure, Endpoint: "x:1"})
	s.Require().NoError(err)
	s.Empty(cfg.Certificates)
}

// TestClientCertRequiresBothPaths — supplying only CertFile (no KeyFile) must
// return an error that mentions "both cert_file and key_file".
func (s *VerifyTestSuite) TestClientCertRequiresBothPaths() {
	certPEM, _, err := servertls.Generate("client.example")
	s.Require().NoError(err)

	dir := s.T().TempDir()
	certPath := filepath.Join(dir, "client.crt")
	s.Require().NoError(os.WriteFile(certPath, certPEM, 0o600))

	cfg, err := BuildConfig(Config{Mode: ModeInsecure, Endpoint: "x:1", CertFile: certPath})
	s.Require().Error(err)
	s.Contains(err.Error(), "both cert_file and key_file")
	s.Nil(cfg)
}

// TestClientCertComposesWithTOFU — client cert population is orthogonal to the
// server-verification mode; verify it works with TOFU too.
func (s *VerifyTestSuite) TestClientCertComposesWithTOFU() {
	certPEM, keyPEM, err := servertls.Generate("client.example")
	s.Require().NoError(err)

	certPath, keyPath := writeTempKeypair(s.T(), certPEM, keyPEM)

	khPath := filepath.Join(s.T().TempDir(), "known_hosts")
	cfg, err := BuildConfig(Config{
		Mode:           ModeTOFU,
		Endpoint:       "server.example:9449",
		KnownHostsPath: khPath,
		CertFile:       certPath,
		KeyFile:        keyPath,
	})
	s.Require().NoError(err)
	s.True(cfg.InsecureSkipVerify, "TOFU sets InsecureSkipVerify")
	s.NotNil(cfg.VerifyPeerCertificate, "TOFU installs VerifyPeerCertificate callback")
	s.Len(cfg.Certificates, 1, "client cert must be present alongside TOFU")
}

func TestVerifyTestSuite(t *testing.T) {
	suite.Run(t, new(VerifyTestSuite))
}
