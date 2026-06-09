package commands

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"

	clientconfig "go.gmountie.dev/gmountie/pkg/client/config"
	commontls "go.gmountie.dev/gmountie/pkg/common/tls"
	servertls "go.gmountie.dev/gmountie/pkg/server/tls"

	"github.com/adrg/xdg"
	"github.com/go-playground/validator/v10"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"github.com/stretchr/testify/suite"
)

type FingerprintCmdSuite struct {
	suite.Suite
	root       *cobra.Command
	buf        *bytes.Buffer
	xdgRestore string
}

func TestFingerprintCmdSuite(t *testing.T) { suite.Run(t, new(FingerprintCmdSuite)) }

func (s *FingerprintCmdSuite) SetupTest() {
	viper.Reset()
	fingerprintVerbose = false

	s.xdgRestore = os.Getenv("XDG_STATE_HOME")
	dir := s.T().TempDir()
	s.Require().NoError(os.Setenv("XDG_STATE_HOME", dir))
	// xdg caches StateHome at package init; Reload re-reads the env so the
	// fresh tempdir actually takes effect for this test run.
	xdg.Reload()

	s.buf = new(bytes.Buffer)
	s.root = &cobra.Command{Use: "root"}
	s.root.AddCommand(fingerprintCmd)
	s.root.SetOut(s.buf)
	s.root.SetErr(s.buf)
}

func (s *FingerprintCmdSuite) TearDownTest() {
	_ = os.Setenv("XDG_STATE_HOME", s.xdgRestore)
	xdg.Reload()
	viper.Reset()
}

func (s *FingerprintCmdSuite) writeCert(host string) (certPath string, fingerprint string) {
	certPEM, _, err := servertls.Generate(host)
	s.Require().NoError(err)
	dir := s.T().TempDir()
	certPath = filepath.Join(dir, "server.crt")
	s.Require().NoError(os.WriteFile(certPath, certPEM, 0o644))
	fp, err := commontls.Fingerprint(certPEM)
	s.Require().NoError(err)
	return certPath, fp
}

func (s *FingerprintCmdSuite) runCmd(args ...string) (string, error) {
	allArgs := append([]string{"fingerprint"}, args...)
	s.root.SetArgs(allArgs)
	err := s.root.Execute()
	return s.buf.String(), err
}

func (s *FingerprintCmdSuite) TestPrintsFingerprintWhenCertExists() {
	certPath, fp := s.writeCert("test.example")
	viper.Set("server.tls.cert_file", certPath)

	out, err := s.runCmd()
	s.Require().NoError(err)
	s.Contains(out, fp)
}

func (s *FingerprintCmdSuite) TestVerboseIncludesSubjectAndDates() {
	certPath, _ := s.writeCert("verbose.example")
	viper.Set("server.tls.cert_file", certPath)

	out, err := s.runCmd("--verbose")
	s.Require().NoError(err)
	s.Contains(out, "Path:")
	s.Contains(out, "Subject:")
	s.Contains(out, "NotAfter:")
	s.Contains(out, "Fingerprint:")
}

func (s *FingerprintCmdSuite) TestMissingConfiguredCertReturnsError() {
	viper.Set("server.tls.cert_file", "/no/such/file.crt")
	_, err := s.runCmd()
	s.Require().Error(err)
	s.Contains(err.Error(), "set server.tls.cert_file to an existing file or run 'gmountie serve' to auto-generate")
}

func (s *FingerprintCmdSuite) TestMissingXdgCertReturnsError() {
	// XDG_STATE_HOME points at an empty tempdir; no config cert_file.
	_, err := s.runCmd()
	s.Require().Error(err)
	s.Contains(err.Error(), "run 'gmountie serve' once to auto-generate, or set server.tls.cert_file")
}

func (s *FingerprintCmdSuite) TestNonVerbosePrintsPasteSnippet() {
	out := renderFingerprint("SHA256:abc123")
	s.Contains(out, "SHA256:abc123")
	s.Contains(out, "expected_fingerprint: SHA256:abc123")
}

// TestSnippetIsValidClientTLS round-trips the pasted snippet through the real
// client TLS config so the emitted `verify:` value can't drift to something the
// validator rejects (e.g. `verify: true`, which is not in the oneof enum).
func (s *FingerprintCmdSuite) TestSnippetIsValidClientTLS() {
	out := renderFingerprint("SHA256:abc123")

	var verify, fp string
	for _, line := range strings.Split(out, "\n") {
		line = strings.TrimSpace(strings.TrimPrefix(strings.TrimSpace(line), "#"))
		switch {
		case strings.HasPrefix(line, "verify:"):
			verify = strings.TrimSpace(strings.TrimPrefix(line, "verify:"))
		case strings.HasPrefix(line, "expected_fingerprint:"):
			fp = strings.TrimSpace(strings.TrimPrefix(line, "expected_fingerprint:"))
		}
	}
	s.Require().NotEmpty(verify, "snippet must include a verify value")
	s.Equal("SHA256:abc123", fp)

	v := viper.New()
	v.Set("address", "127.0.0.1")
	v.Set("port", 9449)
	v.Set("tls.verify", verify)
	v.Set("tls.expected_fingerprint", fp)
	cfg, err := clientconfig.NewServerConfig(v)
	s.Require().NoError(err)
	s.Require().NoError(validator.New().Struct(cfg), "emitted snippet must pass client TLS validation")
}
