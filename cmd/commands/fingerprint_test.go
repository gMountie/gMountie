package commands

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"

	servertls "gmountie/pkg/server/tls"

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

	s.buf = new(bytes.Buffer)
	s.root = &cobra.Command{Use: "root"}
	s.root.AddCommand(fingerprintCmd)
	s.root.SetOut(s.buf)
	s.root.SetErr(s.buf)
}

func (s *FingerprintCmdSuite) TearDownTest() {
	_ = os.Setenv("XDG_STATE_HOME", s.xdgRestore)
	viper.Reset()
}

func (s *FingerprintCmdSuite) writeCert(host string) (certPath string, fingerprint string) {
	certPEM, _, err := servertls.Generate(host)
	s.Require().NoError(err)
	dir := s.T().TempDir()
	certPath = filepath.Join(dir, "server.crt")
	s.Require().NoError(os.WriteFile(certPath, certPEM, 0o644))
	fp, err := servertls.Fingerprint(certPEM)
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
