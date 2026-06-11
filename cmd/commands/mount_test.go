//go:build linux

package commands

import (
	"bytes"
	"encoding/base64"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"go.gmountie.dev/gmountie/pkg/client/credentials"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"github.com/stretchr/testify/suite"
)

type MountCmdTestSuite struct {
	suite.Suite
	cmd       *cobra.Command
	buf       *bytes.Buffer
	tempDir   string
	mountPath string
}

func (s *MountCmdTestSuite) SetupTest() {
	var err error
	s.tempDir, err = os.MkdirTemp("", "mountCmd_test")
	s.Require().NoError(err)

	// Create a mount point directory
	s.mountPath = filepath.Join(s.tempDir, "mountpoint")
	err = os.MkdirAll(s.mountPath, 0755)
	s.Require().NoError(err)

	s.cmd = &cobra.Command{Use: "root"}
	// Re-declare the persistent --config flag on the test root so
	// tests can pass -c without depending on the real rootCmd's init.
	s.cmd.PersistentFlags().StringVarP(&configFile, "config", "c", "", "config file path")
	// A FRESH mount command per test (closure-pattern flag struct): no
	// shared flag state survives between tests, so nothing needs resetting.
	s.cmd.AddCommand(newMountCmd())
	s.buf = new(bytes.Buffer)
	s.cmd.SetOut(s.buf)
	s.cmd.SetErr(s.buf)
	// Empty stdin so any accidental password prompt returns EOF (an error)
	// rather than blocking on a developer's terminal.
	s.cmd.SetIn(bytes.NewReader(nil))
}

func (s *MountCmdTestSuite) TearDownTest() {
	// The mount flags themselves are per-command (newMountCmd); only the
	// genuinely shared globals (root --config, --profile, --credentials)
	// still need resetting.
	configFile = ""
	profileName = ""
	credentialsFile = ""
	if s.tempDir != "" {
		err := os.RemoveAll(s.tempDir)
		s.Require().NoError(err)
	}
}

func (s *MountCmdTestSuite) TestMountCmd_NoVolumeName() {
	// An omitted --volume is no longer a hard error: the empty volume now flows
	// into NewClientForVolume, which lists the caller's volumes to auto-select
	// the sole one. With no server reachable, that List call fails — proving the
	// empty volume reached the resolver path instead of the old "required" guard.
	s.cmd.SetArgs([]string{"mount", s.mountPath, "--username", "admin", "--password", "admin"})
	err := s.cmd.Execute()

	// Verify
	s.Require().Error(err)
	s.Assert().NotContains(err.Error(), "volume name is required")
	s.Assert().Contains(err.Error(), "list volumes for this credential")
}

func (s *MountCmdTestSuite) TestMountCmd_InvalidServerAddress() {
	// Test
	s.cmd.SetArgs([]string{
		"mount",
		s.mountPath,
		"--volume", "test-volume",
		"--server", "invalid-address",
	})
	err := s.cmd.Execute()

	// Verify
	s.Require().Error(err)
	s.Assert().Contains(err.Error(), "invalid server address")
}

func (s *MountCmdTestSuite) TestMountCmd_BasicAuthMissingCredentials() {
	// Test
	s.cmd.SetArgs([]string{
		"mount",
		s.mountPath,
		"--volume", "test-volume",
		"--auth-type", "basic",
	})
	err := s.cmd.Execute()

	// Verify: with no username supplied, the username check trips first
	// (the password is resolved only after a username is present).
	s.Require().Error(err)
	s.Assert().Contains(err.Error(), "username is required for basic auth")
}

// TestMountCmd_ShorthandSpec proves the sshfs-style positional form
// "[user@]host[:port]/volume mountpoint" populates server/username/volume,
// and that the password is taken from $GMOUNTIE_AUTH_PASSWORD (no prompt).
// We point at a non-existent mountpoint so the command stops before
// networking — reaching that guard proves parsing/auth wiring succeeded.
func (s *MountCmdTestSuite) TestMountCmd_ShorthandSpec() {
	s.T().Setenv("GMOUNTIE_AUTH_PASSWORD", "secret")
	missing := filepath.Join(s.tempDir, "no-such-mount")
	s.cmd.SetArgs([]string{"mount", "admin@192.168.11.11:9449/shared", missing})
	err := s.cmd.Execute()
	s.Require().Error(err)
	s.Assert().Contains(err.Error(), fmt.Sprintf("mountpoint %s does not exist", missing))
	s.Assert().NotContains(err.Error(), "volume name is required")
	s.Assert().NotContains(err.Error(), "username is required")
}

// TestMountCmd_ConfigFile_DefaultsAuthType proves a client.yaml that omits
// auth.type still authenticates as basic (matching `gmountie ls`) rather than
// failing with "invalid auth type". Password comes from the env; we stop at the
// non-existent mountpoint, which means auth resolution succeeded.
func (s *MountCmdTestSuite) TestMountCmd_ConfigFile_DefaultsAuthType() {
	s.T().Setenv("GMOUNTIE_AUTH_PASSWORD", "secret")
	cfgPath := filepath.Join(s.tempDir, "client.yaml")
	cfgYAML := `server:
  address: 192.168.11.11
  port: 9449
auth:
  username: demo
`
	s.Require().NoError(os.WriteFile(cfgPath, []byte(cfgYAML), 0o600))
	missingMount := filepath.Join(s.tempDir, "no-such-mount")
	s.cmd.SetArgs([]string{"mount", missingMount, "--volume", "test-volume", "--config", cfgPath})
	err := s.cmd.Execute()
	s.Require().Error(err)
	s.Assert().Contains(err.Error(), fmt.Sprintf("mountpoint %s does not exist", missingMount))
	s.Assert().NotContains(err.Error(), "invalid auth type")
}

func (s *MountCmdTestSuite) TestApplyMountSpecPopulatesFlags() {
	v := viper.New()
	spec := mountSpec{Username: "admin", Host: "10.0.0.5", Port: 9449, Volume: "shared"}
	gotVol := applyMountSpec(v, spec)
	s.Equal("10.0.0.5", v.GetString("server.address"))
	s.Equal("9449", v.GetString("server.port"))
	s.Equal("admin", v.GetString("auth.username"))
	s.Equal("shared", gotVol)
}

// credBlob builds a base64-STD-encoded credential blob for the mount-wiring
// tests. The PEM material is opaque here: the command stops at the
// mountpoint-existence guard (before any TLS handshake), so reaching that guard
// proves the credential populated server/TLS/auth and that cert-only auth
// skipped the username requirement.
func credBlob(host, port string) string {
	json := fmt.Sprintf(
		`{"cert_pem":"CERT","key_pem":"KEY","ca_pem":"CA","endpoint":"%s:%s","server_name":"%s"}`,
		host, port, host)
	return base64.StdEncoding.EncodeToString([]byte(json))
}

// TestApplyCredentialPopulatesViper proves the credential maps onto the viper
// keys ParseConfig reads: endpoint → server.address/port, inline TLS material,
// server_name, verify defaulted to "verify", and auth.type=mtls.
func (s *MountCmdTestSuite) TestApplyCredentialPopulatesViper() {
	v := viper.New()
	cred, err := credentials.Decode(credBlob("data.example.com", "443"))
	s.Require().NoError(err)
	s.Require().NoError(applyCredential(v, cred))
	s.Equal("data.example.com", v.GetString("server.address"))
	s.Equal("443", v.GetString("server.port"))
	s.Equal("data.example.com", v.GetString("server.tls.server_name"))
	s.Equal("CA", v.GetString("server.tls.ca_pem"))
	s.Equal("CERT", v.GetString("server.tls.cert_pem"))
	s.Equal("KEY", v.GetString("server.tls.key_pem"))
	s.Equal("verify", v.GetString("server.tls.verify"))
	s.Equal("mtls", v.GetString("auth.type"))
}

// TestApplyCredentialTokenModePopulatesViper proves that a token-mode credential
// maps renew_endpoint and renew_token onto viper and does NOT set any static TLS
// material — the cert/key/CA arrive later via the refresher's profile exchange.
func (s *MountCmdTestSuite) TestApplyCredentialTokenModePopulatesViper() {
	v := viper.New()
	tokenBlob := base64.StdEncoding.EncodeToString([]byte(
		`{"endpoint":"mount.example:443","renew_endpoint":"https://cp.example/v1/certs","renew_token":"gmpat_x"}`))
	cred, err := credentials.Decode(tokenBlob)
	s.Require().NoError(err)
	s.Require().NoError(applyCredential(v, cred))
	s.Equal("https://cp.example/v1/certs", v.GetString("renew.endpoint"))
	s.Equal("gmpat_x", v.GetString("renew.token"))
	// Static TLS material must NOT be set in token mode.
	s.Empty(v.GetString("server.tls.cert_pem"))
	s.Empty(v.GetString("server.tls.key_pem"))
	s.Empty(v.GetString("server.tls.ca_pem"))
	// Common fields still apply.
	s.Equal("mount.example", v.GetString("server.address"))
	s.Equal("443", v.GetString("server.port"))
	s.Equal("verify", v.GetString("server.tls.verify"))
	s.Equal("mtls", v.GetString("auth.type"))
}

// TestApplyCredentialStaticModeUnchanged proves that a static credential still
// populates cert/key/CA (regression guard — token mode must not disturb the
// existing static path).
func (s *MountCmdTestSuite) TestApplyCredentialStaticModeUnchanged() {
	v := viper.New()
	cred, err := credentials.Decode(credBlob("data.example.com", "443"))
	s.Require().NoError(err)
	s.Require().NoError(applyCredential(v, cred))
	s.Equal("CA", v.GetString("server.tls.ca_pem"))
	s.Equal("CERT", v.GetString("server.tls.cert_pem"))
	s.Equal("KEY", v.GetString("server.tls.key_pem"))
	// Token fields must NOT be set in static mode.
	s.Empty(v.GetString("renew.endpoint"))
	s.Empty(v.GetString("renew.token"))
}

// TestMountCmd_CredentialEnv_CertOnlyAuth proves the target invocation:
// GMOUNTIE_CREDENTIALS=<blob> gmountie mount <mp> -n <vol>, with no -u/-p and no
// config file, authenticates by cert alone (auth.type=mtls, no username
// required) and the credential's endpoint is NOT clobbered by the -s default.
// We stop at the non-existent mountpoint guard.
func (s *MountCmdTestSuite) TestMountCmd_CredentialEnv_CertOnlyAuth() {
	s.T().Setenv(credentialsEnvVar, credBlob("192.168.11.11", "9449"))
	missing := filepath.Join(s.tempDir, "no-such-mount")
	s.cmd.SetArgs([]string{"mount", missing, "--volume", "test-volume"})
	err := s.cmd.Execute()
	s.Require().Error(err)
	s.Assert().Contains(err.Error(), fmt.Sprintf("mountpoint %s does not exist", missing))
	// No basic-auth requirement (cert is the identity) and no auth-type error.
	s.Assert().NotContains(err.Error(), "username is required")
	s.Assert().NotContains(err.Error(), "invalid auth type")
}

// TestMountCmd_CredentialFromFile proves the --credentials file flag works the
// same way and wins over $GMOUNTIE_CREDENTIALS (file source takes precedence).
func (s *MountCmdTestSuite) TestMountCmd_CredentialFromFile() {
	s.T().Setenv(credentialsEnvVar, credBlob("10.0.0.1", "1234"))
	credPath := filepath.Join(s.tempDir, "cred.b64")
	s.Require().NoError(os.WriteFile(credPath, []byte(credBlob("192.168.11.11", "9449")), 0o600))
	missing := filepath.Join(s.tempDir, "no-such-mount")
	s.cmd.SetArgs([]string{"mount", missing, "--volume", "test-volume", "--credentials", credPath})
	err := s.cmd.Execute()
	s.Require().Error(err)
	s.Assert().Contains(err.Error(), fmt.Sprintf("mountpoint %s does not exist", missing))
	s.Assert().NotContains(err.Error(), "username is required")
}

// TestMountCmd_ExplicitServerOverridesCredential proves an explicit -s still
// wins over the credential's endpoint (precedence: flag > credential).
func (s *MountCmdTestSuite) TestMountCmd_ExplicitServerOverridesCredential() {
	s.T().Setenv(credentialsEnvVar, credBlob("192.168.11.11", "9449"))
	s.cmd.SetArgs([]string{
		"mount",
		s.mountPath,
		"--volume", "test-volume",
		"--server", "not-a-valid-endpoint", // no colon → split fails, proving -s ran
	})
	err := s.cmd.Execute()
	s.Require().Error(err)
	s.Assert().Contains(err.Error(), "invalid server address")
}

func (s *MountCmdTestSuite) TestMountCmd_NonExistentMountPoint() {
	nonExistentPath := filepath.Join(s.tempDir, "non-existent")

	// Test
	s.cmd.SetArgs([]string{
		"mount",
		nonExistentPath,
		"--server", "127.0.0.1:9449",
		"--volume", "test-volume",
		"--auth-type", "basic",
		"--username", "admin",
		"--password", "admin",
	})
	err := s.cmd.Execute()

	// Verify
	s.Require().Error(err)
	s.Assert().Contains(err.Error(), fmt.Sprintf("mountpoint %s does not exist", nonExistentPath))
}

func (s *MountCmdTestSuite) TestMountCmd_ConfigFile_NotFound() {
	missing := filepath.Join(s.tempDir, "no-such-config.yaml")
	s.cmd.SetArgs([]string{
		"mount",
		s.mountPath,
		"--volume", "test-volume",
		"--config", missing,
	})
	err := s.cmd.Execute()
	s.Require().Error(err)
	s.Assert().Contains(err.Error(), "failed to read config file")
	s.Assert().Contains(err.Error(), missing)
}

// TestMountCmd_ConfigFile_BasicAuthFromFile proves that basic-auth
// credentials supplied via -c (with no --username / --password flags)
// pass the validation check that previously only honored CLI flags.
// We stop the test before networking by pointing at a non-existent
// mountpoint, which trips the final guard after config parsing.
func (s *MountCmdTestSuite) TestMountCmd_ConfigFile_BasicAuthFromFile() {
	cfgPath := filepath.Join(s.tempDir, "client.yaml")
	cfgYAML := `server:
  address: 192.168.11.11
  port: 9449
auth:
  type: basic
  username: demo
  password: demo
`
	s.Require().NoError(os.WriteFile(cfgPath, []byte(cfgYAML), 0o600))
	missingMount := filepath.Join(s.tempDir, "no-such-mount")
	s.cmd.SetArgs([]string{
		"mount",
		missingMount,
		"--volume", "test-volume",
		"--config", cfgPath,
	})
	err := s.cmd.Execute()
	s.Require().Error(err)
	// The credentials check (which previously failed) is past; we now
	// fail on the mountpoint-existence check at the next step.
	s.Assert().Contains(err.Error(), fmt.Sprintf("mountpoint %s does not exist", missingMount))
	s.Assert().NotContains(err.Error(), "username and password are required")
}

// TestMountCmd_FlagOverridesConfig confirms that an explicit -s flag
// wins over a config file's server.address. We use an invalid
// address shape ("not-a-valid-endpoint") so the override is detected
// — the error must mention the flag value, not the file value.
func (s *MountCmdTestSuite) TestMountCmd_FlagOverridesConfig() {
	cfgPath := filepath.Join(s.tempDir, "client.yaml")
	cfgYAML := `server:
  address: 192.168.11.11
  port: 9449
auth:
  type: basic
  username: admin
  password: admin
`
	s.Require().NoError(os.WriteFile(cfgPath, []byte(cfgYAML), 0o600))
	s.cmd.SetArgs([]string{
		"mount",
		s.mountPath,
		"--volume", "test-volume",
		"--config", cfgPath,
		"--server", "not-a-valid-endpoint", // no colon → split fails
	})
	err := s.cmd.Execute()
	s.Require().Error(err)
	s.Assert().Contains(err.Error(), "invalid server address")
}

// TestWaitForUnmount_ReturnsOnServerExit proves the mount loop wakes when the
// FUSE server exits out-of-band (e.g. an external `fusermount -u`), reporting
// the unmount was external — the property that stops a daemon mount lingering.
func (s *MountCmdTestSuite) TestWaitForUnmount_ReturnsOnServerExit() {
	sig := make(chan os.Signal, 1)
	served := make(chan struct{})
	close(served)
	s.True(waitForUnmount(sig, served))
}

// TestWaitForUnmount_ReturnsOnSignal proves an interrupt/SIGTERM still wakes
// the loop and is reported as a local (non-external) unmount.
func (s *MountCmdTestSuite) TestWaitForUnmount_ReturnsOnSignal() {
	sig := make(chan os.Signal, 1)
	served := make(chan struct{})
	sig <- os.Interrupt
	s.False(waitForUnmount(sig, served))
}

// TestMountCmd_ProfileSuppliesVolume proves a profile can supply server + volume
// so only the mountpoint is needed on the CLI. We stop at the non-existent
// mountpoint guard, which proves the profile resolved the volume (no "volume
// name is required") and the password (no prompt/EOF error).
func (s *MountCmdTestSuite) TestMountCmd_ProfileSuppliesVolume() {
	cfgHome := filepath.Join(s.tempDir, "xdgcfg")
	s.T().Setenv("XDG_CONFIG_HOME", cfgHome)
	pdir := filepath.Join(cfgHome, "gmountie", "profiles")
	s.Require().NoError(os.MkdirAll(pdir, 0o755))
	profile := `server:
  address: 192.168.11.11
  port: 9449
auth:
  type: basic
  username: demo
  password: demo
mount:
  type: single
  volume: shared
`
	s.Require().NoError(os.WriteFile(filepath.Join(pdir, "work.yaml"), []byte(profile), 0o600))

	missing := filepath.Join(s.tempDir, "no-such-mount")
	s.cmd.SetArgs([]string{"mount", missing, "--profile", "work"})
	err := s.cmd.Execute()
	s.Require().Error(err)
	s.Assert().Contains(err.Error(), fmt.Sprintf("mountpoint %s does not exist", missing))
	s.Assert().NotContains(err.Error(), "volume name is required")
}

// TestMountCmd_ProfileAndConfigConflict proves that --profile and --config
// together produce a clear error.
func (s *MountCmdTestSuite) TestMountCmd_ProfileAndConfigConflict() {
	s.cmd.SetArgs([]string{"mount", s.mountPath, "--profile", "work", "--config", "/tmp/x.yaml"})
	err := s.cmd.Execute()
	s.Require().Error(err)
	s.Assert().Contains(err.Error(), "one of --profile or --config")
}

func TestMountCmdSuite(t *testing.T) {
	suite.Run(t, new(MountCmdTestSuite))
}
