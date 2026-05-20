package commands

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/spf13/cobra"
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
	s.cmd.AddCommand(mountCmd)
	s.buf = new(bytes.Buffer)
	s.cmd.SetOut(s.buf)
	s.cmd.SetErr(s.buf)
}

func (s *MountCmdTestSuite) TearDownTest() {
	volumeName = ""
	serverAddr = ""
	authType = "none"
	username = ""
	password = ""
	configFile = ""
	if s.tempDir != "" {
		err := os.RemoveAll(s.tempDir)
		s.Require().NoError(err)
	}
}

func (s *MountCmdTestSuite) TestMountCmd_NoVolumeName() {
	// Test
	s.cmd.SetArgs([]string{"mount", s.mountPath})
	err := s.cmd.Execute()

	// Verify
	s.Require().Error(err)
	s.Assert().Contains(err.Error(), "volume name is required")
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

	// Verify
	s.Require().Error(err)
	s.Assert().Contains(err.Error(), "username and password are required")
}

func (s *MountCmdTestSuite) TestMountCmd_NonExistentMountPoint() {
	nonExistentPath := filepath.Join(s.tempDir, "non-existent")

	// Test
	s.cmd.SetArgs([]string{
		"mount",
		nonExistentPath,
		"--server", "127.0.0.1:9449",
		"--volume", "test-volume",
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
  tls: false
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
  tls: false
auth:
  type: none
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
	s.Assert().Contains(err.Error(), "invalid server address: not-a-valid-endpoint")
}

func TestMountCmdSuite(t *testing.T) {
	suite.Run(t, new(MountCmdTestSuite))
}
