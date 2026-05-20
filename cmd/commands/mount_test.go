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

func TestMountCmdSuite(t *testing.T) {
	suite.Run(t, new(MountCmdTestSuite))
}
