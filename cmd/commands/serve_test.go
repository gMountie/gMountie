package commands

import (
	"bytes"
	"context"
	commonConfig "gmountie/pkg/common/config"
	"gmountie/pkg/server/config"
	"gmountie/test/e2e/utils"
	"os"
	"path/filepath"
	"testing"

	"github.com/spf13/cobra"
	"github.com/stretchr/testify/suite"
)

type ServeCmdTestSuite struct {
	suite.Suite
	cmd                 *cobra.Command
	buf                 *bytes.Buffer
	tempDir             string
	serverStartCalled   bool
	originalServerStart func(ctx context.Context, cfg *config.Config) error
}

func (s *ServeCmdTestSuite) SetupTest() {
	s.tempDir, _ = os.MkdirTemp("", "serveCmd_test")
	utils.Must0(s.T(), os.Setenv("HOME", s.tempDir))

	s.cmd = &cobra.Command{Use: "root"}
	s.cmd.AddCommand(serveCmd)
	s.buf = new(bytes.Buffer)
	s.cmd.SetOut(s.buf)
	s.cmd.SetErr(s.buf)

	s.serverStartCalled = false
	s.originalServerStart = serverStart
	serverStart = func(ctx context.Context, cfg *config.Config) error {
		s.serverStartCalled = true
		return nil
	}
}

func (s *ServeCmdTestSuite) TearDownTest() {
	serverStart = s.originalServerStart
	utils.Must0(s.T(), os.RemoveAll(s.tempDir))
}

func (s *ServeCmdTestSuite) TestServeCmd_ExecuteWithoutConfig() {
	// Test
	s.cmd.SetArgs([]string{"serve"})
	err := s.cmd.Execute()

	// Verify
	s.Require().NoError(err)
	s.Assert().True(s.serverStartCalled)

	// Check if default config was created
	defaultConfigPath := commonConfig.GetDefaultConfigPath(commonConfig.DefaultServerConfigFileName)
	_, err = os.Stat(defaultConfigPath)
	s.Assert().NoError(err)
}

func (s *ServeCmdTestSuite) TestServeCmd_ExecuteWithInvalidConfig() {
	// Setup
	configFile = filepath.Join(s.tempDir, ".config", "gmountie", "config.yaml")
	utils.Must0(s.T(), os.MkdirAll(filepath.Dir(configFile), 0755))
	utils.Must0(s.T(), os.WriteFile(configFile, []byte("test-config"), 0644))

	// Test
	s.cmd.SetArgs([]string{"serve"})
	err := s.cmd.Execute()

	// Verify
	s.Require().Error(err, "failed to parse config")
}

func TestServeCmdSuite(t *testing.T) {
	suite.Run(t, new(ServeCmdTestSuite))
}
