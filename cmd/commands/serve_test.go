//go:build linux

package commands

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"testing"

	commonConfig "go.gmountie.dev/gmountie/pkg/common/config"
	serverConfig "go.gmountie.dev/gmountie/pkg/server/config"
	"go.gmountie.dev/gmountie/test/e2e/utils"

	"github.com/spf13/cobra"
	"github.com/stretchr/testify/suite"
)

type ServeCmdTestSuite struct {
	suite.Suite
	cmd                 *cobra.Command
	buf                 *bytes.Buffer
	tempDir             string
	serverStartCalled   bool
	originalServerStart func(ctx context.Context, cfg *serverConfig.Config) error
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
	serverStart = func(ctx context.Context, cfg *serverConfig.Config) error {
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

	// Check if default config was created with the first-run template.
	defaultConfigPath := commonConfig.GetDefaultConfigPath(commonConfig.DefaultServerConfigFileName)
	written, err := os.ReadFile(defaultConfigPath)
	s.Require().NoError(err)
	s.Assert().Contains(string(written), "address: 0.0.0.0")
	s.Assert().Contains(string(written), "name: shared")

	// The generated password is printed once to the console.
	s.Assert().Contains(s.buf.String(), "Password:")

	// The default volume's data dir is created with 0700.
	volDir := filepath.Join(s.tempDir, ".local", "share", "gmountie", "shared")
	fi, err := os.Stat(volDir)
	s.Require().NoError(err)
	s.Assert().Equal(os.FileMode(0o700), fi.Mode().Perm())
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

func (s *ServeCmdTestSuite) TestFirstRunConfigIsUsable() {
	dataDir := s.T().TempDir()
	pw, cfgYAML, err := buildFirstRunConfig(dataDir)
	s.Require().NoError(err)

	s.NotEqual("admin", pw, "must not ship the fixed admin password")
	s.GreaterOrEqual(len(pw), 20)
	s.Contains(cfgYAML, "address: 0.0.0.0")
	s.Contains(cfgYAML, "name: shared")
	s.Contains(cfgYAML, dataDir)
	s.NotContains(cfgYAML, pw, "plaintext password must not be written to the file")

	cfg, err := serverConfig.LoadConfigFromString(cfgYAML)
	s.Require().NoError(err)
	s.Require().Len(cfg.Volumes, 1)
	s.Equal("shared", cfg.Volumes[0].Name)
}

func TestServeCmdSuite(t *testing.T) {
	suite.Run(t, new(ServeCmdTestSuite))
}
