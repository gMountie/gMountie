package config

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/suite"
)

const (
	defaultConfigFileName = "config.yaml"
)

type PathsTestSuite struct {
	suite.Suite
	tempDir string
}

func (s *PathsTestSuite) SetupTest() {
	var err error
	s.tempDir, err = os.MkdirTemp("", "paths_test")
	s.Require().NoError(err)
}

func (s *PathsTestSuite) TearDownTest() {
	os.RemoveAll(s.tempDir)
}

func (s *PathsTestSuite) TestGetDefaultConfigDir_WithXDG() {
	// Setup
	s.T().Setenv("XDG_CONFIG_HOME", s.tempDir)

	// Test
	result := GetDefaultConfigDir()

	// Verify
	expected := filepath.Join(s.tempDir, DefaultConfigDirName)
	s.Assert().Equal(expected, result)
}

func (s *PathsTestSuite) TestGetDefaultConfigDir_WithoutXDG() {
	// Setup
	s.T().Setenv("XDG_CONFIG_HOME", "")
	s.T().Setenv("HOME", s.tempDir)

	// Test
	result := GetDefaultConfigDir()

	// Verify
	expected := filepath.Join(s.tempDir, ".config", DefaultConfigDirName)
	s.Assert().Equal(expected, result)
}

func (s *PathsTestSuite) TestGetDefaultConfigPath() {
	// Setup
	s.T().Setenv("XDG_CONFIG_HOME", s.tempDir)

	// Test
	result := GetDefaultConfigPath(defaultConfigFileName)

	// Verify
	expected := filepath.Join(s.tempDir, DefaultConfigDirName, defaultConfigFileName)
	s.Assert().Equal(expected, result)
}

func (s *PathsTestSuite) TestEnsureConfigDir() {
	// Setup
	s.T().Setenv("XDG_CONFIG_HOME", s.tempDir)
	configDir := GetDefaultConfigDir()

	// Test
	err := EnsureConfigDir()

	// Verify
	s.Require().NoError(err)
	info, err := os.Stat(configDir)
	s.Require().NoError(err)
	s.Assert().True(info.IsDir())
}

func (s *PathsTestSuite) TestEnsureConfigDir_AlreadyExists() {
	// Setup
	s.T().Setenv("XDG_CONFIG_HOME", s.tempDir)
	configDir := GetDefaultConfigDir()
	err := os.MkdirAll(configDir, 0755)
	s.Require().NoError(err)

	// Test
	err = EnsureConfigDir()

	// Verify
	s.Require().NoError(err)
	info, err := os.Stat(configDir)
	s.Require().NoError(err)
	s.Assert().True(info.IsDir())
}

func (s *PathsTestSuite) TestWriteDefaultConfig() {
	// Setup
	s.T().Setenv("XDG_CONFIG_HOME", s.tempDir)
	err := EnsureConfigDir()
	s.Require().NoError(err)
	cfg := "test"
	// Test
	err = WriteDefaultConfig(defaultConfigFileName, cfg)

	// Verify
	s.Require().NoError(err)
	configPath := GetDefaultConfigPath(defaultConfigFileName)
	content, err := os.ReadFile(configPath)
	s.Require().NoError(err)
	s.Assert().Equal(cfg, string(content))
}

func (s *PathsTestSuite) TestWriteDefaultConfig_OverwriteExisting() {
	// Setup
	s.T().Setenv("XDG_CONFIG_HOME", s.tempDir)
	err := EnsureConfigDir()
	s.Require().NoError(err)
	configPath := GetDefaultConfigPath(defaultConfigFileName)
	err = os.WriteFile(configPath, []byte("old content"), 0644)
	s.Require().NoError(err)
	cfg := "test"

	// Test
	err = WriteDefaultConfig(defaultConfigFileName, cfg)

	// Verify
	s.Require().NoError(err)
	content, err := os.ReadFile(configPath)
	s.Require().NoError(err)
	s.Assert().Equal(cfg, string(content))
}

func TestPathsTestSuite(t *testing.T) {
	suite.Run(t, new(PathsTestSuite))
}

type ProfilePathsSuite struct{ suite.Suite }

func TestProfilePathsSuite(t *testing.T) { suite.Run(t, new(ProfilePathsSuite)) }

func (s *ProfilePathsSuite) TestValidateProfileName() {
	for _, ok := range []string{"work", "home-nas", "a.b_c", "Srv1"} {
		s.Require().NoErrorf(ValidateProfileName(ok), "expected %q valid", ok)
	}
	for _, bad := range []string{"", "../x", "a/b", "a b", "a\tb", ".", ".."} {
		s.Require().Errorf(ValidateProfileName(bad), "expected %q invalid", bad)
	}
}

func (s *ProfilePathsSuite) TestGetProfilePathAndList() {
	dir := s.T().TempDir()
	s.T().Setenv("XDG_CONFIG_HOME", dir)

	names, err := ListProfileNames()
	s.Require().NoError(err)
	s.Empty(names)

	pdir := GetProfilesDir()
	s.Equal(filepath.Join(dir, DefaultConfigDirName, "profiles"), pdir)
	s.Require().NoError(os.MkdirAll(pdir, 0o755))
	s.Require().NoError(os.WriteFile(filepath.Join(pdir, "work.yaml"), []byte("server:\n"), 0o600))
	s.Require().NoError(os.WriteFile(filepath.Join(pdir, "home.yaml"), []byte("server:\n"), 0o600))
	s.Require().NoError(os.WriteFile(filepath.Join(pdir, "notes.txt"), []byte("ignore"), 0o600))

	s.Equal(filepath.Join(pdir, "work.yaml"), GetProfilePath("work"))

	names, err = ListProfileNames()
	s.Require().NoError(err)
	s.Equal([]string{"home", "work"}, names)
}
