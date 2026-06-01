package commands

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/spf13/cobra"
	"github.com/stretchr/testify/suite"
)

type ProfileFlagSuite struct{ suite.Suite }

func TestProfileFlagSuite(t *testing.T) { suite.Run(t, new(ProfileFlagSuite)) }

func (s *ProfileFlagSuite) reset() { profileName = ""; configFile = "" }

func (s *ProfileFlagSuite) TestResolveProfilePath_Unset() {
	s.reset()
	path, err := resolveProfilePath()
	s.Require().NoError(err)
	s.Empty(path)
}

func (s *ProfileFlagSuite) TestResolveProfilePath_ConflictWithConfig() {
	s.reset()
	profileName, configFile = "work", "/tmp/x.yaml"
	_, err := resolveProfilePath()
	s.Error(err)
}

func (s *ProfileFlagSuite) TestResolveProfilePath_Missing() {
	s.reset()
	dir := s.T().TempDir()
	s.T().Setenv("XDG_CONFIG_HOME", dir)
	profileName = "nope"
	_, err := resolveProfilePath()
	s.Require().Error(err)
	s.Contains(err.Error(), "not found")
}

func (s *ProfileFlagSuite) TestResolveProfilePath_Found() {
	s.reset()
	dir := s.T().TempDir()
	s.T().Setenv("XDG_CONFIG_HOME", dir)
	pdir := filepath.Join(dir, "gmountie", "profiles")
	s.Require().NoError(os.MkdirAll(pdir, 0o755))
	s.Require().NoError(os.WriteFile(filepath.Join(pdir, "work.yaml"), []byte("server:\n"), 0o600))
	profileName = "work"
	path, err := resolveProfilePath()
	s.Require().NoError(err)
	s.Equal(filepath.Join(pdir, "work.yaml"), path)
}

func (s *ProfileFlagSuite) TestAddProfileFlag() {
	cmd := &cobra.Command{Use: "x"}
	addProfileFlag(cmd)
	f := cmd.Flags().Lookup("profile")
	s.Require().NotNil(f)
	s.Equal("P", f.Shorthand)
}

func (s *ProfileFlagSuite) TestProfileNameCompletion() {
	s.reset()
	dir := s.T().TempDir()
	s.T().Setenv("XDG_CONFIG_HOME", dir)
	pdir := filepath.Join(dir, "gmountie", "profiles")
	s.Require().NoError(os.MkdirAll(pdir, 0o755))
	s.Require().NoError(os.WriteFile(filepath.Join(pdir, "work.yaml"), []byte("x"), 0o600))
	names, directive := profileNameCompletion(nil, nil, "")
	s.Equal([]string{"work"}, names)
	s.Equal(cobra.ShellCompDirectiveNoFileComp, directive)
}
