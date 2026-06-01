package commands

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"

	"github.com/spf13/cobra"
	"github.com/stretchr/testify/suite"
)

type ProfileCmdSuite struct{ suite.Suite }

func TestProfileCmdSuite(t *testing.T) { suite.Run(t, new(ProfileCmdSuite)) }

func (s *ProfileCmdSuite) runList(cfgHome string) string {
	s.T().Setenv("XDG_CONFIG_HOME", cfgHome)
	root := &cobra.Command{Use: "root"}
	root.AddCommand(profileCmd)
	buf := new(bytes.Buffer)
	root.SetOut(buf)
	root.SetErr(buf)
	root.SetArgs([]string{"profile", "list"})
	s.Require().NoError(root.Execute())
	return buf.String()
}

func (s *ProfileCmdSuite) TestProfileList_Empty() {
	out := s.runList(s.T().TempDir())
	s.Contains(out, "No profiles")
}

func (s *ProfileCmdSuite) TestProfileList_ShowsNames() {
	cfgHome := s.T().TempDir()
	pdir := filepath.Join(cfgHome, "gmountie", "profiles")
	s.Require().NoError(os.MkdirAll(pdir, 0o755))
	s.Require().NoError(os.WriteFile(filepath.Join(pdir, "work.yaml"),
		[]byte("server:\n  address: work.example.com\n  port: 9449\nmount:\n  type: single\n  volume: shared\n"), 0o600))
	out := s.runList(cfgHome)
	s.Contains(out, "work")
	s.Contains(out, "work.example.com")
	s.Contains(out, "shared")
}
