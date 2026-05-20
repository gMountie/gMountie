package commands

import (
	"bytes"
	"testing"

	"github.com/spf13/cobra"
	"github.com/stretchr/testify/suite"
)

type VersionCmdTestSuite struct {
	suite.Suite
	cmd *cobra.Command
	buf *bytes.Buffer
}

func (s *VersionCmdTestSuite) SetupTest() {
	s.cmd = &cobra.Command{Use: "root"}
	s.cmd.AddCommand(versionCmd)
	s.buf = new(bytes.Buffer)
	s.cmd.SetOut(s.buf)
	s.cmd.SetErr(s.buf)
}

func (s *VersionCmdTestSuite) TestVersionCmd_Execute() {
	// Test
	s.cmd.SetArgs([]string{"version"})
	err := s.cmd.Execute()

	// Verify
	s.Require().NoError(err)
	output := s.buf.String()
	s.Assert().Contains(output, "Version")
	s.Assert().Contains(output, "Commit")
	s.Assert().Contains(output, "Build Date")
	// You might want to add more specific checks here, depending on what exactly your version command outputs
}

func TestVersionCmdSuite(t *testing.T) {
	suite.Run(t, new(VersionCmdTestSuite))
}
