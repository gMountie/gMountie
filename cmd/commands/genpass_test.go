package commands

import (
	"bytes"
	"strings"
	"testing"

	"gmountie/pkg/common/passhash"

	"github.com/spf13/cobra"
	"github.com/stretchr/testify/suite"
)

type GenpassCmdSuite struct {
	suite.Suite
	root *cobra.Command
	out  *bytes.Buffer
	err  *bytes.Buffer
}

func TestGenpassCmdSuite(t *testing.T) { suite.Run(t, new(GenpassCmdSuite)) }

func (s *GenpassCmdSuite) SetupTest() {
	s.out = new(bytes.Buffer)
	s.err = new(bytes.Buffer)
	s.root = &cobra.Command{Use: "root"}
	s.root.AddCommand(genpassCmd)
	s.root.SetOut(s.out)
	s.root.SetErr(s.err)
}

func (s *GenpassCmdSuite) run(stdin string, args ...string) error {
	s.root.SetIn(strings.NewReader(stdin))
	s.root.SetArgs(append([]string{"genpass"}, args...))
	return s.root.Execute()
}

func (s *GenpassCmdSuite) TestRoundTripVerifies() {
	err := s.run("hunter2\nhunter2\n")
	s.Require().NoError(err)
	phc := strings.TrimSpace(s.out.String())
	s.True(strings.HasPrefix(phc, "$argon2id$"), "stdout must contain only the PHC; got %q", phc)
	ok, err := passhash.Verify(phc, "hunter2")
	s.Require().NoError(err)
	s.True(ok, "printed PHC must verify against the input password")
}

func (s *GenpassCmdSuite) TestMismatchedConfirmRejected() {
	err := s.run("hunter2\ndifferent\n")
	s.Require().Error(err)
	s.Contains(err.Error(), "passwords do not match")
}

func (s *GenpassCmdSuite) TestEmptyPasswordRejected() {
	err := s.run("\n\n")
	s.Require().Error(err)
	s.Contains(err.Error(), "password required")
}
