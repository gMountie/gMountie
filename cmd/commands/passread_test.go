package commands

import (
	"bytes"
	"strings"
	"testing"

	"github.com/stretchr/testify/suite"
)

type PassReadSuite struct{ suite.Suite }

func TestPassReadSuite(t *testing.T) { suite.Run(t, new(PassReadSuite)) }

func (s *PassReadSuite) TestNonTTYReadsLinesInOrder() {
	var prompt bytes.Buffer
	read := makePasswordReader(strings.NewReader("first\nsecond\n"), &prompt)

	pw1, err := read("Password: ")
	s.Require().NoError(err)
	s.Equal("first", pw1)

	pw2, err := read("Confirm:  ")
	s.Require().NoError(err)
	s.Equal("second", pw2)
	s.Contains(prompt.String(), "Password: ")
}
