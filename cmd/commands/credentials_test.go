package commands

import (
	"bytes"
	"strings"
	"testing"

	"github.com/stretchr/testify/suite"
)

type CredentialsSuite struct{ suite.Suite }

func TestCredentialsSuite(t *testing.T) { suite.Run(t, new(CredentialsSuite)) }

func (s *CredentialsSuite) TestFlagWins() {
	s.T().Setenv("GMOUNTIE_AUTH_PASSWORD", "fromenv")
	got, err := resolvePassword("fromflag", strings.NewReader(""), &bytes.Buffer{})
	s.Require().NoError(err)
	s.Equal("fromflag", got)
}

func (s *CredentialsSuite) TestEnvUsedWhenNoFlag() {
	s.T().Setenv("GMOUNTIE_AUTH_PASSWORD", "fromenv")
	got, err := resolvePassword("", strings.NewReader(""), &bytes.Buffer{})
	s.Require().NoError(err)
	s.Equal("fromenv", got)
}

func (s *CredentialsSuite) TestPromptsFromNonFileReader() {
	s.T().Setenv("GMOUNTIE_AUTH_PASSWORD", "")
	var prompt bytes.Buffer
	got, err := resolvePassword("", strings.NewReader("typed\n"), &prompt)
	s.Require().NoError(err)
	s.Equal("typed", got)
	s.Contains(prompt.String(), "Password")
}

func (s *CredentialsSuite) TestErrorsWhenEmptyAndNoInput() {
	s.T().Setenv("GMOUNTIE_AUTH_PASSWORD", "")
	_, err := resolvePassword("", strings.NewReader(""), &bytes.Buffer{})
	s.Require().Error(err)
}
