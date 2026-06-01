package commands

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/spf13/viper"
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

func (s *CredentialsSuite) TestResolveConfiguredPassword_CommandWins() {
	v := viper.New()
	v.Set("auth.password_command", "printf 'fromcmd'")
	v.Set("auth.password_file", "/should/not/be/read")
	v.Set("auth.password", "inline")
	pw, err := resolveConfiguredPassword(context.Background(), v)
	s.Require().NoError(err)
	s.Equal("fromcmd", pw)
}

func (s *CredentialsSuite) TestResolveConfiguredPassword_FileTrimmedAndPermChecked() {
	dir := s.T().TempDir()
	f := filepath.Join(dir, "pw")
	s.Require().NoError(os.WriteFile(f, []byte("secret\n"), 0o600))
	v := viper.New()
	v.Set("auth.password_file", f)
	pw, err := resolveConfiguredPassword(context.Background(), v)
	s.Require().NoError(err)
	s.Equal("secret", pw)
	s.Require().NoError(os.Chmod(f, 0o644))
	_, err = resolveConfiguredPassword(context.Background(), v)
	s.Error(err)
}

func (s *CredentialsSuite) TestResolveConfiguredPassword_InlineFallback() {
	v := viper.New()
	v.Set("auth.password", "inline")
	pw, err := resolveConfiguredPassword(context.Background(), v)
	s.Require().NoError(err)
	s.Equal("inline", pw)
}

func (s *CredentialsSuite) TestResolveConfiguredPassword_CommandFails() {
	v := viper.New()
	v.Set("auth.password_command", "exit 3")
	_, err := resolveConfiguredPassword(context.Background(), v)
	s.Error(err)
}
