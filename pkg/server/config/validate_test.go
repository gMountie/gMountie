package config

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/suite"
)

type ValidatePathsSuite struct{ suite.Suite }

func TestValidatePathsSuite(t *testing.T) { suite.Run(t, new(ValidatePathsSuite)) }

func (s *ValidatePathsSuite) TestAcceptsExistingDir() {
	dir := s.T().TempDir()
	c := &Config{Volumes: []*VolumeConfig{{Name: "shared", Path: dir}}}
	s.NoError(c.ValidateVolumePaths())
}

func (s *ValidatePathsSuite) TestRejectsMissingPath() {
	missing := filepath.Join(s.T().TempDir(), "nope")
	c := &Config{Volumes: []*VolumeConfig{{Name: "shared", Path: missing}}}
	err := c.ValidateVolumePaths()
	s.Require().Error(err)
	s.Contains(err.Error(), "shared")
	s.Contains(err.Error(), missing)
}

func (s *ValidatePathsSuite) TestRejectsFileNotDir() {
	f := filepath.Join(s.T().TempDir(), "afile")
	s.Require().NoError(os.WriteFile(f, []byte("x"), 0o600))
	c := &Config{Volumes: []*VolumeConfig{{Name: "v", Path: f}}}
	err := c.ValidateVolumePaths()
	s.Require().Error(err)
	s.Contains(err.Error(), "not a directory")
}
