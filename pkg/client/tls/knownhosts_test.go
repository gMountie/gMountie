package tls

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/suite"
)

type KnownHostsTestSuite struct {
	suite.Suite
}

// newKH returns a KnownHosts backed by a temporary file inside T.TempDir().
func (s *KnownHostsTestSuite) newKH() *KnownHosts {
	dir := s.T().TempDir()
	kh, err := openKnownHosts(filepath.Join(dir, "known_hosts"))
	s.Require().NoError(err)
	return kh
}

func (s *KnownHostsTestSuite) TestKnownHostsPinAndLookup() {
	kh := s.newKH()
	err := kh.Pin("host:9449", "SHA256:abc123")
	s.Require().NoError(err)

	fp, ok := kh.Lookup("host:9449")
	s.True(ok)
	s.Equal("SHA256:abc123", fp)
}

func (s *KnownHostsTestSuite) TestKnownHostsLookupMissingEndpointReturnsFalse() {
	kh := s.newKH()
	fp, ok := kh.Lookup("notpinned:9449")
	s.False(ok)
	s.Empty(fp)
}

func (s *KnownHostsTestSuite) TestKnownHostsDuplicatePinIdempotent() {
	kh := s.newKH()
	s.Require().NoError(kh.Pin("host:9449", "SHA256:abc123"))
	s.Require().NoError(kh.Pin("host:9449", "SHA256:abc123"))

	// Exactly one line should be in the file.
	raw, err := os.ReadFile(kh.path)
	s.Require().NoError(err)
	lines := 0
	for _, line := range strings.Split(strings.TrimSpace(string(raw)), "\n") {
		if strings.TrimSpace(line) != "" {
			lines++
		}
	}
	s.Equal(1, lines, "duplicate pin should not write a second line")
}

func (s *KnownHostsTestSuite) TestKnownHostsRefuseOverwrite() {
	kh := s.newKH()
	s.Require().NoError(kh.Pin("host:9449", "SHA256:abc123"))

	err := kh.Pin("host:9449", "SHA256:different")
	s.Require().Error(err)
	s.Contains(err.Error(), "refusing to overwrite")

	// The original pin is unchanged.
	fp, ok := kh.Lookup("host:9449")
	s.True(ok)
	s.Equal("SHA256:abc123", fp)
}

func TestKnownHostsTestSuite(t *testing.T) {
	suite.Run(t, new(KnownHostsTestSuite))
}
