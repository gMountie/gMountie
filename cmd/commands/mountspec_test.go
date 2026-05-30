package commands

import (
	"testing"

	"github.com/stretchr/testify/suite"
)

type MountSpecSuite struct{ suite.Suite }

func TestMountSpecSuite(t *testing.T) { suite.Run(t, new(MountSpecSuite)) }

func (s *MountSpecSuite) TestParses() {
	cases := []struct {
		in              string
		user, host, vol string
		port            int
	}{
		{"admin@host.example:9449/shared", "admin", "host.example", "shared", 9449},
		{"host.example/shared", "", "host.example", "shared", 9449},
		{"admin@10.0.0.5/data", "admin", "10.0.0.5", "data", 9449},
		{"host:7000/vol", "", "host", "vol", 7000},
	}
	for _, c := range cases {
		got, err := parseMountSpec(c.in)
		s.Require().NoError(err, c.in)
		s.Equal(c.user, got.Username, c.in)
		s.Equal(c.host, got.Host, c.in)
		s.Equal(c.port, got.Port, c.in)
		s.Equal(c.vol, got.Volume, c.in)
	}
}

func (s *MountSpecSuite) TestRejectsMalformed() {
	for _, in := range []string{"hostonly", "host/", "/vol", "host:notaport/vol", ""} {
		_, err := parseMountSpec(in)
		s.Require().Error(err, in)
	}
}
