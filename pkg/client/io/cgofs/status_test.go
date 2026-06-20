package cgofs

import (
	"testing"

	"github.com/hanwen/go-fuse/v2/fuse"
	"github.com/stretchr/testify/suite"
)

type StatusSuite struct{ suite.Suite }

func TestStatusSuite(t *testing.T) { suite.Run(t, new(StatusSuite)) }

func (s *StatusSuite) TestOKMapsToZero() {
	s.Equal(0, errc(fuse.OK))
}

func (s *StatusSuite) TestErrnoMapsToNegative() {
	s.Equal(-int(fuse.ENOENT), errc(fuse.ENOENT))
	s.Equal(-int(fuse.EACCES), errc(fuse.EACCES))
	s.Equal(-int(fuse.EIO), errc(fuse.EIO))
}
