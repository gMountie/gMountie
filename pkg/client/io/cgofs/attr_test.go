//go:build darwin || cgofuse

package cgofs

import (
	"testing"

	cgofuse "github.com/winfsp/cgofuse/fuse"
	gio "go.gmountie.dev/gmountie/pkg/client/io"
	"github.com/stretchr/testify/suite"
)

type AttrSuite struct{ suite.Suite }

func TestAttrSuite(t *testing.T) { suite.Run(t, new(AttrSuite)) }

func (s *AttrSuite) TestFillStatCopiesFieldsAndTimes() {
	a := &gio.Attr{
		Ino: 7, Size: 1024, Blocks: 2, Mode: 0o100644, Nlink: 1,
		Uid: 1000, Gid: 1000, Rdev: 0, Blksize: 4096,
		Atime: 100, Atimensec: 5, Mtime: 200, Mtimensec: 6, Ctime: 300, Ctimensec: 7,
	}
	var st cgofuse.Stat_t
	fillStat(&st, a, nil) // nil rewriter = identity
	s.Equal(uint64(7), st.Ino)
	s.Equal(int64(1024), st.Size)
	s.Equal(uint32(0o100644), st.Mode)
	s.Equal(uint32(1000), st.Uid)
	s.Equal(int64(100), st.Atim.Sec)
	s.Equal(int64(5), st.Atim.Nsec)
	s.Equal(int64(200), st.Mtim.Sec)
}

func (s *AttrSuite) TestFillStatAppliesRewriter() {
	// Server identity uid=1000 maps to local uid=501.
	rw := gio.NewIDRewriter(&gio.Identity{Uid: 1000, Gid: 1000}, 501, 20)
	a := &gio.Attr{Mode: 0o100644, Uid: 1000, Gid: 1000}
	var st cgofuse.Stat_t
	fillStat(&st, a, rw)
	s.Equal(uint32(501), st.Uid)
	s.Equal(uint32(20), st.Gid)
}

func (s *AttrSuite) TestFillStatfs() {
	in := &gio.StatFs{Blocks: 10, Bfree: 4, Bavail: 3, Files: 100, Ffree: 50, Bsize: 4096, Namelen: 255, Frsize: 4096}
	var out cgofuse.Statfs_t
	fillStatfs(&out, in)
	s.Equal(uint64(10), out.Blocks)
	s.Equal(uint64(4096), out.Bsize)
	s.Equal(uint64(255), out.Namemax)
}
