//go:build darwin || cgofuse

package cgofs

import (
	"testing"

	"github.com/stretchr/testify/suite"
	cgofuse "github.com/winfsp/cgofuse/fuse"
	"go.gmountie.dev/gmountie/pkg/client/backend"
)

type AttrSuite struct{ suite.Suite }

func TestAttrSuite(t *testing.T) { suite.Run(t, new(AttrSuite)) }

func (s *AttrSuite) TestFillStatCopiesFieldsAndTimes() {
	a := &backend.Attr{
		Ino: 7, Size: 1024, Blocks: 2, Mode: 0o100644, Nlink: 1,
		Uid: 1000, Gid: 1000, Rdev: 0, Blksize: 4096,
		Atime: 100, Atimensec: 5, Mtime: 200, Mtimensec: 6, Ctime: 300, Ctimensec: 7,
	}
	var st cgofuse.Stat_t
	// fillStat is now a plain field copy — uid/gid are already-rewritten local
	// display ids supplied by the identity backend layer, so they pass through.
	fillStat(&st, a)
	s.Equal(uint64(7), st.Ino)
	s.Equal(int64(1024), st.Size)
	s.Equal(uint32(0o100644), st.Mode)
	s.Equal(uint32(1000), st.Uid)
	s.Equal(uint32(1000), st.Gid)
	s.Equal(int64(100), st.Atim.Sec)
	s.Equal(int64(5), st.Atim.Nsec)
	s.Equal(int64(200), st.Mtim.Sec)
	s.Equal(int64(6), st.Mtim.Nsec)
	s.Equal(int64(300), st.Ctim.Sec)
	s.Equal(int64(7), st.Ctim.Nsec)
}

func (s *AttrSuite) TestFillStatfs() {
	in := &backend.StatFs{Blocks: 10, Bfree: 4, Bavail: 3, Files: 100, Ffree: 50, Bsize: 4096, Namelen: 255, Frsize: 4096}
	var out cgofuse.Statfs_t
	fillStatfs(&out, in)
	s.Equal(uint64(10), out.Blocks)
	s.Equal(uint64(4096), out.Bsize)
	s.Equal(uint64(255), out.Namemax)
}
