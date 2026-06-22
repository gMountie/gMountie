//go:build darwin || cgofuse

package cgofs

import (
	"testing"

	"github.com/hanwen/go-fuse/v2/fuse"
	"github.com/stretchr/testify/suite"
	cgofuse "github.com/winfsp/cgofuse/fuse"
	"go.gmountie.dev/gmountie/pkg/client/backend"
	fserr "go.gmountie.dev/gmountie/pkg/common/fserr"
	proto "go.gmountie.dev/gmountie/pkg/proto"
)

type MetaSuite struct {
	suite.Suite
	be *fakeBackend
	fs *MountieCgoFS
}

func TestMetaSuite(t *testing.T) { suite.Run(t, new(MetaSuite)) }

func (s *MetaSuite) SetupTest() {
	s.be = &fakeBackend{}
	s.fs = New(s.be)
}

func (s *MetaSuite) TestGetattrOK() {
	s.be.statAttr = &backend.Attr{Ino: 9, Size: 42, Mode: 0o100644}
	s.be.statSt = proto.FsError_FS_OK
	var st cgofuse.Stat_t
	rc := s.fs.Getattr("/dir/file", &st, ^uint64(0))
	s.Equal(0, rc)
	s.Equal(uint64(9), st.Ino)
	s.Equal(int64(42), st.Size)
	s.Equal("dir/file", s.be.statPath) // leading slash stripped
}

func (s *MetaSuite) TestGetattrENOENT() {
	s.be.statSt = proto.FsError_FS_ENOENT
	var st cgofuse.Stat_t
	rc := s.fs.Getattr("/missing", &st, ^uint64(0))
	s.Equal(-int(fserr.ToErrno(proto.FsError_FS_ENOENT)), rc)
}

func (s *MetaSuite) TestReaddirFillsEntries() {
	s.be.listSt = proto.FsError_FS_OK
	s.be.listEntries = []backend.DirEntryPlus{
		{DirEntry: backend.DirEntry{Name: "a", Ino: 1, Mode: 0o100644}},
		{DirEntry: backend.DirEntry{Name: "b", Ino: 2, Mode: fuse.S_IFDIR | 0o755}},
	}
	var names []string
	fill := func(name string, stat *cgofuse.Stat_t, ofst int64) bool {
		names = append(names, name)
		return true
	}
	rc := s.fs.Readdir("/", fill, 0, 0)
	s.Equal(0, rc)
	s.Equal([]string{".", "..", "a", "b"}, names)
}

func (s *MetaSuite) TestReadlink() {
	s.be.readlink = "target/path"
	s.be.readlinkSt = proto.FsError_FS_OK
	rc, target := s.fs.Readlink("/link")
	s.Equal(0, rc)
	s.Equal("target/path", target)
}

func (s *MetaSuite) TestStatfs() {
	s.be.statfs = &backend.StatFs{Blocks: 10, Bsize: 4096, Namelen: 255}
	s.be.statfsSt = proto.FsError_FS_OK
	var out cgofuse.Statfs_t
	rc := s.fs.Statfs("/", &out)
	s.Equal(0, rc)
	s.Equal(uint64(10), out.Blocks)
}

func (s *MetaSuite) TestOpendirReleasedirAreNoopSuccess() {
	rc, fh := s.fs.Opendir("/")
	s.Equal(0, rc)
	s.Equal(uint64(0), fh)
	s.Equal(0, s.fs.Releasedir("/", fh))
}
