package fsconv

import (
	"testing"

	"github.com/stretchr/testify/suite"
	proto "go.gmountie.dev/gmountie/pkg/proto"
	"golang.org/x/sys/unix"
)

// FsconvSuite checks the host<->wire mapping round-trips using the host's own
// unix.* constants, so it validates whichever platform it is built for (CI runs
// it on Linux, where SEEK_DATA/HOLE = 3/4, F_*LCK = 0/1/2, XATTR_* = 0x1/0x2).
type FsconvSuite struct {
	suite.Suite
}

func TestFsconvSuite(t *testing.T) {
	suite.Run(t, new(FsconvSuite))
}

func (s *FsconvSuite) TestWhenceRoundTrip() {
	for _, native := range []int{unix.SEEK_SET, unix.SEEK_CUR, unix.SEEK_END, unix.SEEK_DATA, unix.SEEK_HOLE} {
		s.Equal(native, WhenceFromProto(WhenceToProto(int32(native))),
			"native whence %d must round-trip", native)
	}
	// The wire values are the canonical ones regardless of host.
	s.Equal(proto.SeekWhence_SEEK_WHENCE_DATA, WhenceToProto(int32(unix.SEEK_DATA)))
	s.Equal(proto.SeekWhence_SEEK_WHENCE_HOLE, WhenceToProto(int32(unix.SEEK_HOLE)))
	s.Equal(unix.SEEK_SET, WhenceFromProto(proto.SeekWhence_SEEK_WHENCE_UNSPECIFIED), "unspecified -> safe SEEK_SET")
}

func (s *FsconvSuite) TestLockTypeRoundTrip() {
	for _, native := range []uint32{uint32(unix.F_RDLCK), uint32(unix.F_WRLCK), uint32(unix.F_UNLCK)} {
		s.Equal(native, LockTypeFromProto(LockTypeToProto(native)),
			"native lock type %d must round-trip", native)
	}
	s.Equal(proto.LockType_LOCK_TYPE_WRITE, LockTypeToProto(uint32(unix.F_WRLCK)))
	s.Equal(uint32(unix.F_UNLCK), LockTypeFromProto(proto.LockType_LOCK_TYPE_UNSPECIFIED), "unspecified -> safe F_UNLCK")
}

func (s *FsconvSuite) TestXAttrModeRoundTrip() {
	s.Equal(proto.XAttrCreateMode_XATTR_CREATE_MODE_CREATE, XAttrModeToProto(unix.XATTR_CREATE))
	s.Equal(proto.XAttrCreateMode_XATTR_CREATE_MODE_REPLACE, XAttrModeToProto(unix.XATTR_REPLACE))
	s.Equal(proto.XAttrCreateMode_XATTR_CREATE_MODE_NONE, XAttrModeToProto(0))
	s.Equal(unix.XATTR_CREATE, XAttrModeFromProto(proto.XAttrCreateMode_XATTR_CREATE_MODE_CREATE))
	s.Equal(unix.XATTR_REPLACE, XAttrModeFromProto(proto.XAttrCreateMode_XATTR_CREATE_MODE_REPLACE))
	s.Equal(0, XAttrModeFromProto(proto.XAttrCreateMode_XATTR_CREATE_MODE_NONE))
}

func (s *FsconvSuite) TestXAttrModeDropsForeignBits() {
	// A high bit with no Linux meaning (mimics macOS XATTR_NODEFAULT 0x10) is
	// dropped, and a CREATE accompanied by such a bit still maps to CREATE.
	s.Equal(proto.XAttrCreateMode_XATTR_CREATE_MODE_NONE, XAttrModeToProto(0x10))
	s.Equal(proto.XAttrCreateMode_XATTR_CREATE_MODE_CREATE, XAttrModeToProto(unix.XATTR_CREATE|0x10))
}
