// Package fsconv maps OS-neutral wire enums (proto.SeekWhence, proto.LockType,
// proto.XAttrCreateMode) to and from the host kernel's native constants. Like
// pkg/common/fserr does for errno, this keeps the wire protocol OS-neutral: the
// raw SEEK_*, F_*LCK and XATTR_* values differ across OSes (notably SEEK_DATA
// and SEEK_HOLE are SWAPPED between Linux and macOS, and the lock-type and
// xattr-flag values all differ), so a darwin client must not hand its native
// values to a Linux server.
//
// The golang.org/x/sys/unix constants are themselves per-GOOS, so a single
// implementation yields the correct host value on whatever platform it is built
// for: each client adapter calls the ToProto direction with its native value,
// the server calls FromProto to get its own (Linux) native value.
package fsconv

import (
	proto "go.gmountie.dev/gmountie/pkg/proto"
	"golang.org/x/sys/unix"
)

// WhenceToProto maps a host lseek whence to the wire enum.
func WhenceToProto(whence int32) proto.SeekWhence {
	switch int(whence) {
	case unix.SEEK_SET:
		return proto.SeekWhence_SEEK_WHENCE_SET
	case unix.SEEK_CUR:
		return proto.SeekWhence_SEEK_WHENCE_CUR
	case unix.SEEK_END:
		return proto.SeekWhence_SEEK_WHENCE_END
	case unix.SEEK_DATA:
		return proto.SeekWhence_SEEK_WHENCE_DATA
	case unix.SEEK_HOLE:
		return proto.SeekWhence_SEEK_WHENCE_HOLE
	default:
		return proto.SeekWhence_SEEK_WHENCE_UNSPECIFIED
	}
}

// WhenceFromProto maps the wire enum to a host lseek whence. Unknown/unspecified
// maps to SEEK_SET (a benign default; callers validate where they care).
func WhenceFromProto(w proto.SeekWhence) int {
	switch w {
	case proto.SeekWhence_SEEK_WHENCE_SET:
		return unix.SEEK_SET
	case proto.SeekWhence_SEEK_WHENCE_CUR:
		return unix.SEEK_CUR
	case proto.SeekWhence_SEEK_WHENCE_END:
		return unix.SEEK_END
	case proto.SeekWhence_SEEK_WHENCE_DATA:
		return unix.SEEK_DATA
	case proto.SeekWhence_SEEK_WHENCE_HOLE:
		return unix.SEEK_HOLE
	default:
		return unix.SEEK_SET
	}
}

// LockTypeToProto maps a host fcntl lock type (F_RDLCK/F_WRLCK/F_UNLCK) to the
// wire enum.
func LockTypeToProto(typ uint32) proto.LockType {
	switch int(typ) {
	case unix.F_RDLCK:
		return proto.LockType_LOCK_TYPE_READ
	case unix.F_WRLCK:
		return proto.LockType_LOCK_TYPE_WRITE
	case unix.F_UNLCK:
		return proto.LockType_LOCK_TYPE_UNLOCK
	default:
		return proto.LockType_LOCK_TYPE_UNSPECIFIED
	}
}

// LockTypeFromProto maps the wire enum to a host fcntl lock type. Unknown maps
// to F_UNLCK (the safe default — never silently upgrades to a held lock).
func LockTypeFromProto(t proto.LockType) uint32 {
	switch t {
	case proto.LockType_LOCK_TYPE_READ:
		return uint32(unix.F_RDLCK)
	case proto.LockType_LOCK_TYPE_WRITE:
		return uint32(unix.F_WRLCK)
	case proto.LockType_LOCK_TYPE_UNLOCK:
		return uint32(unix.F_UNLCK)
	default:
		return uint32(unix.F_UNLCK)
	}
}

// XAttrModeToProto maps host SETXATTR flags to the wire create/replace enum.
// XATTR_CREATE and XATTR_REPLACE are mutually exclusive; every other host bit
// (macOS NOFOLLOW/NOSECURITY/NODEFAULT/SHOWCOMPRESSION) carries no portable
// meaning and is dropped — which is what fixes Finder's FinderInfo write
// (it sets XATTR_NODEFAULT 0x10, an invalid Linux setxattr flag).
func XAttrModeToProto(flags int) proto.XAttrCreateMode {
	switch {
	case flags&unix.XATTR_CREATE != 0:
		return proto.XAttrCreateMode_XATTR_CREATE_MODE_CREATE
	case flags&unix.XATTR_REPLACE != 0:
		return proto.XAttrCreateMode_XATTR_CREATE_MODE_REPLACE
	default:
		return proto.XAttrCreateMode_XATTR_CREATE_MODE_NONE
	}
}

// XAttrModeFromProto maps the wire enum to host SETXATTR flags.
func XAttrModeFromProto(m proto.XAttrCreateMode) int {
	switch m {
	case proto.XAttrCreateMode_XATTR_CREATE_MODE_CREATE:
		return unix.XATTR_CREATE
	case proto.XAttrCreateMode_XATTR_CREATE_MODE_REPLACE:
		return unix.XATTR_REPLACE
	default:
		return 0
	}
}
