package io

import (
	"os"

	"github.com/hanwen/go-fuse/v2/fuse"
	"github.com/hanwen/go-fuse/v2/fuse/nodefs"
	"golang.org/x/sys/unix"
)

const (
	// copyChunk caps a single copy_file_range(2) call.
	copyChunk = 1 << 30
	// copyBufSize is the buffer for the interface-based fallback loop.
	copyBufSize = 1 << 20
)

// RawFdFile couples the loopback nodefs.File with its backing *os.File so
// fd-level server ops (copy_file_range, lseek) can reach the raw
// descriptor. All nodefs.File behavior — including Release closing the
// fd — is delegated to the embedded loopback file.
type RawFdFile struct {
	nodefs.File
	Raw *os.File
}

// NewRawFdFile wraps an already-open *os.File. The loopback takes
// ownership of f; its Release closes the descriptor, after which Raw must
// not be used.
func NewRawFdFile(f *os.File) *RawFdFile {
	return &RawFdFile{File: nodefs.NewLoopbackFile(f), Raw: f}
}

// CopyFileRange copies up to length bytes from src@offIn to dst@offOut
// entirely on the server. Fast path: copy_file_range(2) between raw fds
// (reflink-fast on capable filesystems). Falls back to an interface-based
// read/write loop when the syscall reports it can't operate (EXDEV,
// EOPNOTSUPP, ENOSYS) or when either file isn't fd-backed. EINVAL is NOT
// a fallback trigger — per copy_file_range(2) it signals overlapping
// ranges within one file and must propagate. Short copies (source EOF)
// return the partial count with OK; callers reissue.
func CopyFileRange(src, dst nodefs.File, offIn, offOut, length uint64) (uint64, fuse.Status) {
	if length == 0 {
		return 0, fuse.OK
	}
	sf, sok := src.(*RawFdFile)
	df, dok := dst.(*RawFdFile)
	if sok && dok {
		n, st, fallback := copyRangeFd(sf, df, offIn, offOut, length)
		if !fallback {
			return n, st
		}
	}
	return copyRangeGeneric(src, dst, offIn, offOut, length)
}

// copyRangeFd drives copy_file_range(2). fallback=true means "syscall
// unsupported here, try the generic loop" and is only ever reported
// before any bytes moved — once data has been copied, partial progress
// is returned as a short success instead.
func copyRangeFd(src, dst *RawFdFile, offIn, offOut, length uint64) (copied uint64, st fuse.Status, fallback bool) {
	in, out := int64(offIn), int64(offOut)
	for copied < length {
		chunk := length - copied
		if chunk > copyChunk {
			chunk = copyChunk
		}
		n, err := unix.CopyFileRange(int(src.Raw.Fd()), &in, int(dst.Raw.Fd()), &out, int(chunk), 0)
		if err != nil {
			if copied > 0 {
				return copied, fuse.OK, false
			}
			switch err {
			case unix.EXDEV, unix.EOPNOTSUPP, unix.ENOSYS:
				return 0, fuse.OK, true
			}
			// NOTE: must precede errnoToStatus — that maps EXDEV to
			// EACCES for path-resolution escapes, which is wrong here.
			return 0, errnoToStatus(err), false
		}
		if n == 0 { // source EOF
			break
		}
		copied += uint64(n)
	}
	return copied, fuse.OK, false
}

// rangesOverlap reports whether [offIn, offIn+length) and
// [offOut, offOut+length) intersect.
func rangesOverlap(offIn, offOut, length uint64) bool {
	return offIn < offOut+length && offOut < offIn+length
}

// copyRangeGeneric is the interface-based fallback: read from src, write
// to dst, all server-side. Replicates the kernel's same-file overlap
// check, which the fd path gets for free. Ino==0 means GetAttr gave us
// nothing usable — skip the check rather than false-positive (volumes
// are confined to one filesystem via RESOLVE_NO_XDEV, so Ino equality
// implies same inode).
func copyRangeGeneric(src, dst nodefs.File, offIn, offOut, length uint64) (uint64, fuse.Status) {
	var sa, da fuse.Attr
	if src.GetAttr(&sa).Ok() && dst.GetAttr(&da).Ok() &&
		sa.Ino != 0 && sa.Ino == da.Ino && rangesOverlap(offIn, offOut, length) {
		return 0, fuse.EINVAL
	}
	buf := make([]byte, copyBufSize)
	var copied uint64
	for copied < length {
		chunk := uint64(len(buf))
		if rem := length - copied; rem < chunk {
			chunk = rem
		}
		res, st := src.Read(buf[:chunk], int64(offIn+copied))
		if !st.Ok() {
			return copied, st
		}
		data, st := res.Bytes(buf[:chunk])
		if !st.Ok() {
			return copied, st
		}
		if len(data) == 0 { // source EOF
			break
		}
		written := 0
		for written < len(data) {
			n, wst := dst.Write(data[written:], int64(offOut+copied)+int64(written))
			if !wst.Ok() {
				return copied + uint64(written), wst
			}
			if n == 0 {
				return copied + uint64(written), fuse.EIO
			}
			written += int(n)
		}
		copied += uint64(len(data))
	}
	return copied, fuse.OK
}

// Lseek probes hole geometry (SEEK_DATA/SEEK_HOLE) on an open file. Only
// fd-backed files can answer; anything else reports ENOTSUP. ENXIO
// (offset at/past EOF) passes through — it's the protocol's functional
// "no more data/hole" signal, not an error.
// Callers are responsible for restricting whence to SEEK_DATA/SEEK_HOLE
// (the controller validates); other values would mutate the shared fd
// offset via plain lseek semantics.
func Lseek(f nodefs.File, offset uint64, whence uint32) (uint64, fuse.Status) {
	rf, ok := f.(*RawFdFile)
	if !ok {
		return 0, fuse.ENOTSUP
	}
	off, err := unix.Seek(int(rf.Raw.Fd()), int64(offset), int(whence))
	if err != nil {
		return 0, errnoToStatus(err)
	}
	return uint64(off), fuse.OK
}
