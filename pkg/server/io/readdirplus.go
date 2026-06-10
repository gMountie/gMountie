package io

import (
	"syscall"
	"unsafe"

	"github.com/hanwen/go-fuse/v2/fuse"
	"golang.org/x/sys/unix"
)

// fstatatFn is the syscall seam for per-entry stat in ReadDirPlus. Tests swap
// it to inject failures for specific entry names; production never reassigns it.
var fstatatFn = unix.Fstatat

// DirEntryPlus pairs a directory entry with its attributes (READDIRPLUS).
// Attr is nil when the caller did not ask for attributes or when the
// per-entry stat failed (e.g. the entry was deleted between the getdents
// read and the stat) — the entry itself is still valid.
type DirEntryPlus struct {
	Entry fuse.DirEntry
	Attr  *fuse.Attr
}

// ReadDirPlusser is the optional capability extension to pathfs.FileSystem
// for listing a directory together with per-entry attributes in one call.
// pathfs.FileSystem is go-fuse's interface, so the capability cannot be added
// to it directly; callers type-assert and fall back to OpenDir + nil attrs
// when the filesystem does not implement it.
type ReadDirPlusser interface {
	ReadDirPlus(name string, context *fuse.Context) ([]DirEntryPlus, fuse.Status)
}

// ReadDirPlus lists a directory beneath the volume root and stats every
// entry. Confinement and listing mirror OpenDir exactly (one resolveBeneath +
// one openat2 for the directory itself); the difference is that the directory
// fd is then KEPT OPEN across the loop so each entry is stat'ed with a single
// Fstatat relative to it. Directory entry names never contain a path
// separator, so an fd-relative fstatat of an immediate child cannot traverse
// anywhere — one openat2 confines the whole listing, and re-running
// resolveBeneath per entry would only add N syscall round-trips for zero
// extra safety. Additionally, the kernel delivers "." and ".." in the getdents
// results but Go's os.(*File).ReadDir filters them out before returning — so
// no dot entry ever reaches fstatatFn. AT_SYMLINK_NOFOLLOW returns the attrs
// of a symlink itself (READDIRPLUS semantics — the client decides whether to
// follow).
func (c *ConfinedLoopbackFileSystem) ReadDirPlus(name string, _ *fuse.Context) ([]DirEntryPlus, fuse.Status) {
	// openConfinedDir keeps the *os.File open so its fd anchors every per-entry
	// Fstatat below; we close explicitly at the end rather than via defer in
	// openConfinedDir.
	f, entries, st := c.openConfinedDir(name)
	if st != fuse.OK {
		return nil, st
	}
	defer func() { _ = f.Close() }()
	dirFd := int(f.Fd())
	out := make([]DirEntryPlus, 0, len(entries))
	for _, e := range entries {
		d := DirEntryPlus{Entry: fuse.DirEntry{Name: e.Name()}}
		var stat unix.Stat_t
		if serr := fstatatFn(dirFd, e.Name(), &stat, unix.AT_SYMLINK_NOFOLLOW); serr == nil {
			// Same conversion as GetAttr: fuse.Attr.FromStat over the raw
			// kernel stat — entry Mode/Ino come from the same struct.
			a := &fuse.Attr{}
			a.FromStat((*syscall.Stat_t)(unsafe.Pointer(&stat)))
			d.Attr = a
			d.Entry.Mode = stat.Mode
			d.Entry.Ino = stat.Ino
		} else {
			// Entry vanished (or is unstatable) between getdents and stat.
			// Keep it with the d_type-derived mode and nil attrs — the client
			// treats nil as "attrs unknown", same as a plain readdir entry.
			d.Entry.Mode = direntTypeToMode(e.Type())
		}
		out = append(out, d)
	}
	return out, fuse.OK
}
