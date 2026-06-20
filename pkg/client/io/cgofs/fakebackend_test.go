//go:build darwin || cgofuse

package cgofs

import (
	"context"

	"github.com/hanwen/go-fuse/v2/fuse"
	gio "go.gmountie.dev/gmountie/pkg/client/io"
)

// fakeBackend is a programmable io.FileSystemBackend test double. Each field
// is the canned response for the matching op; calls are recorded for asserts.
// Reused by Tasks 5–8.
type fakeBackend struct {
	statAttr *gio.Attr
	statSt   fuse.Status
	statPath string

	listEntries []gio.DirEntryPlus
	listSt      fuse.Status

	statfs   *gio.StatFs
	statfsSt fuse.Status

	readlink   string
	readlinkSt fuse.Status

	lastCallerUID uint32
}

func (f *fakeBackend) Stat(ctx context.Context, path string) (*gio.Attr, fuse.Status) {
	f.statPath = path
	return f.statAttr, f.statSt
}
func (f *fakeBackend) GetAttrIfChanged(ctx context.Context, path string, v uint64) (*gio.Attr, bool, fuse.Status) {
	return nil, false, fuse.EIO
}
func (f *fakeBackend) Lookup(ctx context.Context, parent, name string) (*gio.Attr, fuse.Status) {
	return f.statAttr, f.statSt
}
func (f *fakeBackend) ListDir(ctx context.Context, path string) ([]gio.DirEntryPlus, fuse.Status) {
	return f.listEntries, f.listSt
}
func (f *fakeBackend) Access(ctx context.Context, path string, mode uint32) fuse.Status { return fuse.OK }
func (f *fakeBackend) StatFs(ctx context.Context, path string) (*gio.StatFs, fuse.Status) {
	return f.statfs, f.statfsSt
}
func (f *fakeBackend) GetXAttr(ctx context.Context, path, attr string) ([]byte, fuse.Status) {
	return nil, fuse.ENOATTR
}
func (f *fakeBackend) SetXAttr(ctx context.Context, path, attr string, data []byte, flags uint32) fuse.Status {
	return fuse.OK
}
func (f *fakeBackend) RemoveXAttr(ctx context.Context, path, attr string) fuse.Status { return fuse.OK }
func (f *fakeBackend) ListXAttr(ctx context.Context, path string) ([]string, fuse.Status) {
	return nil, fuse.OK
}
func (f *fakeBackend) Open(ctx context.Context, path string, flags uint32) (gio.FileHandle, fuse.Status) {
	return nil, fuse.OK
}
func (f *fakeBackend) Create(ctx context.Context, parent, name string, flags, mode uint32) (gio.FileHandle, *gio.Attr, fuse.Status) {
	return nil, nil, fuse.OK
}
func (f *fakeBackend) Read(ctx context.Context, fh gio.FileHandle, off int64, dest []byte) (int, fuse.Status) {
	return 0, fuse.OK
}
func (f *fakeBackend) Write(ctx context.Context, fh gio.FileHandle, off int64, data []byte) (uint32, fuse.Status) {
	return 0, fuse.OK
}
func (f *fakeBackend) Release(ctx context.Context, fh gio.FileHandle) fuse.Status { return fuse.OK }
func (f *fakeBackend) Flush(ctx context.Context, fh gio.FileHandle) fuse.Status   { return fuse.OK }
func (f *fakeBackend) Fsync(ctx context.Context, fh gio.FileHandle, flags int64) fuse.Status {
	return fuse.OK
}
func (f *fakeBackend) Allocate(ctx context.Context, fh gio.FileHandle, off, size uint64, mode uint32) fuse.Status {
	return fuse.OK
}
func (f *fakeBackend) GetLk(ctx context.Context, fh gio.FileHandle, owner uint64, lk *fuse.FileLock, flags uint32, out *fuse.FileLock) fuse.Status {
	return fuse.ENOSYS
}
func (f *fakeBackend) SetLk(ctx context.Context, fh gio.FileHandle, owner uint64, lk *fuse.FileLock, flags uint32) fuse.Status {
	return fuse.ENOSYS
}
func (f *fakeBackend) SetLkw(ctx context.Context, fh gio.FileHandle, owner uint64, lk *fuse.FileLock, flags uint32) fuse.Status {
	return fuse.ENOSYS
}
func (f *fakeBackend) CopyFileRange(ctx context.Context, in gio.FileHandle, io1 uint64, out gio.FileHandle, oo uint64, length, flags uint64) (uint64, fuse.Status) {
	return 0, fuse.ENOSYS
}
func (f *fakeBackend) Lseek(ctx context.Context, fh gio.FileHandle, offset uint64, whence uint32) (uint64, fuse.Status) {
	return 0, fuse.ENOSYS
}
func (f *fakeBackend) Mkdir(ctx context.Context, path string, mode uint32) (*gio.Attr, fuse.Status) {
	return f.statAttr, f.statSt
}
func (f *fakeBackend) Rmdir(ctx context.Context, path string) fuse.Status  { return fuse.OK }
func (f *fakeBackend) Unlink(ctx context.Context, path string) fuse.Status { return fuse.OK }
func (f *fakeBackend) Rename(ctx context.Context, oldPath, newPath string) fuse.Status {
	return fuse.OK
}
func (f *fakeBackend) Readlink(ctx context.Context, path string) (string, fuse.Status) {
	return f.readlink, f.readlinkSt
}
func (f *fakeBackend) Symlink(ctx context.Context, target, linkPath string) (*gio.Attr, fuse.Status) {
	return f.statAttr, f.statSt
}
func (f *fakeBackend) SetAttr(ctx context.Context, path string, in gio.SetAttrIn) (*gio.Attr, fuse.Status) {
	return f.statAttr, f.statSt
}
func (f *fakeBackend) Close() error { return nil }
