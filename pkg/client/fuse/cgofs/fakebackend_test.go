//go:build darwin || cgofuse

package cgofs

import (
	"context"

	gio "go.gmountie.dev/gmountie/pkg/client/io"
	proto "go.gmountie.dev/gmountie/pkg/proto"
)

// fakeBackend is a programmable io.FileSystemBackend test double. Each field
// is the canned response for the matching op; calls are recorded for asserts.
// Reused by Tasks 5–8.
type fakeBackend struct {
	statAttr *gio.Attr
	statSt   proto.FsError
	statPath string

	listEntries []gio.DirEntryPlus
	listSt      proto.FsError

	statfs   *gio.StatFs
	statfsSt proto.FsError

	readlink   string
	readlinkSt proto.FsError

	openFH     gio.FileHandle
	openSt     proto.FsError
	createFH   gio.FileHandle
	createAttr *gio.Attr
	createSt   proto.FsError
	readData   []byte
	readSt     proto.FsError
	wroteData  []byte
	writeSt    proto.FsError
	released   []string
	setAttrIn  gio.SetAttrIn

	xattrData   []byte
	xattrGetSt  proto.FsError
	xattrNames  []string
	xattrListSt proto.FsError
}

func (f *fakeBackend) Stat(ctx context.Context, path string) (*gio.Attr, proto.FsError) {
	f.statPath = path
	return f.statAttr, f.statSt
}
func (f *fakeBackend) GetAttrIfChanged(ctx context.Context, path string, v uint64) (*gio.Attr, bool, proto.FsError) {
	return nil, false, proto.FsError_FS_EIO
}
func (f *fakeBackend) Lookup(ctx context.Context, parent, name string) (*gio.Attr, proto.FsError) {
	return f.statAttr, f.statSt
}
func (f *fakeBackend) ListDir(ctx context.Context, path string) ([]gio.DirEntryPlus, proto.FsError) {
	return f.listEntries, f.listSt
}
func (f *fakeBackend) Access(ctx context.Context, path string, mode uint32) proto.FsError {
	return proto.FsError_FS_OK
}
func (f *fakeBackend) StatFs(ctx context.Context, path string) (*gio.StatFs, proto.FsError) {
	return f.statfs, f.statfsSt
}
func (f *fakeBackend) GetXAttr(ctx context.Context, path, attr string) ([]byte, proto.FsError) {
	return f.xattrData, f.xattrGetSt
}
func (f *fakeBackend) SetXAttr(ctx context.Context, path, attr string, data []byte, flags uint32) proto.FsError {
	return proto.FsError_FS_OK
}
func (f *fakeBackend) RemoveXAttr(ctx context.Context, path, attr string) proto.FsError {
	return proto.FsError_FS_OK
}
func (f *fakeBackend) ListXAttr(ctx context.Context, path string) ([]string, proto.FsError) {
	return f.xattrNames, f.xattrListSt
}
func (f *fakeBackend) Open(ctx context.Context, path string, flags uint32) (gio.FileHandle, proto.FsError) {
	return f.openFH, f.openSt
}
func (f *fakeBackend) Create(ctx context.Context, parent, name string, flags, mode uint32) (gio.FileHandle, *gio.Attr, proto.FsError) {
	return f.createFH, f.createAttr, f.createSt
}
func (f *fakeBackend) Read(ctx context.Context, fh gio.FileHandle, off int64, dest []byte) (int, proto.FsError) {
	n := copy(dest, f.readData)
	return n, f.readSt
}
func (f *fakeBackend) Write(ctx context.Context, fh gio.FileHandle, off int64, data []byte) (uint32, proto.FsError) {
	f.wroteData = append([]byte(nil), data...)
	return uint32(len(data)), f.writeSt
}
func (f *fakeBackend) Release(ctx context.Context, fh gio.FileHandle) proto.FsError {
	f.released = append(f.released, fh.Path())
	return proto.FsError_FS_OK
}
func (f *fakeBackend) Flush(ctx context.Context, fh gio.FileHandle) proto.FsError {
	return proto.FsError_FS_OK
}
func (f *fakeBackend) Fsync(ctx context.Context, fh gio.FileHandle, flags int64) proto.FsError {
	return proto.FsError_FS_OK
}
func (f *fakeBackend) Allocate(ctx context.Context, fh gio.FileHandle, off, size uint64, mode uint32) proto.FsError {
	return proto.FsError_FS_OK
}
func (f *fakeBackend) GetLk(ctx context.Context, fh gio.FileHandle, owner uint64, lk *gio.FileLock, flags uint32, out *gio.FileLock) proto.FsError {
	return proto.FsError_FS_ENOSYS
}
func (f *fakeBackend) SetLk(ctx context.Context, fh gio.FileHandle, owner uint64, lk *gio.FileLock, flags uint32) proto.FsError {
	return proto.FsError_FS_ENOSYS
}
func (f *fakeBackend) SetLkw(ctx context.Context, fh gio.FileHandle, owner uint64, lk *gio.FileLock, flags uint32) proto.FsError {
	return proto.FsError_FS_ENOSYS
}
func (f *fakeBackend) CopyFileRange(ctx context.Context, in gio.FileHandle, io1 uint64, out gio.FileHandle, oo uint64, length, flags uint64) (uint64, proto.FsError) {
	return 0, proto.FsError_FS_ENOSYS
}
func (f *fakeBackend) Lseek(ctx context.Context, fh gio.FileHandle, offset uint64, whence uint32) (uint64, proto.FsError) {
	return 0, proto.FsError_FS_ENOSYS
}
func (f *fakeBackend) Mkdir(ctx context.Context, path string, mode uint32) (*gio.Attr, proto.FsError) {
	return f.statAttr, f.statSt
}
func (f *fakeBackend) Rmdir(ctx context.Context, path string) proto.FsError {
	return proto.FsError_FS_OK
}
func (f *fakeBackend) Unlink(ctx context.Context, path string) proto.FsError {
	return proto.FsError_FS_OK
}
func (f *fakeBackend) Rename(ctx context.Context, oldPath, newPath string) proto.FsError {
	return proto.FsError_FS_OK
}
func (f *fakeBackend) Readlink(ctx context.Context, path string) (string, proto.FsError) {
	return f.readlink, f.readlinkSt
}
func (f *fakeBackend) Symlink(ctx context.Context, target, linkPath string) (*gio.Attr, proto.FsError) {
	return f.statAttr, f.statSt
}
func (f *fakeBackend) SetAttr(ctx context.Context, path string, in gio.SetAttrIn) (*gio.Attr, proto.FsError) {
	f.setAttrIn = in
	return f.statAttr, f.statSt
}
func (f *fakeBackend) Close() error { return nil }
