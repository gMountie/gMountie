//go:build darwin || cgofuse

package cgofs

import (
	"context"

	"go.gmountie.dev/gmountie/pkg/client/backend"
	proto "go.gmountie.dev/gmountie/pkg/proto"
)

// fakeBackend is a programmable io.FileSystemBackend test double. Each field
// is the canned response for the matching op; calls are recorded for asserts.
// Reused by Tasks 5–8.
type fakeBackend struct {
	statAttr *backend.Attr
	statSt   proto.FsError
	statPath string

	listEntries []backend.DirEntryPlus
	listSt      proto.FsError

	statfs   *backend.StatFs
	statfsSt proto.FsError

	readlink   string
	readlinkSt proto.FsError

	openFH     backend.FileHandle
	openSt     proto.FsError
	createFH   backend.FileHandle
	createAttr *backend.Attr
	createSt   proto.FsError
	readData   []byte
	readSt     proto.FsError
	wroteData  []byte
	writeSt    proto.FsError
	released   []string
	setAttrIn  backend.SetAttrIn

	xattrData   []byte
	xattrGetSt  proto.FsError
	xattrNames  []string
	xattrListSt proto.FsError
}

func (f *fakeBackend) Stat(ctx context.Context, path string) (*backend.Attr, proto.FsError) {
	f.statPath = path
	return f.statAttr, f.statSt
}
func (f *fakeBackend) GetAttrIfChanged(ctx context.Context, path string, v uint64) (*backend.Attr, bool, proto.FsError) {
	return nil, false, proto.FsError_FS_EIO
}
func (f *fakeBackend) Lookup(ctx context.Context, parent, name string) (*backend.Attr, proto.FsError) {
	return f.statAttr, f.statSt
}
func (f *fakeBackend) ListDir(ctx context.Context, path string) ([]backend.DirEntryPlus, proto.FsError) {
	return f.listEntries, f.listSt
}
func (f *fakeBackend) Access(ctx context.Context, path string, mode uint32) proto.FsError {
	return proto.FsError_FS_OK
}
func (f *fakeBackend) StatFs(ctx context.Context, path string) (*backend.StatFs, proto.FsError) {
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
func (f *fakeBackend) Open(ctx context.Context, path string, flags uint32) (backend.FileHandle, proto.FsError) {
	return f.openFH, f.openSt
}
func (f *fakeBackend) Create(ctx context.Context, parent, name string, flags, mode uint32) (backend.FileHandle, *backend.Attr, proto.FsError) {
	return f.createFH, f.createAttr, f.createSt
}
func (f *fakeBackend) Read(ctx context.Context, fh backend.FileHandle, off int64, dest []byte) (int, proto.FsError) {
	n := copy(dest, f.readData)
	return n, f.readSt
}
func (f *fakeBackend) Write(ctx context.Context, fh backend.FileHandle, off int64, data []byte) (uint32, proto.FsError) {
	f.wroteData = append([]byte(nil), data...)
	return uint32(len(data)), f.writeSt
}
func (f *fakeBackend) Release(ctx context.Context, fh backend.FileHandle) proto.FsError {
	f.released = append(f.released, fh.Path())
	return proto.FsError_FS_OK
}
func (f *fakeBackend) Flush(ctx context.Context, fh backend.FileHandle) proto.FsError {
	return proto.FsError_FS_OK
}
func (f *fakeBackend) Fsync(ctx context.Context, fh backend.FileHandle, flags int64) proto.FsError {
	return proto.FsError_FS_OK
}
func (f *fakeBackend) Allocate(ctx context.Context, fh backend.FileHandle, off, size uint64, mode uint32) proto.FsError {
	return proto.FsError_FS_OK
}
func (f *fakeBackend) GetLk(ctx context.Context, fh backend.FileHandle, owner uint64, lk *backend.FileLock, flags uint32, out *backend.FileLock) proto.FsError {
	return proto.FsError_FS_ENOSYS
}
func (f *fakeBackend) SetLk(ctx context.Context, fh backend.FileHandle, owner uint64, lk *backend.FileLock, flags uint32) proto.FsError {
	return proto.FsError_FS_ENOSYS
}
func (f *fakeBackend) SetLkw(ctx context.Context, fh backend.FileHandle, owner uint64, lk *backend.FileLock, flags uint32) proto.FsError {
	return proto.FsError_FS_ENOSYS
}
func (f *fakeBackend) CopyFileRange(ctx context.Context, in backend.FileHandle, io1 uint64, out backend.FileHandle, oo uint64, length, flags uint64) (uint64, proto.FsError) {
	return 0, proto.FsError_FS_ENOSYS
}
func (f *fakeBackend) Lseek(ctx context.Context, fh backend.FileHandle, offset uint64, whence uint32) (uint64, proto.FsError) {
	return 0, proto.FsError_FS_ENOSYS
}
func (f *fakeBackend) Mkdir(ctx context.Context, path string, mode uint32) (*backend.Attr, proto.FsError) {
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
func (f *fakeBackend) Symlink(ctx context.Context, target, linkPath string) (*backend.Attr, proto.FsError) {
	return f.statAttr, f.statSt
}
func (f *fakeBackend) SetAttr(ctx context.Context, path string, in backend.SetAttrIn) (*backend.Attr, proto.FsError) {
	f.setAttrIn = in
	return f.statAttr, f.statSt
}
func (f *fakeBackend) Close() error { return nil }
