package io

import (
	"context"

	"go.gmountie.dev/gmountie/pkg/proto"
	"github.com/hanwen/go-fuse/v2/fuse"
)

// PassthroughBackend is an embeddable base for OBSERVER layers — layers that
// add side effects (metrics, tracing, audit) without changing behavior. Embed
// it, set Inner, and override only the ops you observe; all other ops forward
// to Inner unchanged.
//
// DO NOT embed this in a SEMANTIC layer (cache, write-batcher, WAL). A new
// interface method would forward silently and bypass the layer's behavior — a
// stale-data bug. Semantic layers must implement FileSystemBackend explicitly
// so the compiler forces a decision on every method. (Enforced by
// TestSemanticLayersDoNotEmbedPassthrough.)
type PassthroughBackend struct {
	Inner FileSystemBackend
}

func (p *PassthroughBackend) Stat(ctx context.Context, path string) (*Attr, proto.FsError) {
	return p.Inner.Stat(ctx, path)
}
func (p *PassthroughBackend) GetAttrIfChanged(ctx context.Context, path string, knownVersion uint64) (*Attr, bool, proto.FsError) {
	return p.Inner.GetAttrIfChanged(ctx, path, knownVersion)
}
func (p *PassthroughBackend) Lookup(ctx context.Context, parent, name string) (*Attr, proto.FsError) {
	return p.Inner.Lookup(ctx, parent, name)
}
func (p *PassthroughBackend) ListDir(ctx context.Context, path string) ([]DirEntryPlus, proto.FsError) {
	return p.Inner.ListDir(ctx, path)
}
func (p *PassthroughBackend) Access(ctx context.Context, path string, mode uint32) proto.FsError {
	return p.Inner.Access(ctx, path, mode)
}
func (p *PassthroughBackend) StatFs(ctx context.Context, path string) (*StatFs, proto.FsError) {
	return p.Inner.StatFs(ctx, path)
}
func (p *PassthroughBackend) GetXAttr(ctx context.Context, path, attr string) ([]byte, proto.FsError) {
	return p.Inner.GetXAttr(ctx, path, attr)
}
func (p *PassthroughBackend) SetXAttr(ctx context.Context, path, attr string, data []byte, flags uint32) proto.FsError {
	return p.Inner.SetXAttr(ctx, path, attr, data, flags)
}
func (p *PassthroughBackend) RemoveXAttr(ctx context.Context, path, attr string) proto.FsError {
	return p.Inner.RemoveXAttr(ctx, path, attr)
}
func (p *PassthroughBackend) ListXAttr(ctx context.Context, path string) ([]string, proto.FsError) {
	return p.Inner.ListXAttr(ctx, path)
}
func (p *PassthroughBackend) Open(ctx context.Context, path string, flags uint32) (FileHandle, proto.FsError) {
	return p.Inner.Open(ctx, path, flags)
}
func (p *PassthroughBackend) Create(ctx context.Context, parent, name string, flags, mode uint32) (FileHandle, *Attr, proto.FsError) {
	return p.Inner.Create(ctx, parent, name, flags, mode)
}
func (p *PassthroughBackend) Read(ctx context.Context, fh FileHandle, off int64, dest []byte) (int, proto.FsError) {
	return p.Inner.Read(ctx, fh, off, dest)
}
func (p *PassthroughBackend) Write(ctx context.Context, fh FileHandle, off int64, data []byte) (uint32, proto.FsError) {
	return p.Inner.Write(ctx, fh, off, data)
}
func (p *PassthroughBackend) Release(ctx context.Context, fh FileHandle) proto.FsError {
	return p.Inner.Release(ctx, fh)
}
func (p *PassthroughBackend) Flush(ctx context.Context, fh FileHandle) proto.FsError {
	return p.Inner.Flush(ctx, fh)
}
func (p *PassthroughBackend) Fsync(ctx context.Context, fh FileHandle, flags int64) proto.FsError {
	return p.Inner.Fsync(ctx, fh, flags)
}
func (p *PassthroughBackend) Allocate(ctx context.Context, fh FileHandle, off, size uint64, mode uint32) proto.FsError {
	return p.Inner.Allocate(ctx, fh, off, size, mode)
}
func (p *PassthroughBackend) GetLk(ctx context.Context, fh FileHandle, owner uint64, lk *fuse.FileLock, flags uint32, out *fuse.FileLock) proto.FsError {
	return p.Inner.GetLk(ctx, fh, owner, lk, flags, out)
}
func (p *PassthroughBackend) SetLk(ctx context.Context, fh FileHandle, owner uint64, lk *fuse.FileLock, flags uint32) proto.FsError {
	return p.Inner.SetLk(ctx, fh, owner, lk, flags)
}
func (p *PassthroughBackend) SetLkw(ctx context.Context, fh FileHandle, owner uint64, lk *fuse.FileLock, flags uint32) proto.FsError {
	return p.Inner.SetLkw(ctx, fh, owner, lk, flags)
}
func (p *PassthroughBackend) CopyFileRange(ctx context.Context, fhIn FileHandle, offIn uint64, fhOut FileHandle, offOut uint64, length, flags uint64) (uint64, proto.FsError) {
	return p.Inner.CopyFileRange(ctx, fhIn, offIn, fhOut, offOut, length, flags)
}
func (p *PassthroughBackend) Lseek(ctx context.Context, fh FileHandle, offset uint64, whence uint32) (uint64, proto.FsError) {
	return p.Inner.Lseek(ctx, fh, offset, whence)
}
func (p *PassthroughBackend) Mkdir(ctx context.Context, path string, mode uint32) (*Attr, proto.FsError) {
	return p.Inner.Mkdir(ctx, path, mode)
}
func (p *PassthroughBackend) Rmdir(ctx context.Context, path string) proto.FsError {
	return p.Inner.Rmdir(ctx, path)
}
func (p *PassthroughBackend) Unlink(ctx context.Context, path string) proto.FsError {
	return p.Inner.Unlink(ctx, path)
}
func (p *PassthroughBackend) Rename(ctx context.Context, oldPath, newPath string) proto.FsError {
	return p.Inner.Rename(ctx, oldPath, newPath)
}
func (p *PassthroughBackend) Readlink(ctx context.Context, path string) (string, proto.FsError) {
	return p.Inner.Readlink(ctx, path)
}
func (p *PassthroughBackend) Symlink(ctx context.Context, target, linkPath string) (*Attr, proto.FsError) {
	return p.Inner.Symlink(ctx, target, linkPath)
}
func (p *PassthroughBackend) SetAttr(ctx context.Context, path string, in SetAttrIn) (*Attr, proto.FsError) {
	return p.Inner.SetAttr(ctx, path, in)
}
func (p *PassthroughBackend) Close() error { return p.Inner.Close() }
