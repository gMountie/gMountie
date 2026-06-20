//go:build darwin || cgofuse

package cgofs

import (
	"context"
	"strings"
	"time"

	"github.com/hanwen/go-fuse/v2/fuse"
	cgofuse "github.com/winfsp/cgofuse/fuse"
	gio "go.gmountie.dev/gmountie/pkg/client/io"
)

// MountieCgoFS adapts io.FileSystemBackend to cgofuse's FileSystemInterface.
// It is the macOS/Windows sibling of the go-fuse gMountieNode and delegates
// every op to the same backend. backend + rewriter are set at construction
// and shared for the mount's lifetime; the handle table maps cgofuse's uint64
// handles to io.FileHandle objects.
type MountieCgoFS struct {
	cgofuse.FileSystemBase
	backend     gio.FileSystemBackend
	rewriter    *gio.IDRewriter
	handles     *handleTable
	metaTimeout time.Duration
}

// New builds an adapter over backend. rw may be nil (raw_ids / no rewrite).
// metaTimeout bounds metadata ops (mirrors the client MetaTimeout).
func New(backend gio.FileSystemBackend, rw *gio.IDRewriter, metaTimeout time.Duration) *MountieCgoFS {
	return &MountieCgoFS{
		backend:     backend,
		rewriter:    rw,
		handles:     newHandleTable(),
		metaTimeout: metaTimeout,
	}
}

// clean normalizes a cgofuse path (always absolute, leading "/") to the
// backend's path convention (relative to mount root, no leading slash). The
// root "/" becomes "".
func clean(p string) string { return strings.TrimPrefix(p, "/") }

// opCtx builds a per-op context carrying the kernel caller (uid/gid/pid from
// cgofuse) so the gRPC backend stamps proto.Caller correctly, with a timeout.
func (fs *MountieCgoFS) opCtx() (context.Context, context.CancelFunc) {
	uid, gid, pid := cgofuse.Getcontext()
	ctx := gio.WithCaller(context.Background(), uid, gid, uint32(pid))
	return context.WithTimeout(ctx, fs.metaTimeout)
}

func (fs *MountieCgoFS) Getattr(path string, stat *cgofuse.Stat_t, fh uint64) int {
	ctx, cancel := fs.opCtx()
	defer cancel()
	a, st := fs.backend.Stat(ctx, clean(path))
	if !st.Ok() {
		return errc(st)
	}
	fillStat(stat, a, fs.rewriter)
	return 0
}

func (fs *MountieCgoFS) Readdir(path string, fill func(name string, stat *cgofuse.Stat_t, ofst int64) bool, ofst int64, fh uint64) int {
	ctx, cancel := fs.opCtx()
	defer cancel()
	entries, st := fs.backend.ListDir(ctx, clean(path))
	if !st.Ok() {
		return errc(st)
	}
	// "." and ".." are not returned by the backend; cgofuse expects them.
	fill(".", nil, 0)
	fill("..", nil, 0)
	for _, e := range entries {
		var stat cgofuse.Stat_t
		stat.Mode = e.Mode
		stat.Ino = e.Ino
		if !fill(e.Name, &stat, 0) {
			break
		}
	}
	return 0
}

func (fs *MountieCgoFS) Readlink(path string) (int, string) {
	ctx, cancel := fs.opCtx()
	defer cancel()
	target, st := fs.backend.Readlink(ctx, clean(path))
	if !st.Ok() {
		return errc(st), ""
	}
	return 0, target
}

func (fs *MountieCgoFS) Access(path string, mask uint32) int {
	ctx, cancel := fs.opCtx()
	defer cancel()
	return errc(fs.backend.Access(ctx, clean(path), mask))
}

func (fs *MountieCgoFS) Statfs(path string, stat *cgofuse.Statfs_t) int {
	ctx, cancel := fs.opCtx()
	defer cancel()
	s, st := fs.backend.StatFs(ctx, clean(path))
	if !st.Ok() {
		return errc(st)
	}
	fillStatfs(stat, s)
	return 0
}

// Opendir is a no-op success: directory reads go through Readdir(path,...)
// directly; we keep no per-dir handle.
func (fs *MountieCgoFS) Opendir(path string) (int, uint64) { return 0, 0 }

// Releasedir is a no-op success (no per-dir handle to release).
func (fs *MountieCgoFS) Releasedir(path string, fh uint64) int { return 0 }

// splitPath splits a cleaned path into (parent, name) for Create. "f" -> ("","f").
func splitPath(p string) (parent, name string) {
	i := strings.LastIndex(p, "/")
	if i < 0 {
		return "", p
	}
	return p[:i], p[i+1:]
}

func (fs *MountieCgoFS) Open(path string, flags int) (int, uint64) {
	ctx, cancel := fs.opCtx()
	defer cancel()
	fh, st := fs.backend.Open(ctx, clean(path), uint32(flags))
	if !st.Ok() {
		return errc(st), ^uint64(0)
	}
	return 0, fs.handles.add(fh)
}

func (fs *MountieCgoFS) Create(path string, flags int, mode uint32) (int, uint64) {
	ctx, cancel := fs.opCtx()
	defer cancel()
	parent, name := splitPath(clean(path))
	fh, _, st := fs.backend.Create(ctx, parent, name, uint32(flags), mode)
	if !st.Ok() {
		return errc(st), ^uint64(0)
	}
	return 0, fs.handles.add(fh)
}

func (fs *MountieCgoFS) Read(path string, buff []byte, ofst int64, fh uint64) int {
	h, ok := fs.handles.get(fh)
	if !ok {
		return -int(fuse.EBADF)
	}
	ctx, cancel := fs.opCtx()
	defer cancel()
	n, st := fs.backend.Read(ctx, h, ofst, buff)
	if !st.Ok() {
		return errc(st)
	}
	return n
}

func (fs *MountieCgoFS) Write(path string, buff []byte, ofst int64, fh uint64) int {
	h, ok := fs.handles.get(fh)
	if !ok {
		return -int(fuse.EBADF)
	}
	ctx, cancel := fs.opCtx()
	defer cancel()
	n, st := fs.backend.Write(ctx, h, ofst, buff)
	if !st.Ok() {
		return errc(st)
	}
	return int(n)
}

func (fs *MountieCgoFS) Flush(path string, fh uint64) int {
	h, ok := fs.handles.get(fh)
	if !ok {
		return -int(fuse.EBADF)
	}
	ctx, cancel := fs.opCtx()
	defer cancel()
	return errc(fs.backend.Flush(ctx, h))
}

func (fs *MountieCgoFS) Fsync(path string, datasync bool, fh uint64) int {
	h, ok := fs.handles.get(fh)
	if !ok {
		return -int(fuse.EBADF)
	}
	ctx, cancel := fs.opCtx()
	defer cancel()
	var flags int64
	if datasync {
		flags = 1
	}
	return errc(fs.backend.Fsync(ctx, h, flags))
}

func (fs *MountieCgoFS) Release(path string, fh uint64) int {
	h, ok := fs.handles.remove(fh)
	if !ok {
		return -int(fuse.EBADF)
	}
	ctx, cancel := fs.opCtx()
	defer cancel()
	return errc(fs.backend.Release(ctx, h))
}

func (fs *MountieCgoFS) Truncate(path string, size int64, fh uint64) int {
	ctx, cancel := fs.opCtx()
	defer cancel()
	in := gio.SetAttrIn{Valid: uint32(fuse.FATTR_SIZE), Size: uint64(size)}
	_, st := fs.backend.SetAttr(ctx, clean(path), in)
	return errc(st)
}

func (fs *MountieCgoFS) Mkdir(path string, mode uint32) int {
	ctx, cancel := fs.opCtx()
	defer cancel()
	_, st := fs.backend.Mkdir(ctx, clean(path), mode)
	return errc(st)
}

func (fs *MountieCgoFS) Rmdir(path string) int {
	ctx, cancel := fs.opCtx()
	defer cancel()
	return errc(fs.backend.Rmdir(ctx, clean(path)))
}

func (fs *MountieCgoFS) Unlink(path string) int {
	ctx, cancel := fs.opCtx()
	defer cancel()
	return errc(fs.backend.Unlink(ctx, clean(path)))
}

func (fs *MountieCgoFS) Rename(oldpath string, newpath string) int {
	ctx, cancel := fs.opCtx()
	defer cancel()
	return errc(fs.backend.Rename(ctx, clean(oldpath), clean(newpath)))
}

func (fs *MountieCgoFS) Symlink(target string, newpath string) int {
	ctx, cancel := fs.opCtx()
	defer cancel()
	_, st := fs.backend.Symlink(ctx, target, clean(newpath))
	return errc(st)
}

func (fs *MountieCgoFS) Chmod(path string, mode uint32) int {
	ctx, cancel := fs.opCtx()
	defer cancel()
	in := gio.SetAttrIn{Valid: uint32(fuse.FATTR_MODE), Mode: mode}
	_, st := fs.backend.SetAttr(ctx, clean(path), in)
	return errc(st)
}

func (fs *MountieCgoFS) Chown(path string, uid uint32, gid uint32) int {
	ctx, cancel := fs.opCtx()
	defer cancel()
	suid, sgid := fs.rewriter.Outbound(uid, gid)
	in := gio.SetAttrIn{Valid: uint32(fuse.FATTR_UID | fuse.FATTR_GID), Uid: suid, Gid: sgid}
	_, st := fs.backend.SetAttr(ctx, clean(path), in)
	return errc(st)
}

func (fs *MountieCgoFS) Utimens(path string, tmsp []cgofuse.Timespec) int {
	ctx, cancel := fs.opCtx()
	defer cancel()
	in := gio.SetAttrIn{Valid: uint32(fuse.FATTR_ATIME | fuse.FATTR_MTIME)}
	if len(tmsp) >= 2 {
		at := tmsp[0].Time()
		mt := tmsp[1].Time()
		in.Atime = &at
		in.Mtime = &mt
	}
	_, st := fs.backend.SetAttr(ctx, clean(path), in)
	return errc(st)
}
