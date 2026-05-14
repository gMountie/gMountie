package middleware

import (
	"gmountie/pkg/utils/log"
	"runtime"
	"syscall"
	"time"

	"github.com/hanwen/go-fuse/v2/fuse"
	"github.com/hanwen/go-fuse/v2/fuse/nodefs"
	"github.com/hanwen/go-fuse/v2/fuse/pathfs"
	"go.uber.org/zap"
)

// Make syscall functions package variables for testing
var (
	setfsuid = syscall.Setfsuid
	setfsgid = syscall.Setfsgid
)

// changeUser changes the user and group of the current OS thread to those in
// the FUSE context. It returns a cleanup function that restores root, plus an
// error. On error, the OS thread is unlocked before returning (so callers must
// not call the cleanup function).
//
// If the cleanup function itself fails to restore root, it deliberately does
// NOT call runtime.UnlockOSThread — the tainted thread will die with the
// goroutine, which is the safe outcome per runtime.LockOSThread semantics.
func changeUser(context *fuse.Context) (func(), error) {
	userId := context.Owner.Uid
	groupId := context.Owner.Gid
	runtime.LockOSThread()

	if err := setfsuid(int(userId)); err != nil {
		runtime.UnlockOSThread()
		return nil, err
	}
	if err := setfsgid(int(groupId)); err != nil {
		// uid was set; best-effort restore before unlocking
		_ = setfsuid(syscall.Geteuid())
		runtime.UnlockOSThread()
		return nil, err
	}
	return func() {
		if err := setfsuid(syscall.Geteuid()); err != nil {
			log.Log.Error("failed to restore root uid; leaking OS thread", zap.Error(err))
			return // do NOT UnlockOSThread; let the tainted thread die with the goroutine
		}
		if err := setfsgid(syscall.Getegid()); err != nil {
			log.Log.Error("failed to restore root gid; leaking OS thread", zap.Error(err))
			return
		}
		runtime.UnlockOSThread()
	}, nil
}

// AssumeUserMiddleware creates a new assumeUserMiddleware.
func AssumeUserMiddleware(next pathfs.FileSystem) pathfs.FileSystem {
	return &assumeUserMiddleware{
		FileSystem: next,
	}
}

type assumeUserMiddleware struct {
	pathfs.FileSystem
}

func (a *assumeUserMiddleware) GetAttr(name string, context *fuse.Context) (*fuse.Attr, fuse.Status) {
	cleanup, err := changeUser(context)
	if err != nil {
		log.Log.Error("failed to assume user", zap.Error(err))
		return nil, fuse.EPERM
	}
	defer cleanup()
	return a.FileSystem.GetAttr(name, context)
}

func (a *assumeUserMiddleware) Chmod(name string, mode uint32, context *fuse.Context) (code fuse.Status) {
	cleanup, err := changeUser(context)
	if err != nil {
		log.Log.Error("failed to assume user", zap.Error(err))
		return fuse.EPERM
	}
	defer cleanup()
	return a.FileSystem.Chmod(name, mode, context)
}

func (a *assumeUserMiddleware) Chown(name string, uid uint32, gid uint32, context *fuse.Context) (code fuse.Status) {
	cleanup, err := changeUser(context)
	if err != nil {
		log.Log.Error("failed to assume user", zap.Error(err))
		return fuse.EPERM
	}
	defer cleanup()
	return a.FileSystem.Chown(name, uid, gid, context)
}

func (a *assumeUserMiddleware) Utimens(name string, Atime *time.Time, Mtime *time.Time, context *fuse.Context) (code fuse.Status) {
	cleanup, err := changeUser(context)
	if err != nil {
		log.Log.Error("failed to assume user", zap.Error(err))
		return fuse.EPERM
	}
	defer cleanup()
	return a.FileSystem.Utimens(name, Atime, Mtime, context)
}

func (a *assumeUserMiddleware) Truncate(name string, size uint64, context *fuse.Context) (code fuse.Status) {
	cleanup, err := changeUser(context)
	if err != nil {
		log.Log.Error("failed to assume user", zap.Error(err))
		return fuse.EPERM
	}
	defer cleanup()
	return a.FileSystem.Truncate(name, size, context)
}

func (a *assumeUserMiddleware) Access(name string, mode uint32, context *fuse.Context) (code fuse.Status) {
	cleanup, err := changeUser(context)
	if err != nil {
		log.Log.Error("failed to assume user", zap.Error(err))
		return fuse.EPERM
	}
	defer cleanup()
	return a.FileSystem.Access(name, mode, context)
}

func (a *assumeUserMiddleware) Link(oldName string, newName string, context *fuse.Context) (code fuse.Status) {
	cleanup, err := changeUser(context)
	if err != nil {
		log.Log.Error("failed to assume user", zap.Error(err))
		return fuse.EPERM
	}
	defer cleanup()
	return a.FileSystem.Link(oldName, newName, context)
}

func (a *assumeUserMiddleware) Mkdir(name string, mode uint32, context *fuse.Context) fuse.Status {
	cleanup, err := changeUser(context)
	if err != nil {
		log.Log.Error("failed to assume user", zap.Error(err))
		return fuse.EPERM
	}
	defer cleanup()
	return a.FileSystem.Mkdir(name, mode, context)
}

func (a *assumeUserMiddleware) Mknod(name string, mode uint32, dev uint32, context *fuse.Context) fuse.Status {
	cleanup, err := changeUser(context)
	if err != nil {
		log.Log.Error("failed to assume user", zap.Error(err))
		return fuse.EPERM
	}
	defer cleanup()
	return a.FileSystem.Mknod(name, mode, dev, context)
}

func (a *assumeUserMiddleware) Rename(oldName string, newName string, context *fuse.Context) (code fuse.Status) {
	cleanup, err := changeUser(context)
	if err != nil {
		log.Log.Error("failed to assume user", zap.Error(err))
		return fuse.EPERM
	}
	defer cleanup()
	return a.FileSystem.Rename(oldName, newName, context)
}

func (a *assumeUserMiddleware) Rmdir(name string, context *fuse.Context) (code fuse.Status) {
	cleanup, err := changeUser(context)
	if err != nil {
		log.Log.Error("failed to assume user", zap.Error(err))
		return fuse.EPERM
	}
	defer cleanup()
	return a.FileSystem.Rmdir(name, context)
}

func (a *assumeUserMiddleware) Unlink(name string, context *fuse.Context) (code fuse.Status) {
	cleanup, err := changeUser(context)
	if err != nil {
		log.Log.Error("failed to assume user", zap.Error(err))
		return fuse.EPERM
	}
	defer cleanup()
	return a.FileSystem.Unlink(name, context)
}

func (a *assumeUserMiddleware) GetXAttr(name string, attribute string, context *fuse.Context) (data []byte, code fuse.Status) {
	cleanup, err := changeUser(context)
	if err != nil {
		log.Log.Error("failed to assume user", zap.Error(err))
		return nil, fuse.EPERM
	}
	defer cleanup()
	return a.FileSystem.GetXAttr(name, attribute, context)
}

func (a *assumeUserMiddleware) ListXAttr(name string, context *fuse.Context) (attributes []string, code fuse.Status) {
	cleanup, err := changeUser(context)
	if err != nil {
		log.Log.Error("failed to assume user", zap.Error(err))
		return nil, fuse.EPERM
	}
	defer cleanup()
	return a.FileSystem.ListXAttr(name, context)
}

func (a *assumeUserMiddleware) RemoveXAttr(name string, attr string, context *fuse.Context) fuse.Status {
	cleanup, err := changeUser(context)
	if err != nil {
		log.Log.Error("failed to assume user", zap.Error(err))
		return fuse.EPERM
	}
	defer cleanup()
	return a.FileSystem.RemoveXAttr(name, attr, context)
}

func (a *assumeUserMiddleware) SetXAttr(name string, attr string, data []byte, flags int, context *fuse.Context) fuse.Status {
	cleanup, err := changeUser(context)
	if err != nil {
		log.Log.Error("failed to assume user", zap.Error(err))
		return fuse.EPERM
	}
	defer cleanup()
	return a.FileSystem.SetXAttr(name, attr, data, flags, context)
}

func (a *assumeUserMiddleware) Open(name string, flags uint32, context *fuse.Context) (file nodefs.File, code fuse.Status) {
	cleanup, err := changeUser(context)
	if err != nil {
		log.Log.Error("failed to assume user", zap.Error(err))
		return nil, fuse.EPERM
	}
	defer cleanup()
	return a.FileSystem.Open(name, flags, context)
}

func (a *assumeUserMiddleware) Create(name string, flags uint32, mode uint32, context *fuse.Context) (file nodefs.File, code fuse.Status) {
	cleanup, err := changeUser(context)
	if err != nil {
		log.Log.Error("failed to assume user", zap.Error(err))
		return nil, fuse.EPERM
	}
	defer cleanup()
	return a.FileSystem.Create(name, flags, mode, context)
}

func (a *assumeUserMiddleware) OpenDir(name string, context *fuse.Context) (stream []fuse.DirEntry, code fuse.Status) {
	cleanup, err := changeUser(context)
	if err != nil {
		log.Log.Error("failed to assume user", zap.Error(err))
		return nil, fuse.EPERM
	}
	defer cleanup()
	return a.FileSystem.OpenDir(name, context)
}

func (a *assumeUserMiddleware) Symlink(value string, linkName string, context *fuse.Context) (code fuse.Status) {
	cleanup, err := changeUser(context)
	if err != nil {
		log.Log.Error("failed to assume user", zap.Error(err))
		return fuse.EPERM
	}
	defer cleanup()
	return a.FileSystem.Symlink(value, linkName, context)
}

func (a *assumeUserMiddleware) Readlink(name string, context *fuse.Context) (string, fuse.Status) {
	cleanup, err := changeUser(context)
	if err != nil {
		log.Log.Error("failed to assume user", zap.Error(err))
		return "", fuse.EPERM
	}
	defer cleanup()
	return a.FileSystem.Readlink(name, context)
}
