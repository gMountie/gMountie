package io

import (
	"context"
	"gmountie/pkg/client/grpc"
	"gmountie/pkg/proto"
	"gmountie/pkg/utils/log"

	"github.com/hanwen/go-fuse/v2/fuse"
	"github.com/hanwen/go-fuse/v2/fuse/nodefs"
	"github.com/hanwen/go-fuse/v2/fuse/pathfs"
	"go.uber.org/zap"
)

type LocalFileSystem struct {
	volume string
	client grpc.Client
	pathfs.FileSystem
}

// NewLocalFileSystem creates a new LocalFileSystem
func NewLocalFileSystem(client grpc.Client, volume string) pathfs.FileSystem {
	return &LocalFileSystem{
		volume:     volume,
		client:     client,
		FileSystem: pathfs.NewDefaultFileSystem(),
	}
}

// OnMount is called after the file system is mounted
func (fs *LocalFileSystem) OnMount(nodeFs *pathfs.PathNodeFs) {
	log.Log.Debug("file system is mounted", zap.String("volume", fs.volume))
}

// GetAttr returns the attributes of a file.
func (fs *LocalFileSystem) GetAttr(name string, fctx *fuse.Context) (*fuse.Attr, fuse.Status) {
	if fctx == nil {
		// When mounting in another node through the connector, fctx is nil.
		fctx = &fuse.Context{
			Caller: fuse.Caller{Owner: fuse.Owner{Uid: 1000, Gid: 1000}, Pid: 1000},
			Cancel: make(chan struct{}),
		}
	}
	ctx, cancel := withMetaTimeout(fctx, fs.client.MetaTimeout())
	defer cancel()
	res, err := retryableCall(ctx, "GetAttr", func(ctx context.Context) (*proto.GetAttrReply, error) {
		return fs.client.Fs().GetAttr(ctx, &proto.GetAttrRequest{
			Volume: fs.volume,
			Caller: createCaller(fctx),
			Path:   name,
		})
	})
	if err != nil {
		log.Log.Error("error in call: GetAttr", zap.String("path", name), zap.Error(err))
		return &fuse.Attr{}, fuse.EIO
	}
	if res.GetAttributes() == nil {
		return &fuse.Attr{}, fuse.Status(res.Status)
	}
	// We ignore Ino to force the client to generate its own ino numbers
	a := &fuse.Attr{
		Ino:    res.GetAttributes().Ino,
		Size:   res.GetAttributes().Size,
		Blocks: res.GetAttributes().Blocks,
		Atime:  res.GetAttributes().Atime,
		Mtime:  res.GetAttributes().Mtime,
		Ctime:  res.GetAttributes().Ctime,
		Mode:   res.GetAttributes().Mode,
		Nlink:  res.GetAttributes().Nlink,
		Owner: fuse.Owner{
			Uid: res.GetAttributes().Owner.Uid,
			Gid: res.GetAttributes().Owner.Gid,
		},
		Rdev:    res.GetAttributes().Rdev,
		Blksize: res.GetAttributes().Blksize,
		Padding: res.GetAttributes().Padding,
	}
	return a, fuse.Status(res.Status)
}

// OpenDir opens a directory
func (fs *LocalFileSystem) OpenDir(name string, fctx *fuse.Context) (stream []fuse.DirEntry, code fuse.Status) {
	ctx, cancel := withMetaTimeout(fctx, fs.client.MetaTimeout())
	defer cancel()
	res, err := retryableCall(ctx, "OpenDir", func(ctx context.Context) (*proto.OpenDirReply, error) {
		return fs.client.Fs().OpenDir(ctx, &proto.OpenDirRequest{
			Volume: fs.volume,
			Caller: createCaller(fctx),
			Path:   name,
		})
	})
	if err != nil || res == nil {
		log.Log.Error("error in call: OpenDir", zap.String("path", name), zap.Error(err))
		return nil, fuse.EIO
	}
	var entries []fuse.DirEntry
	for _, entry := range res.Entries {
		entries = append(entries, fuse.DirEntry{
			Mode: entry.Mode,
			Name: entry.Name,
			Ino:  entry.Ino,
			Off:  entry.Off,
		})
	}
	return entries, fuse.Status(res.Status)
}

// Access checks access permissions
func (fs *LocalFileSystem) Access(name string, mode uint32, fctx *fuse.Context) (code fuse.Status) {
	ctx, cancel := withMetaTimeout(fctx, fs.client.MetaTimeout())
	defer cancel()
	res, err := retryableCall(ctx, "Access", func(ctx context.Context) (*proto.AccessReply, error) {
		return fs.client.Fs().Access(ctx, &proto.AccessRequest{
			Volume: fs.volume,
			Caller: createCaller(fctx),
			Path:   name,
			Mode:   mode,
		})
	})
	if err != nil || res == nil {
		log.Log.Error("error in call: Access", zap.String("path", name), zap.Error(err))
		return fuse.EIO
	}
	return fuse.Status(res.Status)
}

// GetXAttr gets an extended attribute
func (fs *LocalFileSystem) GetXAttr(name string, attribute string, fctx *fuse.Context) (data []byte, code fuse.Status) {
	ctx, cancel := withMetaTimeout(fctx, fs.client.MetaTimeout())
	defer cancel()
	res, err := retryableCall(ctx, "GetXAttr", func(ctx context.Context) (*proto.GetXAttrReply, error) {
		return fs.client.Fs().GetXAttr(ctx, &proto.GetXAttrRequest{
			Volume: fs.volume, Caller: createCaller(fctx), Path: name, Attribute: attribute,
		})
	})
	if err != nil || res == nil {
		log.Log.Error("error in call: GetXAttr", zap.String("path", name), zap.Error(err))
		return nil, fuse.EIO
	}
	return res.Data, fuse.Status(res.Status)
}

func (fs *LocalFileSystem) StatFs(name string) *fuse.StatfsOut {
	ctx, cancel := withMetaTimeout(context.Background(), fs.client.MetaTimeout())
	defer cancel()
	res, err := retryableCall(ctx, "StatFs", func(ctx context.Context) (*proto.StatFsReply, error) {
		return fs.client.Fs().StatFs(ctx, &proto.StatFsRequest{Volume: fs.volume, Path: name})
	})
	if err != nil || res == nil {
		log.Log.Error("error in call: StatFs", zap.String("path", name), zap.Error(err))
		return nil
	}
	stats := &fuse.StatfsOut{
		Blocks:  res.Blocks,
		Bfree:   res.Bfree,
		Bavail:  res.Bavail,
		Files:   res.Files,
		Ffree:   res.Ffree,
		Bsize:   res.Bsize,
		NameLen: res.Namelen,
		Frsize:  res.Frsize,
	}
	if len(res.Spare) == 6 {
		stats.Spare = [6]uint32{res.Spare[0], res.Spare[1], res.Spare[2], res.Spare[3], res.Spare[4], res.Spare[5]}
	}
	return stats
}

// Mkdir creates a directory
func (fs *LocalFileSystem) Mkdir(name string, mode uint32, fctx *fuse.Context) fuse.Status {
	ctx, cancel := withMetaTimeout(fctx, fs.client.MetaTimeout())
	defer cancel()
	res, err := fs.client.Fs().Mkdir(ctx, &proto.MkdirRequest{
		Volume:    fs.volume,
		Caller:    createCaller(fctx),
		Path:      name,
		Mode:      mode,
		SessionId: fs.client.SessionID(),
	})
	if err != nil || res == nil {
		log.Log.Error("error in call: MkDir", zap.String("path", name), zap.Error(err))
		return fuse.EIO
	}
	return fuse.Status(res.Status)
}

// Rmdir removes a directory
func (fs *LocalFileSystem) Rmdir(name string, fctx *fuse.Context) (code fuse.Status) {
	ctx, cancel := withMetaTimeout(fctx, fs.client.MetaTimeout())
	defer cancel()
	res, err := fs.client.Fs().Rmdir(ctx, &proto.RmdirRequest{
		Volume:    fs.volume,
		Caller:    createCaller(fctx),
		Path:      name,
		SessionId: fs.client.SessionID(),
	})
	if err != nil || res == nil {
		log.Log.Error("error in call: RmDir", zap.String("path", name), zap.Error(err))
		return fuse.EIO
	}
	return fuse.Status(res.Status)
}

// Rename renames a file
func (fs *LocalFileSystem) Rename(oldName string, newName string, fctx *fuse.Context) (code fuse.Status) {
	ctx, cancel := withMetaTimeout(fctx, fs.client.MetaTimeout())
	defer cancel()
	res, err := fs.client.Fs().Rename(ctx, &proto.RenameRequest{
		Volume:    fs.volume,
		Caller:    createCaller(fctx),
		OldName:   oldName,
		NewName:   newName,
		SessionId: fs.client.SessionID(),
	})
	if err != nil || res == nil {
		log.Log.Error("error in call: Rename", zap.String("oldName", oldName), zap.String("newName", newName), zap.Error(err))
		return fuse.EIO
	}
	return fuse.Status(res.Status)
}

func (fs *LocalFileSystem) Open(name string, flags uint32, fctx *fuse.Context) (file nodefs.File, code fuse.Status) {
	ctx, cancel := withMetaTimeout(fctx, fs.client.MetaTimeout())
	defer cancel()
	res, err := fs.client.File().Open(ctx, &proto.OpenRequest{
		Volume:    fs.volume,
		Caller:    createCaller(fctx),
		Path:      name,
		Flags:     flags,
		SessionId: fs.client.SessionID(),
	})
	if err != nil || res == nil {
		log.Log.Error("error in call: Open", zap.String("path", name), zap.Error(err))
		return nil, fuse.EIO
	}
	if fuse.Status(res.Status) != fuse.OK {
		return nil, fuse.Status(res.Status)
	}
	return NewGrpcFile(fs.client.File(), fs.volume, name, res.Fd, fs.client.IOTimeout(), fs.client.SessionID()), fuse.Status(res.Status)
}

func (fs *LocalFileSystem) Create(name string, flags uint32, mode uint32, fctx *fuse.Context) (file nodefs.File, code fuse.Status) {
	ctx, cancel := withMetaTimeout(fctx, fs.client.MetaTimeout())
	defer cancel()
	res, err := fs.client.File().Create(ctx, &proto.CreateRequest{
		Volume:    fs.volume,
		Caller:    createCaller(fctx),
		Path:      name,
		Flags:     flags,
		Mode:      mode,
		SessionId: fs.client.SessionID(),
	})
	if err != nil || res == nil {
		log.Log.Error("error in call: Create", zap.String("path", name), zap.Error(err))
		return nil, fuse.EIO
	}
	if fuse.Status(res.Status) != fuse.OK {
		return nil, fuse.Status(res.Status)
	}
	return NewGrpcFile(fs.client.File(), fs.volume, name, res.Fd, fs.client.IOTimeout(), fs.client.SessionID()), fuse.Status(res.Status)
}

func (fs *LocalFileSystem) Unlink(name string, fctx *fuse.Context) (code fuse.Status) {
	ctx, cancel := withMetaTimeout(fctx, fs.client.MetaTimeout())
	defer cancel()
	res, err := fs.client.Fs().Unlink(ctx, &proto.UnlinkRequest{
		Volume:    fs.volume,
		Caller:    createCaller(fctx),
		Path:      name,
		SessionId: fs.client.SessionID(),
	})
	if err != nil || res == nil {
		log.Log.Error("error in call: Unlink", zap.String("path", name), zap.Error(err))
		return fuse.EIO
	}
	return fuse.Status(res.Status)
}

// Truncate truncates a file
func (fs *LocalFileSystem) Truncate(name string, size uint64, fctx *fuse.Context) (code fuse.Status) {
	ctx, cancel := withMetaTimeout(fctx, fs.client.MetaTimeout())
	defer cancel()
	res, err := fs.client.Fs().Truncate(ctx, &proto.TruncateRequest{
		Volume:    fs.volume,
		Caller:    createCaller(fctx),
		Path:      name,
		Size:      size,
		SessionId: fs.client.SessionID(),
	})
	if err != nil || res == nil {
		log.Log.Error("error in call: Truncate", zap.String("path", name), zap.Error(err))
		return fuse.EIO
	}
	return fuse.Status(res.Status)
}

// Chmod changes the mode of a file
func (fs *LocalFileSystem) Chmod(name string, mode uint32, fctx *fuse.Context) (code fuse.Status) {
	ctx, cancel := withMetaTimeout(fctx, fs.client.MetaTimeout())
	defer cancel()
	res, err := fs.client.Fs().Chmod(ctx, &proto.ChmodRequest{
		Volume:    fs.volume,
		Caller:    createCaller(fctx),
		Path:      name,
		Mode:      mode,
		SessionId: fs.client.SessionID(),
	})
	if err != nil || res == nil {
		log.Log.Error("error in call: Chmod", zap.String("path", name), zap.Error(err))
		return fuse.EIO
	}
	return fuse.Status(res.Status)
}

// Chown changes the owner of a file
func (fs *LocalFileSystem) Chown(name string, uid uint32, gid uint32, fctx *fuse.Context) (code fuse.Status) {
	ctx, cancel := withMetaTimeout(fctx, fs.client.MetaTimeout())
	defer cancel()
	res, err := fs.client.Fs().Chown(ctx, &proto.ChownRequest{
		Volume:    fs.volume,
		Caller:    createCaller(fctx),
		Path:      name,
		Uid:       uid,
		Gid:       gid,
		SessionId: fs.client.SessionID(),
	})
	if err != nil || res == nil {
		log.Log.Error("error in call: Chown", zap.String("path", name), zap.Error(err))
		return fuse.EIO
	}
	return fuse.Status(res.Status)
}

// createCaller creates a caller for the file system
func createCaller(ctx *fuse.Context) *proto.Caller {
	return &proto.Caller{
		Owner: &proto.Owner{
			Uid: ctx.Owner.Uid,
			Gid: ctx.Owner.Gid,
		},
		Pid: ctx.Pid,
	}
}
