package delegation

import (
	"context"

	"go.gmountie.dev/gmountie/pkg/client/backend"
	"go.gmountie.dev/gmountie/pkg/proto"
)

// layer is the posWritePath delegation layer. It records every mutating
// operation's path into the Manager's write-set so the Manager can compute
// an LCA delegation root to piggyback on the next RPC. All durability still
// lives in the transport leaf; this layer only tracks and records.
type layer struct {
	backend.PassthroughBackend
	inner backend.FileSystemBackend
	mgr   *Manager
}

// Compile-time assertion: layer satisfies the full FileSystemBackend surface.
var _ backend.FileSystemBackend = (*layer)(nil)

// NewLayer returns the posWritePath delegation layer. It records the write-set
// on mutating ops (so the Manager can pick a delegation root) and forces a
// cross-subtree rename down the synchronous path (it cannot be covered by a
// single delegation). All durability still happens in the transport leaf.
func NewLayer(inner backend.FileSystemBackend, m *Manager) backend.FileSystemBackend {
	return &layer{PassthroughBackend: backend.PassthroughBackend{Inner: inner}, inner: inner, mgr: m}
}

// joinPath joins parent and name with a "/" separator. An empty parent
// returns name unchanged, matching the transport helper's semantics.
func joinPath(parent, name string) string {
	if parent == "" {
		return name
	}
	return parent + "/" + name
}

func (l *layer) Create(ctx context.Context, parent, name string, flags, mode uint32) (backend.FileHandle, *backend.Attr, proto.FsError) {
	l.mgr.Record(joinPath(parent, name))
	return l.inner.Create(ctx, parent, name, flags, mode)
}

func (l *layer) Mkdir(ctx context.Context, path string, mode uint32) (*backend.Attr, proto.FsError) {
	l.mgr.Record(path)
	return l.inner.Mkdir(ctx, path, mode)
}

func (l *layer) Write(ctx context.Context, fh backend.FileHandle, off int64, data []byte) (uint32, proto.FsError) {
	l.mgr.Record(fh.Path())
	return l.inner.Write(ctx, fh, off, data)
}

func (l *layer) SetAttr(ctx context.Context, path string, in backend.SetAttrIn) (*backend.Attr, proto.FsError) {
	l.mgr.Record(path)
	return l.inner.SetAttr(ctx, path, in)
}

func (l *layer) Symlink(ctx context.Context, target, linkPath string) (*backend.Attr, proto.FsError) {
	// Record linkPath (the created path), not target (verbatim link contents).
	l.mgr.Record(linkPath)
	return l.inner.Symlink(ctx, target, linkPath)
}

func (l *layer) Unlink(ctx context.Context, path string) proto.FsError {
	l.mgr.Record(path)
	return l.inner.Unlink(ctx, path)
}

func (l *layer) Rmdir(ctx context.Context, path string) proto.FsError {
	l.mgr.Record(path)
	return l.inner.Rmdir(ctx, path)
}

func (l *layer) SetXAttr(ctx context.Context, path, attr string, data []byte, flags uint32) proto.FsError {
	l.mgr.Record(path)
	return l.inner.SetXAttr(ctx, path, attr, data, flags)
}

func (l *layer) RemoveXAttr(ctx context.Context, path, attr string) proto.FsError {
	l.mgr.Record(path)
	return l.inner.RemoveXAttr(ctx, path, attr)
}

func (l *layer) Rename(ctx context.Context, oldPath, newPath string) proto.FsError {
	// Cross-subtree rename can't be covered by one delegation: record both ends
	// so the write-set may promote, but otherwise just delegate (the server
	// arbitrates both paths and recalls as needed — Task 6).
	l.mgr.Record(oldPath)
	l.mgr.Record(newPath)
	return l.inner.Rename(ctx, oldPath, newPath)
}

func (l *layer) Close() error {
	l.mgr.Close()
	return l.inner.Close()
}
