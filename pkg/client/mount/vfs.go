package mount

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"time"

	"gmountie/pkg/client/cache"
	"gmountie/pkg/client/cache/persist"
	"gmountie/pkg/client/config"
	"gmountie/pkg/client/grpc"
	"gmountie/pkg/client/io"
	"gmountie/pkg/utils/log"

	gofs "github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"
	pkgerrors "github.com/pkg/errors"
	"github.com/puzpuzpuz/xsync/v3"
	"go.uber.org/zap"
)

type VFSVolumeMounter interface {
	Start() error
	Mount(volumeName string) error
	Mounter
}

// vfsRoot is the in-memory parent inode of all volume subdirs. It has
// no methods of its own: the go-fuse fs framework drives Lookup/Readdir
// directly off the persistent children added via AddChild. Each child
// is a gMountieRoot wrapping a BackendClient for that volume.
type vfsRoot struct {
	gofs.Inode
}

// VFSVolumeMounterImpl is the interface for the root filesystem that supports multiple volumes
// It mounts an in-memory parent inode then attaches per-volume subdirs
// as persistent children.
type VFSVolumeMounterImpl struct {
	// path is the path where the in-memory root will be mounted
	path string
	// root is the in-memory parent inode (children are per-volume gMountieRoots)
	root *vfsRoot
	// server is the FUSE server for the mounted parent
	server *fuse.Server
	// volumes tracks the persistent child inode for each mounted volume
	volumes *xsync.MapOf[string, *gofs.Inode]
	// backends tracks the FileSystemBackend for each mounted volume so Close
	// can be called on Unmount to stop subscriber goroutines.
	backends *xsync.MapOf[string, io.FileSystemBackend]
	// persists tracks the open persist handle for each mounted volume;
	// only populated when cache.Enabled is true.
	persists *xsync.MapOf[string, *persist.Persist]
	// client is the grpc client
	client grpc.Client
	// fuse is the FUSE-kernel-side tuning config.
	fuse *config.FUSEConfig
	// cache is the client-side cache config; when cache.Enabled is true,
	// each per-volume backend is wrapped in cache.NewCachedBackend.
	cache config.CacheConfig
	// initialized is a flag to check if the mounter is initialized
	initialized bool
}

// NewMultiVolumeMounter creates a new VFSVolumeMounterImpl. fuseCfg must
// be non-nil; the client config layer guarantees this by treating FUSE
// as a required block with defaults. cacheCfg is consumed by value and
// only applied (per-volume) when cacheCfg.Enabled is true.
func NewMultiVolumeMounter(client grpc.Client, path string, fuseCfg *config.FUSEConfig, cacheCfg config.CacheConfig) VFSVolumeMounter {
	m := &VFSVolumeMounterImpl{
		path:        path,
		volumes:     xsync.NewMapOf[string, *gofs.Inode](),
		backends:    xsync.NewMapOf[string, io.FileSystemBackend](),
		persists:    xsync.NewMapOf[string, *persist.Persist](),
		client:      client,
		fuse:        fuseCfg,
		cache:       cacheCfg,
		initialized: false,
	}
	return m
}

// Start starts the VFSVolumeMounter
func (m *VFSVolumeMounterImpl) Start() error {
	if !m.initialized {
		if err := m.mountMemFS(m.path); err != nil {
			return err
		}
		m.initialized = true
	}
	return nil
}

// Mount mounts the volume to the root filesystem
func (m *VFSVolumeMounterImpl) Mount(volumeName string) error {
	if !m.initialized {
		return errors.New("mounter not started")
	}
	// Check if the volume is already mounted
	if _, ok := m.volumes.Load(volumeName); ok {
		return fmt.Errorf("volume %s is already mounted", volumeName)
	}
	// Build a per-volume BackendClient + gMountieRoot and attach it as
	// a persistent child of the in-memory parent. NewPersistentInode
	// keeps the child alive across kernel forgets so the subtree
	// survives until we explicitly RmChild it.
	var backend io.FileSystemBackend = io.NewBackendClient(m.client, volumeName)
	if m.cache.Enabled {
		root := filepath.Join(m.cache.Path, volumeName)
		p, err := persist.Open(persist.Options{Root: root, DiskMaxBytes: int64(m.cache.DiskMaxBytes)})
		if err != nil {
			return pkgerrors.Wrap(err, "open cache persist")
		}
		m.persists.Store(volumeName, p)
		backend = cache.NewCachedBackend(backend, cache.ConfigFromClient(m.cache), p, m.client.Fs(), volumeName)
	}
	m.backends.Store(volumeName, backend)
	volRoot := io.NewMountieRoot(backend)
	ctx := context.Background()
	parent := &m.root.Inode
	childInode := parent.NewPersistentInode(ctx, volRoot, gofs.StableAttr{Mode: fuse.S_IFDIR})
	if !parent.AddChild(volumeName, childInode, true) {
		childInode.ForgetPersistent()
		log.Log.Error("attaching the volume failed", zap.String("volume", volumeName))
		return fmt.Errorf("attaching the volume failed: %s", volumeName)
	}
	m.volumes.Store(volumeName, childInode)
	log.Log.Info("volume mounted", zap.String("volume", volumeName))
	return nil
}

// Unmount unmounts the volume from the root filesystem
func (m *VFSVolumeMounterImpl) Unmount(volumeName string) error {
	// Check if the volume is already mounted
	childInode, ok := m.volumes.Load(volumeName)
	if !ok {
		return fmt.Errorf("volume %s is not mounted", volumeName)
	}
	parent := &m.root.Inode
	if success, _ := parent.RmChild(volumeName); !success {
		log.Log.Error(
			"unmounting the volume failed",
			zap.String("volume", volumeName),
		)
		return fmt.Errorf("unmounting the volume failed: %s", volumeName)
	}
	childInode.ForgetPersistent()
	m.volumes.Delete(volumeName)
	if be, ok := m.backends.Load(volumeName); ok {
		_ = be.Close()
		m.backends.Delete(volumeName)
	}
	if p, ok := m.persists.Load(volumeName); ok {
		_ = p.Close()
		m.persists.Delete(volumeName)
	}
	log.Log.Info("volume unmounted", zap.String("volume", volumeName))
	return nil
}

// UnmountAll unmounts all volumes from the root filesystem
func (m *VFSVolumeMounterImpl) UnmountAll() error {
	var errs = make([]error, 0)
	m.volumes.Range(func(key string, _ *gofs.Inode) bool {
		if err := m.Unmount(key); err != nil {
			log.Log.Error(
				"unmounting the volume failed",
				zap.String("volume", key),
				zap.Error(err),
			)
			errs = append(errs, err)
			return true
		}
		return true
	})
	return errors.Join(errs...)
}

// Close closes the root filesystem
func (m *VFSVolumeMounterImpl) Close() error {
	if m.server == nil {
		return nil
	}
	if err := stopServer(m.server, m.path); err != nil {
		log.Log.Error("unmount fail, giving up", zap.Error(err))
		return err
	}
	log.Log.Info("root filesystem unmounted", zap.String("path", m.path))
	// Close any persist handles that were not already drained by UnmountAll.
	// The kernel connection is gone at this point, so no in-flight requests
	// can reach the cached backend.
	var persistErrs []error
	m.persists.Range(func(volume string, p *persist.Persist) bool {
		if err := p.Close(); err != nil {
			persistErrs = append(persistErrs, err)
		}
		m.persists.Delete(volume)
		return true
	})
	return errors.Join(persistErrs...)
}

// mountMemFS mounts the in-memory parent inode to the path. gofs.Mount
// handles raw-FS construction, the Serve goroutine, and WaitMount in
// one call.
func (m *VFSVolumeMounterImpl) mountMemFS(path string) error {
	m.root = &vfsRoot{}
	// Negotiate the FUSE frame ceiling with the server up-front. A failed
	// handshake falls back to the configured value — never gate the mount.
	maxWrite := negotiateMaxWriteBytes(m.client, m.fuse)
	mountOpts := createMountOptions(m.client.GetEndpoint(), "", m.fuse, maxWrite)
	entryTimeout := time.Second
	attrTimeout := time.Second
	fsOpts := &gofs.Options{
		MountOptions: *mountOpts,
		EntryTimeout: &entryTimeout,
		AttrTimeout:  &attrTimeout,
	}
	server, err := gofs.Mount(path, m.root, fsOpts)
	if err != nil {
		log.Log.Error("mounting the root filesystem failed", zap.Error(err))
		return err
	}
	m.server = server
	log.Log.Info("root filesystem mounted", zap.String("path", path))
	return nil
}

func (m *VFSVolumeMounterImpl) IsVolumeMounted(volume string) bool {
	_, ok := m.volumes.Load(volume)
	return ok
}
