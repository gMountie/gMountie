package mount

import (
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
	"github.com/pkg/errors"
	"github.com/puzpuzpuz/xsync/v3"
	"go.uber.org/zap"
)

// SingleVolumeMounter is the interface for the mounter that supports a single volume
type SingleVolumeMounter interface {
	Mount(volumeName, path string) error
	Mounter
}

// SingleVolumeMounterImpl is a service that mounts volumes
type SingleVolumeMounterImpl struct {
	client   grpc.Client
	fuse     *config.FUSEConfig
	cache    config.CacheConfig
	mounts   *xsync.MapOf[string, *fuse.Server]
	persists *xsync.MapOf[string, *persist.Persist]
	backends *xsync.MapOf[string, io.FileSystemBackend]
}

// NewSingleVolumeMounter creates a new SingleVolumeMounterImpl. fuseCfg
// must be non-nil; the client config layer guarantees this by treating
// FUSE as a required block with defaults. cacheCfg is consumed by value
// and only applied when cacheCfg.Enabled is true.
func NewSingleVolumeMounter(client grpc.Client, fuseCfg *config.FUSEConfig, cacheCfg config.CacheConfig) SingleVolumeMounter {
	return &SingleVolumeMounterImpl{
		client:   client,
		fuse:     fuseCfg,
		cache:    cacheCfg,
		mounts:   xsync.NewMapOf[string, *fuse.Server](),
		persists: xsync.NewMapOf[string, *persist.Persist](),
		backends: xsync.NewMapOf[string, io.FileSystemBackend](),
	}
}

// Mount mounts a volume
func (m *SingleVolumeMounterImpl) Mount(volume, mountPath string) error {
	// Check if the volume is already mounted
	if m.IsVolumeMounted(volume) {
		return errors.Errorf("volume %s is already mounted", volume)
	}

	maxWrite := negotiateMaxWriteBytes(m.client, m.fuse)

	var backend io.FileSystemBackend = io.NewBackendClient(m.client, volume)
	if m.cache.Enabled {
		root := filepath.Join(m.cache.Path, volume)
		p, err := persist.Open(persist.Options{Root: root, DiskMaxBytes: int64(m.cache.DiskMaxBytes)})
		if err != nil {
			return errors.Wrap(err, "open cache persist")
		}
		m.persists.Store(volume, p)
		backend = cache.NewCachedBackend(backend, cache.ConfigFromClient(m.cache), p, m.client.Fs(), volume)
	}
	m.backends.Store(volume, backend)
	root := io.NewMountieRoot(backend)
	mountOpts := createMountOptions(m.client.GetEndpoint(), volume, m.fuse, maxWrite)
	entryTimeout := time.Second
	attrTimeout := time.Second
	fsOpts := &gofs.Options{
		MountOptions: *mountOpts,
		EntryTimeout: &entryTimeout,
		AttrTimeout:  &attrTimeout,
	}
	// gofs.Mount is self-contained: it constructs the raw FS via
	// NewNodeFS, spawns the Serve goroutine, and blocks on WaitMount
	// before returning. No explicit go server.Serve()/WaitMount needed.
	server, err := gofs.Mount(mountPath, root, fsOpts)
	if err != nil {
		return errors.Wrap(err, "mount fail")
	}
	m.mounts.Store(volume, server)
	return nil
}

// IsVolumeMounted checks if a volume is mounted
func (m *SingleVolumeMounterImpl) IsVolumeMounted(volume string) bool {
	_, ok := m.mounts.Load(volume)
	return ok
}

// GetMounts returns the mounts
func (m *SingleVolumeMounterImpl) GetMounts() []string {
	mounts := make([]string, 0)
	m.mounts.Range(func(volume string, _ *fuse.Server) bool {
		mounts = append(mounts, volume)
		return true
	})
	return mounts
}

// Unmount unmounts a volume
func (m *SingleVolumeMounterImpl) Unmount(volume string) error {
	server, ok := m.mounts.Load(volume)
	if !ok {
		return errors.Errorf("volume %s is not mounted", volume)
	}
	if err := stopServer(server); err != nil {
		return err
	}
	m.mounts.Delete(volume)
	if be, ok := m.backends.Load(volume); ok {
		_ = be.Close()
		m.backends.Delete(volume)
	}
	if p, ok := m.persists.Load(volume); ok {
		_ = p.Close()
		m.persists.Delete(volume)
	}
	log.Log.Info("unmounted volume", zap.String("volume", volume))
	return nil
}

// UnmountAll unmounts all volumes
func (m *SingleVolumeMounterImpl) UnmountAll() error {
	for _, volume := range m.GetMounts() {
		err := m.Unmount(volume)
		if err != nil {
			return err
		}
	}
	return nil
}

// Close closes the mounter
func (m *SingleVolumeMounterImpl) Close() error {
	return m.UnmountAll()
}
