package mount

import (
	"gmountie/pkg/client/grpc"
	"gmountie/pkg/client/io"
	"gmountie/pkg/utils/log"

	"github.com/hanwen/go-fuse/v2/fuse"
	"github.com/hanwen/go-fuse/v2/fuse/nodefs"
	"github.com/hanwen/go-fuse/v2/fuse/pathfs"
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
	client grpc.Client
	mounts *xsync.MapOf[string, *fuse.Server]
}

// NewSingleVolumeMounter creates a new SingleVolumeMounterImpl
func NewSingleVolumeMounter(client grpc.Client) SingleVolumeMounter {
	return &SingleVolumeMounterImpl{
		client: client,
		mounts: xsync.NewMapOf[string, *fuse.Server](),
	}
}

// Mount mounts a volume
func (m *SingleVolumeMounterImpl) Mount(volume, path string) error {
	// Check if the volume is already mounted
	if m.IsVolumeMounted(volume) {
		return errors.Errorf("volume %s is already mounted", volume)
	}

	fs := io.NewLocalFileSystem(m.client, volume)
	nodeFS := pathfs.NewPathNodeFs(fs, createFsOptions())
	connector := nodefs.NewFileSystemConnector(nodeFS.Root(), createConnectorOptions())
	server, err := fuse.NewServer(connector.RawFS(), path, createMountOptions(m.client.GetEndpoint(), volume))
	if err != nil {
		return errors.Wrap(err, "mount fail")
	}

	// Create the mount
	go server.Serve()
	err = server.WaitMount()
	if err != nil {
		return err
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
