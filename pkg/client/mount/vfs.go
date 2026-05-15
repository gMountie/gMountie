package mount

import (
	"errors"
	"fmt"
	"gmountie/pkg/client/config"
	"gmountie/pkg/client/grpc"
	"gmountie/pkg/client/io"
	"gmountie/pkg/utils/log"
	"time"

	"github.com/avast/retry-go/v4"
	"github.com/hanwen/go-fuse/v2/fuse"
	"github.com/hanwen/go-fuse/v2/fuse/nodefs"
	"github.com/hanwen/go-fuse/v2/fuse/pathfs"
	"github.com/puzpuzpuz/xsync/v3"
	"go.uber.org/zap"
)

type VFSVolumeMounter interface {
	Start() error
	Mount(volumeName string) error
	Mounter
}

// VFSVolumeMounterImpl is the interface for the root filesystem that supports multiple volumes
// It mounts a MemFS then it attaches the volumes to the MemFS
type VFSVolumeMounterImpl struct {
	// path is the path where the MemFS will be mounted
	path string
	// root is the root of the MemFS
	root nodefs.Node
	// connector is the connector of the MemFS
	connector *nodefs.FileSystemConnector
	// server is the server of the MemFS
	server *fuse.Server
	// volumes is the map of volumes mounted
	volumes *xsync.MapOf[string, *pathfs.PathNodeFs]
	// client is the grpc client
	client grpc.Client
	// fuse is the FUSE-kernel-side tuning config.
	fuse *config.FUSEConfig
	// initialized is a flag to check if the mounter is initialized
	initialized bool
}

// NewMultiVolumeMounter creates a new VFSVolumeMounterImpl. fuseCfg must
// be non-nil; the client config layer guarantees this by treating FUSE
// as a required block with defaults.
func NewMultiVolumeMounter(client grpc.Client, path string, fuseCfg *config.FUSEConfig) VFSVolumeMounter {
	m := &VFSVolumeMounterImpl{
		path:        path,
		volumes:     xsync.NewMapOf[string, *pathfs.PathNodeFs](),
		client:      client,
		fuse:        fuseCfg,
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
	// Create the remote filesystem.
	fs := io.NewLocalFileSystem(m.client, volumeName)
	nFs := pathfs.NewPathNodeFs(fs, createFsOptions())

	// Mount the remote filesystem to the root filesystem
	status := m.connector.Mount(m.root.Inode(), volumeName, nFs.Root(), nil)
	if status != fuse.OK {
		log.Log.Error(
			"mounting the volume failed",
			zap.String("volume", volumeName),
			zap.String("status", status.String()),
		)
		return fmt.Errorf("mounting the volume failed: %s", status.String())
	}
	m.volumes.Store(volumeName, nFs)
	log.Log.Info("volume mounted", zap.String("volume", volumeName))
	return nil
}

// Unmount unmounts the volume from the root filesystem
func (m *VFSVolumeMounterImpl) Unmount(volumeName string) error {
	// Check if the volume is already mounted
	nFS, ok := m.volumes.Load(volumeName)
	if !ok {
		return fmt.Errorf("volume %s is not mounted", volumeName)
	}

	status := m.connector.Unmount(nFS.Root().Inode())
	if status != fuse.OK {
		log.Log.Error(
			"unmounting the volume failed",
			zap.String("volume", volumeName),
			zap.String("status", status.String()),
		)
		return fmt.Errorf("unmounting the volume failed: %s", status.String())
	}
	m.volumes.Delete(volumeName)
	log.Log.Info("volume unmounted", zap.String("volume", volumeName))
	return nil
}

// UnmountAll unmounts all volumes from the root filesystem
func (m *VFSVolumeMounterImpl) UnmountAll() error {
	var errs = make([]error, 0)
	m.volumes.Range(func(key string, value *pathfs.PathNodeFs) bool {
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
	errRetry := retry.Do(
		func() error {
			err := m.server.Unmount()
			if err != nil {
				log.Log.Warn("unmount fail, retrying ...", zap.Error(err))
				return err
			}
			return nil
		},
		retry.Attempts(3),
		retry.Delay(5*time.Second),
	)
	if errRetry != nil {
		log.Log.Error("unmount fail, giving up", zap.Error(errRetry))
		return errRetry
	}
	log.Log.Info("root filesystem unmounted", zap.String("path", m.path))
	return nil
}

// mountMemFS mounts a MemFS to the path
func (m *VFSVolumeMounterImpl) mountMemFS(path string) error {
	// create a new MemFS
	m.root = nodefs.NewDefaultNode()
	// create a new FileSystemConnector
	m.connector = nodefs.NewFileSystemConnector(m.root, createConnectorOptions())
	// Negotiate the FUSE frame ceiling with the server up-front. A failed
	// handshake falls back to the configured value — never gate the mount.
	maxWrite := negotiateMaxWriteBytes(m.client, m.fuse)
	// create a new server
	var err error
	m.server, err = fuse.NewServer(m.connector.RawFS(), path, createMountOptions(m.client.GetEndpoint(), "", m.fuse, maxWrite))
	if err != nil {
		return err
	}
	// start the server
	go m.server.Serve()

	if err = m.server.WaitMount(); err != nil {
		log.Log.Error("mounting the root filesystem failed", zap.Error(err))
		return err
	}
	log.Log.Info("root filesystem mounted", zap.String("path", path))
	return nil
}

func (m *VFSVolumeMounterImpl) IsVolumeMounted(volume string) bool {
	_, ok := m.volumes.Load(volume)
	return ok
}
