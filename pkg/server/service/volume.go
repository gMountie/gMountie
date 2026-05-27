package service

import (
	"context"
	"time"

	"gmountie/pkg/common"
	"gmountie/pkg/proto"
	"gmountie/pkg/server/config"
	"gmountie/pkg/server/io"
	"gmountie/pkg/server/principal"

	"github.com/hanwen/go-fuse/v2/fuse/pathfs"
	"github.com/pkg/errors"
)

const defaultIdentityTTL = 60 * time.Second

// VolumeService is a service that manages volumes.
type VolumeService interface {
	// List lists all volumes.
	List() ([]common.Volume, error)
	// GetVolumeFileSystem gets the filesystem for a volume.
	GetVolumeFileSystem(name string) (pathfs.FileSystem, error)
	// BindIdentity resolves the request's identity for a volume and returns a
	// per-request identity-bound filesystem wrapping the volume's loopback.
	BindIdentity(ctx context.Context, volume string, caller *proto.Caller) (pathfs.FileSystem, error)
}

type VolumeServiceOptions func(*VolumeServiceImpl)

func WithMiddleware(middleware ...io.Middleware) VolumeServiceOptions {
	return func(s *VolumeServiceImpl) {
		s.middleware = append(s.middleware, middleware...)
	}
}

// VolumeServiceImpl is an implementation of the VolumeService interface.
type VolumeServiceImpl struct {
	config      *config.Config
	filesystems map[string]pathfs.FileSystem
	middleware  []io.Middleware
	resolvers   map[string]IdentityResolver
	mappings    map[string]config.MappingConfig
}

// NewVolumeService creates a new VolumeService.
func NewVolumeService(cfg *config.Config, options ...VolumeServiceOptions) VolumeService {
	fs := make(map[string]pathfs.FileSystem)
	svc := &VolumeServiceImpl{
		config:      cfg,
		filesystems: fs,
		middleware:  make([]io.Middleware, 0),
		resolvers:   make(map[string]IdentityResolver),
		mappings:    make(map[string]config.MappingConfig),
	}
	for _, option := range options {
		option(svc)
	}
	for _, v := range cfg.Volumes {
		svc.addFileSystem(v.Name, io.NewLocalFilesystem(v.Path))
		svc.mappings[v.Name] = v.Mapping
		switch v.Mapping.Mode {
		case config.MappingModeSquash:
			svc.resolvers[v.Name] = NewSquashResolver(v.Mapping.Uid, v.Mapping.Gid)
		case config.MappingModeStatic:
			svc.resolvers[v.Name] = NewCachedResolver(NewStaticResolver(v.Mapping), defaultIdentityTTL)
		case config.MappingModeSystem:
			svc.resolvers[v.Name] = NewCachedResolver(NewSystemResolver(), defaultIdentityTTL)
		case config.MappingModePassthrough:
			// no resolver; identity derives from the wire caller
		}
	}
	return svc
}

// List lists all volumes.
func (s *VolumeServiceImpl) List() ([]common.Volume, error) {
	volumes := make([]common.Volume, 0)
	for _, v := range s.config.Volumes {
		volumes = append(volumes, common.Volume{Name: v.Name})
	}
	return volumes, nil
}

// GetVolumeFileSystem gets the filesystem for a volume.
func (s *VolumeServiceImpl) GetVolumeFileSystem(name string) (pathfs.FileSystem, error) {
	fs, ok := s.filesystems[name]
	if !ok {
		return nil, errors.Errorf("volume %s not found", name)
	}
	return fs, nil
}

// BindIdentity resolves the request's identity for a volume and returns a
// per-request identity-bound filesystem wrapping the volume's loopback.
func (s *VolumeServiceImpl) BindIdentity(ctx context.Context, volume string, caller *proto.Caller) (pathfs.FileSystem, error) {
	fs, ok := s.filesystems[volume]
	if !ok {
		return nil, errors.Errorf("volume %s not found", volume)
	}
	id, err := s.resolveIdentity(ctx, volume, caller)
	if err != nil {
		return nil, err
	}
	return io.NewIdentityBoundFS(fs, &io.Identity{Uid: id.Uid, Gid: id.Gid, Gids: id.Gids}), nil
}

// resolveIdentity determines the effective identity for a request against a
// volume. Mapped modes resolve the authenticated principal (from ctx) through
// the volume's resolver; passthrough derives the identity from the wire caller.
func (s *VolumeServiceImpl) resolveIdentity(ctx context.Context, volume string, caller *proto.Caller) (Identity, error) {
	m, ok := s.mappings[volume]
	if !ok {
		return Identity{}, errors.Errorf("volume %s not found", volume)
	}
	if m.Mode == config.MappingModePassthrough {
		return passthroughIdentity(m, caller), nil
	}
	if m.Mode == config.MappingModeSquash {
		// Squash maps every caller to one fixed identity, independent of the
		// authenticated principal, so no principal is required.
		return s.resolvers[volume].Resolve("")
	}
	p, ok := principal.FromContext(ctx)
	if !ok {
		return Identity{}, errors.Errorf("no authenticated principal for volume %s (mode %s)", volume, m.Mode)
	}
	return s.resolvers[volume].Resolve(p)
}

// passthroughIdentity derives the identity directly from the wire caller,
// applying root_squash (default on) so an incoming uid 0 maps to AnonUid unless
// no_root_squash is configured.
func passthroughIdentity(m config.MappingConfig, caller *proto.Caller) Identity {
	var uid, gid uint32
	if caller != nil && caller.Owner != nil {
		uid, gid = caller.Owner.Uid, caller.Owner.Gid
	}
	squashRoot := m.RootSquash == nil || *m.RootSquash // default true
	if squashRoot && uid == 0 {
		uid = m.AnonUid
		if gid == 0 {
			gid = m.AnonUid
		}
	}
	return Identity{Uid: uid, Gid: gid, Gids: []uint32{gid}}
}

// addFileSystem adds a filesystem to the volume service.
func (s *VolumeServiceImpl) addFileSystem(name string, fs pathfs.FileSystem) {
	// Apply middleware
	for _, currentMiddleware := range s.middleware {
		fs = currentMiddleware(fs)
	}
	s.filesystems[name] = fs
}
