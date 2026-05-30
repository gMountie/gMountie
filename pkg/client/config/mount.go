package config

import (
	"fmt"

	"github.com/spf13/viper"
)

type MountType string

const (
	// MountTypeSingle is a single mount type
	MountTypeSingle = MountType("single")
	// MountTypeVFS is a VFS mount type
	MountTypeVFS = MountType("vfs")
)

// MountConfig is an interface that holds the configuration for a mount
type MountConfig interface {
	GetType() MountType
}

// --- SingleMountConfig ---

// SingleMountConfig is a struct that holds the configuration for a single mount
type SingleMountConfig struct {
	Type   MountType `validate:"required"`
	Path   string    `validate:"required"`
	Volume string    `validate:"required"`
	// RawIDs disables server-to-local UID/GID rewriting. When true the kernel
	// sees the server-side numeric IDs unchanged; useful for backup tools and
	// admin inspection that need to preserve the original ownership.
	RawIDs bool `mapstructure:"raw_ids"`
}

// GetType returns the mount type
func (s *SingleMountConfig) GetType() MountType {
	return MountTypeSingle
}

// NewSingleMountConfig creates a new SingleMountConfig with defaults
func NewSingleMountConfig(v *viper.Viper) (*SingleMountConfig, error) {
	var mount SingleMountConfig
	if err := v.Unmarshal(&mount); err != nil {
		return nil, err
	}
	mount.Type = MountTypeSingle
	return &mount, nil
}

// --- VFSMountConfig ---

// VFSMountConfig is a struct that holds the configuration for a VFS mount
type VFSMountConfig struct {
	Type     MountType `validate:"required"`
	Path     string    `validate:"required"`
	MountAll bool      `mapstructure:"mount_all"`
	Volumes  []string
}

// GetType returns the mount type
func (v *VFSMountConfig) GetType() MountType {
	return MountTypeVFS
}

// NewVFSMountConfig creates a new VFSMountConfig with defaults
func NewVFSMountConfig(v *viper.Viper) (*VFSMountConfig, error) {
	var mount VFSMountConfig

	v.SetDefault("mount_all", false)
	v.SetDefault("volumes", make([]string, 0))

	if err := v.Unmarshal(&mount); err != nil {
		return nil, err
	}
	mount.Type = MountTypeVFS
	return &mount, nil
}

// --- Factory functions ---

// NewMountConfig creates a new MountConfig from a viper config
func NewMountConfig(v *viper.Viper) (MountConfig, error) {
	switch v.GetString("type") {
	case string(MountTypeSingle):
		return NewSingleMountConfig(v)
	case string(MountTypeVFS):
		return NewVFSMountConfig(v)
	default:
		return nil, fmt.Errorf("invalid mount type: %s", v.GetString("type"))
	}
}
