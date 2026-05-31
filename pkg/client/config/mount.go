package config

import (
	"fmt"

	"github.com/spf13/viper"
)

type MountType string

const (
	// MountTypeSingle is a single mount type
	MountTypeSingle = MountType("single")
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

// --- Factory functions ---

// NewMountConfig creates a new MountConfig from a viper config
func NewMountConfig(v *viper.Viper) (MountConfig, error) {
	switch v.GetString("type") {
	case string(MountTypeSingle):
		return NewSingleMountConfig(v)
	default:
		return nil, fmt.Errorf("invalid mount type: %s", v.GetString("type"))
	}
}
