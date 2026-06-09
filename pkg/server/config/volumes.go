package config

import (
	"github.com/pkg/errors"
	"github.com/spf13/viper"
)

type MappingMode string

const (
	MappingModeSquash      MappingMode = "squash"
	MappingModeStatic      MappingMode = "static"
	MappingModeSystem      MappingMode = "system"
	MappingModePassthrough MappingMode = "passthrough"
)

// StaticUser is one principal's identity in a `static` mapping table.
type StaticUser struct {
	Uid    uint32   `mapstructure:"uid"`
	Gid    uint32   `mapstructure:"gid"`
	Groups []string `mapstructure:"groups"`
	Caps   []string `mapstructure:"caps"` // Phase 3; parsed but unused in 1a
}

// MappingConfig declares how a volume maps the authenticated principal to a
// server-side identity. See docs/design/identity-and-permissions.md §2.
type MappingConfig struct {
	Mode MappingMode `mapstructure:"mode" validate:"required,oneof=squash static system passthrough"`

	// squash:
	Uid uint32 `mapstructure:"uid"`
	Gid uint32 `mapstructure:"gid"`

	// static:
	Users  map[string]StaticUser `mapstructure:"users"`
	Groups map[string]uint32     `mapstructure:"groups"`

	// passthrough:
	RootSquash *bool  `mapstructure:"root_squash"` // nil => default true
	AnonUid    uint32 `mapstructure:"anon_uid"`

	// system:
	AdminGroups map[string][]string `mapstructure:"admin_groups"` // cap name -> server-side group names
}

type VolumeConfig struct {
	Name string `validate:"required"`
	Path string `validate:"required"`
	// No `required` tag: a struct-value field is never the zero value at
	// validate time, so it would be a no-op. The validator recurses into
	// MappingConfig automatically, enforcing Mode's `oneof`.
	Mapping MappingConfig
}

// NewVolumeConfig creates a new VolumeConfig with defaults. An absent or empty
// `mapping` block defaults to squash (the safe default). A mapping block that
// fails to unmarshal is a hard error: this is the identity-enforcement config
// — a partially-decoded block (type-mismatched uid, mis-indented users table)
// could otherwise validate and serve the volume with silently-wrong identity
// mapping.
func NewVolumeConfig(v *viper.Viper) (*VolumeConfig, error) {
	m := MappingConfig{Mode: MappingModeSquash}
	if sub := v.Sub("mapping"); sub != nil {
		if err := sub.Unmarshal(&m); err != nil {
			return nil, errors.Wrap(err, "parse mapping block")
		}
		if m.Mode == "" {
			m.Mode = MappingModeSquash
		}
	}
	return &VolumeConfig{
		Name:    v.GetString("name"),
		Path:    v.GetString("path"),
		Mapping: m,
	}, nil
}
