package config

import (
	"fmt"
	"reflect"
	"sort"
	"strings"
	"time"

	"github.com/spf13/viper"
	"go.gmountie.dev/gmountie/pkg/utils/log"
)

// This file is the single source of truth for which client-config keys are
// overridable — via environment variables (mirrorEnvToSub, see config.go) and
// via the `--set key=value` CLI flag (ApplyOverrides). Both derive their key
// set by reflecting the config structs' `mapstructure` tags, so the set can
// never drift from the structs the way a hand-maintained list does.

// configSections maps a config section (the viper sub-tree key ParseConfig
// uses) to the Go struct that defines its keys. auth and mount are intentionally
// absent: they have dedicated flags (--auth-type/--username/--password and
// --server/--volume) and non-scalar shapes, so `--set auth.* / mount.*` errors
// with a pointer to the right flag (see unknownKeyError).
func configSections() map[string]reflect.Type {
	return map[string]reflect.Type{
		"server": reflect.TypeOf(ServerConfig{}),
		"rpc":    reflect.TypeOf(RpcConfig{}),
		"fuse":   reflect.TypeOf(FUSEConfig{}),
		"cache":  reflect.TypeOf(CacheConfig{}),
		"wal":    reflect.TypeOf(WALConfig{}),
		"renew":  reflect.TypeOf(RenewConfig{}),
		"log":    reflect.TypeOf(log.LogConfig{}),
	}
}

var timeType = reflect.TypeOf(time.Time{})

// leafKeys returns every dotted key of struct type t, relative to the struct
// root, derived from `mapstructure` tags. Rules, matching how viper+mapstructure
// actually decode:
//   - tag present  → use the tag (the part before any ",option").
//   - tag == "-"   → skip (mapstructure ignores it).
//   - tag absent   → use the lowercased field name (mapstructure matches field
//     names case-insensitively and viper lowercases keys; e.g. ServerConfig
//     Address/Port carry no tag yet decode from "address"/"port").
//   - nested struct → recurse with the field's key as prefix (e.g. tls.ca_file).
//   - anonymous (embedded) struct → flatten: recurse with NO added segment.
//   - time.Duration is reflect.Int64, so it is a leaf naturally; time.Time is a
//     struct and is guarded as a leaf.
func leafKeys(t reflect.Type) []string {
	var out []string
	var walk func(t reflect.Type, prefix string)
	walk = func(t reflect.Type, prefix string) {
		for t.Kind() == reflect.Pointer {
			t = t.Elem()
		}
		if t.Kind() != reflect.Struct {
			return
		}
		for i := 0; i < t.NumField(); i++ {
			f := t.Field(i)
			if f.PkgPath != "" && !f.Anonymous {
				continue // unexported, non-embedded
			}
			seg := ""
			if tag, ok := f.Tag.Lookup("mapstructure"); ok {
				tag = strings.Split(tag, ",")[0]
				if tag == "-" {
					continue
				}
				seg = tag
			} else if !f.Anonymous {
				seg = strings.ToLower(f.Name)
			}
			ft := f.Type
			for ft.Kind() == reflect.Pointer {
				ft = ft.Elem()
			}
			if ft.Kind() == reflect.Struct && ft != timeType {
				if f.Anonymous {
					walk(ft, prefix) // flatten embedded struct
				} else {
					walk(ft, joinKey(prefix, seg))
				}
				continue
			}
			out = append(out, joinKey(prefix, seg))
		}
	}
	walk(t, "")
	return out
}

func joinKey(prefix, seg string) string {
	switch {
	case prefix == "":
		return seg
	case seg == "":
		return prefix
	default:
		return prefix + "." + seg
	}
}

// sectionLeafKeys returns the reflected leaf keys for a section (relative to the
// section root, e.g. "tls.ca_file" for "server"). It drives the mirrorEnvToSub
// calls in ParseConfig. An unknown section yields nil.
func sectionLeafKeys(section string) []string {
	t, ok := configSections()[section]
	if !ok {
		return nil
	}
	return leafKeys(t)
}

// overridableKeys is the full set of fully-qualified ("section.leaf") keys that
// --set and env vars may target.
func overridableKeys() map[string]bool {
	m := map[string]bool{}
	for section, t := range configSections() {
		for _, k := range leafKeys(t) {
			m[section+"."+k] = true
		}
	}
	return m
}

// ApplyOverrides applies `key=value` CLI overrides onto v with the highest
// precedence (viper Set beats config file, env, and defaults), so a --set wins
// over everything. Keys are validated against the struct-derived set and
// normalized to lowercase (viper stores keys case-insensitively); values are
// passed through untouched and coerced to the target field type by viper's
// WeaklyTypedInput decode during ParseConfig. Must be called before ParseConfig
// so the mirrorEnvToSub copies see the override via parent.IsSet.
func ApplyOverrides(v *viper.Viper, sets []string) error {
	if len(sets) == 0 {
		return nil
	}
	valid := overridableKeys()
	for _, s := range sets {
		eq := strings.IndexByte(s, '=')
		if eq < 0 {
			return fmt.Errorf("invalid --set %q: expected key=value", s)
		}
		key := strings.ToLower(strings.TrimSpace(s[:eq]))
		val := s[eq+1:]
		if key == "" {
			return fmt.Errorf("invalid --set %q: empty key", s)
		}
		if !valid[key] {
			return unknownKeyError(key, valid)
		}
		v.Set(key, val)
	}
	return nil
}

// unknownKeyError builds a helpful error for a key not in the overridable set:
// it points auth/mount at their dedicated flags, lists a known section's valid
// keys when the section is right but the leaf is wrong, and lists valid sections
// otherwise.
func unknownKeyError(key string, valid map[string]bool) error {
	switch {
	case strings.HasPrefix(key, "auth."):
		return fmt.Errorf("--set %q: auth is set via --auth-type/--username/--password, not --set", key)
	case strings.HasPrefix(key, "mount."):
		return fmt.Errorf("--set %q: mount is set via --server/--volume, not --set", key)
	}
	section := key
	if i := strings.IndexByte(key, '.'); i >= 0 {
		section = key[:i]
	}
	if _, ok := configSections()[section]; ok {
		var ks []string
		for k := range valid {
			if strings.HasPrefix(k, section+".") {
				ks = append(ks, k)
			}
		}
		sort.Strings(ks)
		return fmt.Errorf("--set %q: unknown key in section %q; valid keys: %s", key, section, strings.Join(ks, ", "))
	}
	var sections []string
	for s := range configSections() {
		sections = append(sections, s)
	}
	sort.Strings(sections)
	return fmt.Errorf("--set %q: unknown config section %q; valid sections: %s", key, section, strings.Join(sections, ", "))
}
