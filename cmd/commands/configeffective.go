package commands

import (
	"fmt"
	"reflect"
	"strings"
	"time"

	clientconfig "go.gmountie.dev/gmountie/pkg/client/config"
	serverconfig "go.gmountie.dev/gmountie/pkg/server/config"

	"github.com/spf13/viper"
	"gopkg.in/yaml.v3"
)

var durationType = reflect.TypeOf(time.Duration(0))

// structToDisplayMap converts a parsed config value into a tree of
// map[string]any / []any / scalars keyed by each field's `mapstructure` tag —
// the same snake_case names used in config files — so the result re-marshals
// to YAML that a user could paste straight back. It is used to render the
// *effective* config (file + defaults + env, as resolved by ParseConfig),
// which plain yaml.Marshal can't do faithfully: the config structs carry
// mapstructure tags, not yaml tags, so yaml.Marshal would emit lower-cased
// CamelCase keys (maxwritebytes) and raw nanosecond durations.
//
// Rules: pointers and interfaces are dereferenced (nil → null); time.Duration
// renders as a human string ("5s"); embedded/`,squash` structs are flattened
// into the parent; `mapstructure:"-"` and unexported fields are dropped;
// untagged named fields fall back to their lower-cased Go name.
func structToDisplayMap(v reflect.Value) any {
	for v.Kind() == reflect.Pointer || v.Kind() == reflect.Interface {
		if v.IsNil() {
			return nil
		}
		v = v.Elem()
	}

	if v.Type() == durationType {
		d, _ := v.Interface().(time.Duration)
		return d.String()
	}

	switch v.Kind() {
	case reflect.Struct:
		m := map[string]any{}
		fillStructMap(v, m)
		return m
	case reflect.Slice, reflect.Array:
		if v.Kind() == reflect.Slice && v.IsNil() {
			return nil
		}
		out := make([]any, v.Len())
		for i := 0; i < v.Len(); i++ {
			out[i] = structToDisplayMap(v.Index(i))
		}
		return out
	case reflect.Map:
		if v.IsNil() {
			return nil
		}
		m := map[string]any{}
		for _, k := range v.MapKeys() {
			m[fmt.Sprintf("%v", k.Interface())] = structToDisplayMap(v.MapIndex(k))
		}
		return m
	default:
		return v.Interface()
	}
}

// fillStructMap writes v's exported fields into m, keyed by mapstructure tag,
// flattening embedded/squashed structs so their fields land in the parent (as
// the config file shapes them, e.g. auth.type sitting beside auth.users).
func fillStructMap(v reflect.Value, m map[string]any) {
	t := v.Type()
	for i := 0; i < t.NumField(); i++ {
		f := t.Field(i)
		if !f.IsExported() {
			continue
		}
		name, opts := parseMapstructureTag(f.Tag.Get("mapstructure"))
		if name == "-" {
			continue
		}
		fv := v.Field(i)
		if opts["squash"] || (f.Anonymous && name == "") {
			if sub, ok := structToDisplayMap(fv).(map[string]any); ok {
				for k, val := range sub {
					m[k] = val
				}
			}
			continue
		}
		if name == "" {
			name = strings.ToLower(f.Name)
		}
		m[name] = structToDisplayMap(fv)
	}
}

// parseMapstructureTag splits a mapstructure tag into its name and option set
// (e.g. "addr,omitempty" → "addr", {omitempty}; ",squash" → "", {squash}).
func parseMapstructureTag(tag string) (string, map[string]bool) {
	parts := strings.Split(tag, ",")
	opts := map[string]bool{}
	for _, o := range parts[1:] {
		opts[o] = true
	}
	return parts[0], opts
}

// marshalEffective renders a parsed config value as YAML keyed by mapstructure
// names. Secrets are NOT redacted here; callers pass the result through
// redactConfigYAML.
func marshalEffective(cfg any) (string, error) {
	out, err := yaml.Marshal(structToDisplayMap(reflect.ValueOf(cfg)))
	if err != nil {
		return "", err
	}
	return string(out), nil
}

// renderEffectiveConfig loads the config file at path, resolves it through the
// appropriate ParseConfig (merging defaults and GMOUNTIE_* env overrides), and
// returns the effective config as redacted YAML. `which` is "server" or
// "client"/"" (the same selector as config show's --for).
func renderEffectiveConfig(path, which string) (string, error) {
	v := viper.New()
	v.SetConfigFile(path)
	if err := v.ReadInConfig(); err != nil {
		return "", fmt.Errorf("read config %s: %w", path, err)
	}

	var cfg any
	var err error
	switch which {
	case "server":
		cfg, err = serverconfig.ParseConfig(v)
	case "client", "":
		cfg, err = clientconfig.ParseConfig(v)
	default:
		return "", fmt.Errorf("--for must be server or client, got %q", which)
	}
	if err != nil {
		return "", err
	}

	yamlStr, err := marshalEffective(cfg)
	if err != nil {
		return "", err
	}
	return redactConfigYAML(yamlStr), nil
}
