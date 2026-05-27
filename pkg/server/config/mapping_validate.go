package config

import "github.com/pkg/errors"

// ValidateMapping enforces cross-field rules struct tags can't express: the
// resolver-backed modes need an authenticated principal, so they are invalid
// with auth disabled. Fail closed at startup rather than silently resolving
// "anonymous".
func ValidateMapping(mode MappingMode, authType AuthConfigType) error {
	switch mode {
	case MappingModeSystem, MappingModeStatic:
		if authType == AuthConfigTypeNone {
			return errors.Errorf("mapping mode %q requires authentication (auth.type must not be none)", mode)
		}
	}
	return nil
}
