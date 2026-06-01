package config

import (
	"errors"
	"fmt"
	"strings"

	"github.com/go-playground/validator/v10"
)

// FriendlyValidationError turns a validator.ValidationErrors into a single,
// human-readable error listing each offending field as a snake_case dotted
// config key plus a plain-language reason. Non-validation errors (and nil) are
// returned unchanged, so it is safe to wrap any error-returning validate call.
//
// Example:
//
//	config error:
//	  - server.address is required
//	  - server.frame_size_bytes must be at least 4096
func FriendlyValidationError(err error) error {
	if err == nil {
		return nil
	}
	var verrs validator.ValidationErrors
	if !errors.As(err, &verrs) {
		return err
	}

	lines := make([]string, 0, len(verrs))
	for _, fe := range verrs {
		lines = append(lines, fmt.Sprintf("  - %s %s", keyPath(fe.Namespace()), reason(fe)))
	}
	return fmt.Errorf("config error:\n%s", strings.Join(lines, "\n"))
}

// keyPath converts a validator namespace ("Config.Server.Address") into the
// snake_case dotted config key a user actually writes ("server.address"). The
// leading struct-type segment is dropped; each remaining segment is converted
// from CamelCase to snake_case. Slice indices (e.g. "Volumes[0]") are kept.
func keyPath(namespace string) string {
	parts := strings.Split(namespace, ".")
	if len(parts) > 1 {
		parts = parts[1:] // drop the root struct type name
	}
	for i, p := range parts {
		parts[i] = camelToSnake(p)
	}
	return strings.Join(parts, ".")
}

// camelToSnake lowercases CamelCase into snake_case, preserving any "[n]"
// index suffix. "FrameSizeBytes" -> "frame_size_bytes".
func camelToSnake(s string) string {
	idx := ""
	if b := strings.IndexByte(s, '['); b >= 0 {
		idx = s[b:]
		s = s[:b]
	}
	var out strings.Builder
	runes := []rune(s)
	for i, r := range runes {
		if r >= 'A' && r <= 'Z' {
			// Insert "_" before an uppercase run boundary, but not at the start
			// and not between consecutive uppercase letters (acronyms).
			if i > 0 && (runes[i-1] < 'A' || runes[i-1] > 'Z') {
				out.WriteByte('_')
			}
			out.WriteRune(r - 'A' + 'a')
			continue
		}
		out.WriteRune(r)
	}
	return out.String() + idx
}

// reason maps a validator tag to a plain-language explanation.
func reason(fe validator.FieldError) string {
	switch fe.Tag() {
	case "required":
		return "is required"
	case "ip":
		return "must be a valid IP address (e.g. 0.0.0.0 to listen on all interfaces, or 127.0.0.1)"
	case "hostname_port":
		return "must be host:port (e.g. 127.0.0.1:9090)"
	case "oneof":
		return fmt.Sprintf("must be one of: %s", fe.Param())
	case "min":
		return fmt.Sprintf("must be at least %s", fe.Param())
	case "max":
		return fmt.Sprintf("must be at most %s", fe.Param())
	case "gte":
		return fmt.Sprintf("must be >= %s", fe.Param())
	case "lte":
		return fmt.Sprintf("must be <= %s", fe.Param())
	case "gt":
		return fmt.Sprintf("must be > %s", fe.Param())
	case "lt":
		return fmt.Sprintf("must be < %s", fe.Param())
	default:
		if fe.Param() != "" {
			return fmt.Sprintf("failed %q validation (%s)", fe.Tag(), fe.Param())
		}
		return fmt.Sprintf("failed %q validation", fe.Tag())
	}
}
