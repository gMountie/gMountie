//go:build !darwin

package gofuse

// On non-macOS platforms the go-fuse path speaks native Linux FUSE and never
// sees com.apple.* names, so xattr names pass through unchanged.

func xattrNameToBackend(name string) string   { return name }
func xattrNameFromBackend(name string) string { return name }

// xattrFlagsToBackend is identity off darwin: Linux FUSE already delivers Linux
// SETXATTR flag values.
func xattrFlagsToBackend(flags uint32) uint32 { return flags }
