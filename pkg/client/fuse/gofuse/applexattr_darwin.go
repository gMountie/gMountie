//go:build darwin

package gofuse

// On macFUSE the kernel forwards macOS com.apple.* extended attributes that the
// server cannot store verbatim. Remap them into the user.* namespace on the way
// out and reverse the mapping on the way back, so copyfile()-based copies (cp,
// ditto, Finder) succeed and the metadata round-trips. See applexattr.go.

func xattrNameToBackend(name string) string   { return appleXattrToBackend(name) }
func xattrNameFromBackend(name string) string { return appleXattrFromBackend(name) }
