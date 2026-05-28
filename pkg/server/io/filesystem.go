package io

// LocalFilesystem is a confined loopback filesystem rooted at a single volume
// directory. Every path is resolved beneath the root via openat2 RESOLVE_BENEATH
// so a malicious or buggy wire path can never reach a file outside the volume.
type LocalFilesystem struct {
	*ConfinedLoopbackFileSystem
}

// NewLocalFilesystem opens path as a confined volume root. Returns an error if
// the path does not exist, is not a directory, or cannot be opened.
func NewLocalFilesystem(path string) (*LocalFilesystem, error) {
	fs, err := NewConfinedLoopbackFileSystem(path)
	if err != nil {
		return nil, err
	}
	return &LocalFilesystem{ConfinedLoopbackFileSystem: fs}, nil
}
