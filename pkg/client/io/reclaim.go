package io

import "syscall"

// sanitizeReopenFlags returns the open flags to use when REOPENING an
// already-open file during reclaim. The file already exists and already holds
// the application's data, so creation/exclusivity/truncation flags must be
// stripped: O_TRUNC would discard the bytes the app has been writing, and
// O_EXCL would fail because the path now exists. The access mode and O_APPEND
// are preserved so reads/writes keep the same semantics.
func sanitizeReopenFlags(flags uint32) uint32 {
	const strip = uint32(syscall.O_CREAT | syscall.O_EXCL | syscall.O_TRUNC)
	return flags &^ strip
}
