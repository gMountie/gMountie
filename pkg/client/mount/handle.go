package mount

// mountHandle is the platform-agnostic lifecycle handle for an established
// mount. The go-fuse path wraps *fuse.Server; the cgofuse path wraps a
// cgofuse FileSystemHost goroutine.
type mountHandle interface {
	// Wait blocks until the mount's serve loop exits (our own unmount or an
	// out-of-band detach).
	Wait()
	// Unmount requests teardown and blocks until the serve loop has exited.
	Unmount(mountPath string) error
}
