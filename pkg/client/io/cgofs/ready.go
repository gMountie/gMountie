//go:build darwin || cgofuse

package cgofs

// Init is called by cgofuse when the filesystem is initialized; closing ready
// lets the mounter return once the volume is live. Destroy is called on
// teardown; closing done lets Wait/Unmount observe the serve loop's exit.
func (fs *MountieCgoFS) Init() { fs.readyOnce.Do(func() { close(fs.ready) }) }

// Destroy is called by cgofuse on teardown; closing done lets Wait/Unmount
// observe the serve loop's exit. The mounter goroutine also calls Destroy
// after Mount returns to ensure done closes even if cgofuse skips the hook.
func (fs *MountieCgoFS) Destroy() { fs.doneOnce.Do(func() { close(fs.done) }) }

// Ready returns a channel that is closed when Init fires (mount is live).
func (fs *MountieCgoFS) Ready() <-chan struct{} { return fs.ready }

// Done returns a channel that is closed when Destroy fires (serve loop exited).
func (fs *MountieCgoFS) Done() <-chan struct{} { return fs.done }
