package persist

import (
	"os"

	"github.com/pkg/errors"
	"golang.org/x/sys/unix"
)

// lockHandle owns an OS-level advisory lock on a file. release()
// closes the fd, which the kernel uses to drop the lock.
type lockHandle struct {
	f *os.File
}

// acquireLock takes an exclusive non-blocking flock on path. Returns
// ErrCacheLocked when another process holds it.
func acquireLock(path string) (*lockHandle, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return nil, errors.Wrap(err, "open lock file")
	}
	if err := unix.Flock(int(f.Fd()), unix.LOCK_EX|unix.LOCK_NB); err != nil {
		_ = f.Close()
		if errors.Is(err, unix.EWOULDBLOCK) {
			return nil, ErrCacheLocked
		}
		return nil, errors.Wrap(err, "flock LOCK")
	}
	return &lockHandle{f: f}, nil
}

func (l *lockHandle) release() error {
	if l.f == nil {
		return nil
	}
	// Closing the fd releases the lock; explicit LOCK_UN is redundant.
	err := l.f.Close()
	l.f = nil
	return err
}
