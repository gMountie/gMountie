package persist

import (
	"os"
	"time"

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

// acquireLockWithRetry takes a non-blocking flock on path, retrying on
// ErrCacheLocked until timeout elapses. Negative timeout disables the
// retry (single attempt). Zero uses DefaultLockAcquireTimeout. Any
// non-ErrCacheLocked error (open failure, unexpected flock error) is
// returned immediately without retry.
func acquireLockWithRetry(path string, timeout time.Duration) (*lockHandle, error) {
	if timeout == 0 {
		timeout = DefaultLockAcquireTimeout
	}
	if timeout < 0 {
		return acquireLock(path)
	}
	deadline := time.Now().Add(timeout)
	for {
		lock, err := acquireLock(path)
		if err == nil || !errors.Is(err, ErrCacheLocked) {
			return lock, err
		}
		if time.Now().After(deadline) {
			return nil, err
		}
		time.Sleep(lockRetryInterval)
	}
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
