package tls

import (
	"crypto/tls"
	"os"
	"sync"
	"sync/atomic"
	"time"
)

// stamp identifies a cert file's on-disk version. The inode catches the
// kubelet projected-volume rotation (an atomic ..data symlink swap = a new
// inode) and same-second mtime granularity; os.Stat follows symlinks so the
// projected layout is transparent.
type stamp struct {
	modTime time.Time
	size    int64
	inode   uint64
}

// equal compares stamps. modTime is compared with Equal (never ==) so a
// monotonic-clock component can't poison the comparison.
func (a stamp) equal(b stamp) bool {
	return a.modTime.Equal(b.modTime) && a.size == b.size && a.inode == b.inode
}

// statStamp reads the current stamp of path.
func statStamp(path string) (stamp, error) {
	fi, err := os.Stat(path)
	if err != nil {
		return stamp{}, err
	}
	return stamp{modTime: fi.ModTime(), size: fi.Size(), inode: inodeOf(fi)}, nil
}

// Reloader serves a TLS cert+key pair from disk, transparently reloading
// it when the cert file changes (Task 3). Safe for concurrent use as a
// tls.Config.GetCertificate callback.
type Reloader struct {
	certPath string
	keyPath  string

	current atomic.Pointer[tls.Certificate]
	loaded  atomic.Pointer[stamp]

	// mu serializes reloads and guards fingerprint. failing is atomic so
	// the lock-free stat-failure path can warn-once too.
	mu          sync.Mutex
	fingerprint string
	failing     atomic.Bool
}

// NewReloader loads the initial pair — failing fast on a bad cert exactly
// like the static startup path — and caches the cert file's stamp.
func NewReloader(certPath, keyPath string) (*Reloader, error) {
	cert, certPEM, err := Load(certPath, keyPath)
	if err != nil {
		return nil, err
	}
	st, err := statStamp(certPath)
	if err != nil {
		return nil, err
	}
	fp, err := Fingerprint(certPEM)
	if err != nil {
		return nil, err
	}
	r := &Reloader{certPath: certPath, keyPath: keyPath, fingerprint: fp}
	r.current.Store(&cert)
	r.loaded.Store(&st)
	return r, nil
}

// GetCertificate is a tls.Config.GetCertificate callback: it serves the
// cached pair. (Rotation handling lands in the next slice.)
func (r *Reloader) GetCertificate(*tls.ClientHelloInfo) (*tls.Certificate, error) {
	return r.current.Load(), nil
}
