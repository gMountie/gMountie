package tls

import (
	"crypto/tls"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"go.gmountie.dev/gmountie/pkg/utils/log"
	"go.uber.org/zap"
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
// cached pair, reloading it first when the cert file's stamp has changed.
//
// Fail-open contract: any stat or reload failure (file briefly missing
// mid-rotation, cert/key mismatch from a non-atomic swap, unparsable PEM)
// keeps the last good pair — a handshake the old cert could serve is never
// failed by the reloader. Failures warn once per streak, not per handshake.
func (r *Reloader) GetCertificate(*tls.ClientHelloInfo) (*tls.Certificate, error) {
	st, err := statStamp(r.certPath)
	if err != nil {
		r.warnOnce("server cert stat failed; serving previous cert", err)
		return r.current.Load(), nil
	}
	if st.equal(*r.loaded.Load()) {
		return r.current.Load(), nil
	}

	// Stamp changed: one handshake reloads, concurrent ones keep serving
	// the cached pair without blocking on disk I/O.
	if !r.mu.TryLock() {
		return r.current.Load(), nil
	}
	defer r.mu.Unlock()
	if st.equal(*r.loaded.Load()) { // raced with a completed reload
		return r.current.Load(), nil
	}

	cert, certPEM, err := Load(r.certPath, r.keyPath)
	if err != nil {
		r.warnOnce("server cert changed on disk but reload failed; serving previous cert", err)
		return r.current.Load(), nil
	}
	fp, err := Fingerprint(certPEM)
	if err != nil {
		r.warnOnce("server cert changed on disk but reload failed; serving previous cert", err)
		return r.current.Load(), nil
	}

	if r.failing.Swap(false) {
		log.Log.Info("server cert reload recovered", zap.String("cert_path", r.certPath))
	}
	log.Log.Info("server cert reloaded",
		zap.String("cert_path", r.certPath),
		zap.String("old_fingerprint", r.fingerprint),
		zap.String("fingerprint", fp))
	r.fingerprint = fp
	r.current.Store(&cert)
	r.loaded.Store(&st)
	return r.current.Load(), nil
}

// warnOnce logs the first failure of a streak; subsequent failures are
// silent until a reload succeeds (no per-handshake log spam).
func (r *Reloader) warnOnce(msg string, err error) {
	if r.failing.CompareAndSwap(false, true) {
		log.Log.Warn(msg, zap.String("cert_path", r.certPath), zap.Error(err))
	}
}
