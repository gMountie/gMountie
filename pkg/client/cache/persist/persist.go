// Package persist is the on-disk backing store for the client-side
// cache. It owns a bbolt database (meta.db) under a per-volume root
// directory and a content-addressable chunks/ tree. A LOCK file
// enforces single-process ownership. Higher layers in pkg/client/cache
// compose a Persist with their in-memory tiers.
package persist

import (
	"os"
	"path/filepath"
	"time"

	"github.com/pkg/errors"
	bolt "go.etcd.io/bbolt"
)

// ErrCacheLocked is returned by Open when another process already
// holds the LOCK file in Root. Wrap-checked with errors.Is.
var ErrCacheLocked = errors.New("cache directory is locked by another process")

// Options governs Open behaviour.
type Options struct {
	// Root is the per-volume cache directory. Created if missing.
	Root string
	// DiskMaxBytes is the advisory byte budget for chunks/. 0 = unbounded.
	DiskMaxBytes int64
}

// Persist owns the bbolt handle, chunks/ tree, and LOCK file for one
// cache directory. Safe for concurrent use; bbolt is single-writer
// but Persist serializes writes internally.
type Persist struct {
	root string
	db   *bolt.DB
	lock *lockHandle
	disk *diskAccountant
}

// Open acquires the LOCK file, opens (or creates) meta.db, ensures
// buckets exist, and validates format_version (wipes on mismatch).
// Returns ErrCacheLocked when another process holds the lock.
func Open(opts Options) (*Persist, error) {
	if opts.Root == "" {
		return nil, errors.New("persist.Open: Root is required")
	}
	if err := os.MkdirAll(opts.Root, 0o755); err != nil {
		return nil, errors.Wrap(err, "create cache root")
	}
	if err := os.MkdirAll(filepath.Join(opts.Root, "chunks"), 0o755); err != nil {
		return nil, errors.Wrap(err, "create chunks dir")
	}
	lock, err := acquireLock(filepath.Join(opts.Root, "LOCK"))
	if err != nil {
		return nil, err
	}
	db, err := bolt.Open(filepath.Join(opts.Root, "meta.db"), 0o600, &bolt.Options{Timeout: time.Second})
	if err != nil {
		_ = lock.release()
		return nil, errors.Wrap(err, "open meta.db")
	}
	wiped, err := ensureSchema(db)
	if err != nil {
		_ = db.Close()
		_ = lock.release()
		return nil, err
	}
	if wiped {
		if err := wipeChunksFor(opts.Root); err != nil {
			_ = db.Close()
			lock.release()
			return nil, err
		}
	}
	p := &Persist{root: opts.Root, db: db, lock: lock, disk: newDiskAccountant(opts.DiskMaxBytes)}
	if err := p.seedDiskBytes(); err != nil {
		_ = db.Close()
		_ = lock.release()
		return nil, err
	}
	if err := p.enforceDiskBudget(); err != nil {
		_ = db.Close()
		_ = lock.release()
		return nil, err
	}
	p.startBackgroundSweeps()
	return p, nil
}

// Close flushes bbolt, releases the lock file, and frees OS resources.
func (p *Persist) Close() error {
	if err := p.db.Close(); err != nil {
		_ = p.lock.release()
		return errors.Wrap(err, "close meta.db")
	}
	return p.lock.release()
}

// Root returns the cache directory passed to Open.
func (p *Persist) Root() string { return p.root }

