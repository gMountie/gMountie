package service

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"github.com/hanwen/go-fuse/v2/fuse/nodefs"
	lru "github.com/hashicorp/golang-lru/v2"
	"github.com/pkg/errors"
	"github.com/puzpuzpuz/xsync/v3"
	"golang.org/x/sync/singleflight"
)

// FileEntry is a per-session record for an open file.
type FileEntry struct {
	File nodefs.File
	Path string
	Fd   uint64
}

// Session is the per-client view of server state. Each session owns its own
// fd numbering and fd table.
type Session interface {
	ID() string
	// Principal returns the authenticated principal that created this session.
	// Set once at Create time; never mutated.
	Principal() string
	// Serial returns the canonical cert SerialKey bound at Create (empty for
	// basic-auth). Used by the reaper to match a blocked serial out of band.
	Serial() string
	RegisterFile(path string, file nodefs.File) uint64
	GetFile(fd uint64) (*FileEntry, bool)
	ReleaseFile(fd uint64)
	// ReleaseAll releases every fd in the session. Called by the manager when
	// the session is reaped.
	ReleaseAll()
	// DoOnce returns the cached reply for requestID if present; otherwise it
	// calls fn, caches the successful reply, and returns it. Concurrent calls
	// with the same requestID are collapsed via singleflight so fn runs at
	// most once. Errored fns are NOT cached — the next caller re-executes.
	DoOnce(requestID string, fn func() (any, error)) (any, error)
}

// SessionManager is the per-server registry of sessions.
type SessionManager interface {
	// Create creates a new session bound to principal and the canonical cert
	// SerialKey (empty for basic-auth), returning the session id.
	Create(principal, serial string) (string, error)
	Get(id string) (Session, error)
	// Resume cancels a pending reap timer for the given session if there is
	// one. Returns (true, nil) if the session existed and was reattached;
	// (false, nil) if the session is unknown (caller should Create a new one).
	Resume(id string) (bool, error)
	// MarkDisconnected starts the grace-period reap timer for the given
	// session. Idempotent: calling twice without a Resume in between is a
	// no-op.
	MarkDisconnected(id string)
	// ReapIf releases all fds of, and removes, every session for which pred
	// returns true; returns the count reaped. Called by the ops reload path to
	// evict revoked principals/serials without a restart. pred receives the
	// session's principal and cert SerialKey.
	ReapIf(pred func(principal, serial string) bool) int
	// Stop cancels any in-flight grace timers and forcibly releases all fds.
	// Called on server shutdown.
	Stop(ctx context.Context) error
}

// SessionMetrics is a thin hook for the session manager to report its
// active-session count to a metrics sink. Defined here so the service
// package stays decoupled from the metrics package.
type SessionMetrics interface {
	SessionsActiveInc()
	SessionsActiveDec()
}

type noopSessionMetrics struct{}

func (noopSessionMetrics) SessionsActiveInc() {}
func (noopSessionMetrics) SessionsActiveDec() {}

type SessionManagerOptions struct {
	GracePeriod time.Duration
	// Metrics is an optional sink for the active-session gauge. Nil
	// substitutes a no-op implementation.
	Metrics SessionMetrics
}

// DefaultGracePeriod is how long the server retains a disconnected client's
// session (fds + idempotency cache) before reaping it, so a brief network drop
// can Resume the same session. Aligned with the client rpc.retry_window default.
// Cost: a dropped client's fds and POSIX locks are held for this long.
const DefaultGracePeriod = 60 * time.Second

// DefaultIdempotencyCacheSize is the per-session LRU size for dedup. 256 covers
// a comfortable churn window for typical FUSE traffic without bloating memory.
const DefaultIdempotencyCacheSize = 256

type sessionImpl struct {
	id        string
	principal string // set once at Create; never mutated
	serial    string // canonical cert SerialKey; "" for basic-auth
	fdNum     atomic.Uint64
	files     *xsync.MapOf[uint64, *FileEntry]
	replies   *lru.Cache[string, any]
	sf        singleflight.Group
}

func (s *sessionImpl) ID() string        { return s.id }
func (s *sessionImpl) Principal() string { return s.principal }
func (s *sessionImpl) Serial() string    { return s.serial }

func (s *sessionImpl) RegisterFile(path string, file nodefs.File) uint64 {
	fd := s.fdNum.Add(1)
	s.files.Store(fd, &FileEntry{File: file, Path: path, Fd: fd})
	return fd
}

func (s *sessionImpl) GetFile(fd uint64) (*FileEntry, bool) {
	return s.files.Load(fd)
}

func (s *sessionImpl) ReleaseFile(fd uint64) {
	entry, ok := s.files.LoadAndDelete(fd)
	if ok && entry.File != nil {
		entry.File.Release()
	}
}

func (s *sessionImpl) DoOnce(requestID string, fn func() (any, error)) (any, error) {
	if v, ok := s.replies.Get(requestID); ok {
		return v, nil
	}
	v, err, _ := s.sf.Do(requestID, func() (any, error) {
		// Double-check after the singleflight wait — another caller may have
		// completed while we queued.
		if cached, ok := s.replies.Get(requestID); ok {
			return cached, nil
		}
		out, err := fn()
		if err != nil {
			return nil, err
		}
		s.replies.Add(requestID, out)
		return out, nil
	})
	return v, err
}

func (s *sessionImpl) ReleaseAll() {
	s.files.Range(func(fd uint64, entry *FileEntry) bool {
		s.files.Delete(fd)
		if entry.File != nil {
			entry.File.Release()
		}
		return true
	})
}

type pendingReap struct {
	cancel context.CancelFunc
}

type sessionManagerImpl struct {
	sessions *xsync.MapOf[string, *sessionImpl]
	reapers  *xsync.MapOf[string, *pendingReap]
	grace    time.Duration
	metrics  SessionMetrics
	wg       sync.WaitGroup
}

func NewSessionManager(opts SessionManagerOptions) SessionManager {
	grace := opts.GracePeriod
	if grace == 0 {
		grace = DefaultGracePeriod
	}
	m := opts.Metrics
	if m == nil {
		m = noopSessionMetrics{}
	}
	return &sessionManagerImpl{
		sessions: xsync.NewMapOf[string, *sessionImpl](),
		reapers:  xsync.NewMapOf[string, *pendingReap](),
		grace:    grace,
		metrics:  m,
	}
}

func (m *sessionManagerImpl) Create(principal, serial string) (string, error) {
	id := uuid.NewString()
	replies, err := lru.New[string, any](DefaultIdempotencyCacheSize)
	if err != nil {
		return "", errors.Wrap(err, "create idempotency cache")
	}
	sess := &sessionImpl{
		id:        id,
		principal: principal,
		serial:    serial,
		files:     xsync.NewMapOf[uint64, *FileEntry](),
		replies:   replies,
	}
	m.sessions.Store(id, sess)
	m.metrics.SessionsActiveInc()
	return id, nil
}

func (m *sessionManagerImpl) Get(id string) (Session, error) {
	sess, ok := m.sessions.Load(id)
	if !ok {
		return nil, errors.Errorf("session not found: %s", id)
	}
	return sess, nil
}

func (m *sessionManagerImpl) Resume(id string) (bool, error) {
	if _, ok := m.sessions.Load(id); !ok {
		return false, nil
	}
	if reaper, ok := m.reapers.LoadAndDelete(id); ok {
		reaper.cancel()
	}
	return true, nil
}

func (m *sessionManagerImpl) MarkDisconnected(id string) {
	sess, ok := m.sessions.Load(id)
	if !ok {
		return
	}
	ctx, cancel := context.WithCancel(context.Background())
	reaper := &pendingReap{cancel: cancel}
	if _, loaded := m.reapers.LoadOrStore(id, reaper); loaded {
		// Another caller already scheduled a reap for this session — drop
		// ours on the floor.
		cancel()
		return
	}
	m.wg.Add(1)
	go func() {
		defer m.wg.Done()
		select {
		case <-ctx.Done():
			return
		case <-time.After(m.grace):
			// Only proceed if our reaper entry is still present. If Resume
			// raced and won the LoadAndDelete, abort without touching the
			// session.
			if _, ok := m.reapers.LoadAndDelete(sess.id); !ok {
				return
			}
			// The sessions LoadAndDelete is the single authoritative release
			// token: only the winner releases the fds. Without this guard a
			// concurrent ReapIf (which also claims via sessions) could
			// double-ReleaseAll the same session and double-close its fds.
			if _, ok := m.sessions.LoadAndDelete(sess.id); ok {
				m.metrics.SessionsActiveDec()
				sess.ReleaseAll()
			}
		}
	}()
}

func (m *sessionManagerImpl) ReapIf(pred func(principal, serial string) bool) int {
	reaped := 0
	m.sessions.Range(func(id string, sess *sessionImpl) bool {
		if !pred(sess.principal, sess.serial) {
			return true
		}
		if _, ok := m.sessions.LoadAndDelete(id); ok {
			// Cancel any pending grace-reaper for this session so its goroutine
			// doesn't linger (up to GracePeriod) holding the WaitGroup and
			// delaying Stop. Safe: the grace goroutine no-ops once we've won the
			// sessions claim above.
			if r, had := m.reapers.LoadAndDelete(id); had {
				r.cancel()
			}
			m.metrics.SessionsActiveDec()
			sess.ReleaseAll()
			reaped++
		}
		return true
	})
	return reaped
}

func (m *sessionManagerImpl) Stop(ctx context.Context) error {
	// Cancel all pending reapers — claim each to prevent the goroutine
	// from doing its own delete.
	m.reapers.Range(func(id string, r *pendingReap) bool {
		if _, ok := m.reapers.LoadAndDelete(id); ok {
			r.cancel()
		}
		return true
	})

	// Wait for reap goroutines to exit, honouring ctx.
	done := make(chan struct{})
	go func() {
		m.wg.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-ctx.Done():
		// Caller's deadline elapsed; proceed to release anyway.
	}

	// Claim each remaining session before releasing.
	m.sessions.Range(func(id string, sess *sessionImpl) bool {
		if _, ok := m.sessions.LoadAndDelete(id); ok {
			m.metrics.SessionsActiveDec()
			sess.ReleaseAll()
		}
		return true
	})
	return nil
}
