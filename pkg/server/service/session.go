package service

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"go.gmountie.dev/gmountie/pkg/common"
	"go.gmountie.dev/gmountie/pkg/utils/log"

	"github.com/google/uuid"
	"github.com/hanwen/go-fuse/v2/fuse/nodefs"
	lru "github.com/hashicorp/golang-lru/v2"
	"github.com/pkg/errors"
	"github.com/puzpuzpuz/xsync/v3"
	"go.uber.org/zap"
	"golang.org/x/sync/singleflight"
)

// FileEntry is a per-session record for an open file. Volume is the volume
// the fd was opened on — fd-based RPCs must only honour the fd for that
// volume (the controller enforces the binding).
type FileEntry struct {
	File   nodefs.File
	Volume string
	Path   string
	Fd     uint64
	// refs guards File against the reap-vs-in-flight race: the fd table holds
	// one ref; every in-flight fd-op holds one more. File.Release() runs only
	// when the last ref drops, so a grace-expiry reap (or revocation ReapIf)
	// can no longer close the fd under a concurrent pread/pwrite.
	refs atomic.Int64
}

// Acquire takes a ref for the duration of an fd-op. It fails once the entry
// is fully released (refs hit zero) — a dead entry can never be revived.
func (e *FileEntry) Acquire() bool {
	for {
		r := e.refs.Load()
		if r <= 0 {
			return false
		}
		if e.refs.CompareAndSwap(r, r+1) {
			return true
		}
	}
}

// ReleaseRef drops one ref; the last ref out closes the underlying File.
func (e *FileEntry) ReleaseRef() {
	n := e.refs.Add(-1)
	if n == 0 && e.File != nil {
		e.File.Release()
	}
	if n < 0 {
		// An unmatched ReleaseRef silently re-arms the reap race this counter
		// exists to prevent (a later legitimate release would close the file
		// under a live ref). Fail loud instead.
		panic("FileEntry: ReleaseRef without matching Acquire")
	}
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
	// RegisterFile records an open file under a fresh fd, bound to the volume
	// it was opened on. The binding is what lets fd-based RPCs reject a
	// client-supplied volume that doesn't match the fd's volume.
	RegisterFile(volume, path string, file nodefs.File) uint64
	GetFile(fd uint64) (*FileEntry, bool)
	ReleaseFile(fd uint64)
	// ReleaseAll releases every fd in the session, returning the number of
	// fds released. Called by the manager when the session is reaped.
	ReleaseAll() int
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

// SessionMetrics is a thin hook for the session layer to report its
// active-session count and per-volume open-fd gauge to a metrics sink.
// Defined here so the service package stays decoupled from the metrics
// package. The open-files accounting lives HERE (RegisterFile/ReleaseFile/
// ReleaseAll) rather than in the RPC handlers so that session reaps — grace
// expiry, revocation reload, shutdown — decrement the gauge too, and so a
// retried Release (fd already gone) cannot double-decrement.
type SessionMetrics interface {
	SessionsActiveInc()
	SessionsActiveDec()
	OpenFilesInc(volume string)
	OpenFilesDec(volume string)
}

type noopSessionMetrics struct{}

func (noopSessionMetrics) SessionsActiveInc()  {}
func (noopSessionMetrics) SessionsActiveDec()  {}
func (noopSessionMetrics) OpenFilesInc(string) {}
func (noopSessionMetrics) OpenFilesDec(string) {}

type SessionManagerOptions struct {
	GracePeriod time.Duration
	// IdempotencyCacheSize is the per-session LRU capacity for request-id
	// deduplication. 0 uses DefaultIdempotencyCacheSize.
	IdempotencyCacheSize int
	// Metrics is an optional sink for the active-session gauge. Nil
	// substitutes a no-op implementation.
	Metrics SessionMetrics
}

// DefaultGracePeriod is how long the server retains a disconnected client's
// session (fds + idempotency cache) before reaping it, so a brief network drop
// can Resume the same session. Aligned with the client rpc.retry_window default.
// Cost: a dropped client's fds and POSIX locks are held for this long.
const DefaultGracePeriod = 60 * time.Second

// DefaultIdempotencyCacheSize is the per-session LRU capacity for request-id
// deduplication. With kernel writeback enabled the client keeps up to 64 WRITEs
// in flight, each retried with a stable request_id; the LRU must comfortably
// hold the in-flight + recently-completed set or an evicted entry's retry
// re-executes the write. 4096 × ~100 B cached replies ≈ 400 KiB/session,
// cheap insurance against spurious double-writes.
const DefaultIdempotencyCacheSize = 4096

type sessionImpl struct {
	id        string
	principal string // set once at Create; never mutated
	serial    string // canonical cert SerialKey; "" for basic-auth
	fdNum     atomic.Uint64
	files     *xsync.MapOf[uint64, *FileEntry]
	replies   *lru.Cache[string, any]
	sf        singleflight.Group
	metrics   SessionMetrics // never nil; manager injects its sink
}

func (s *sessionImpl) ID() string        { return s.id }
func (s *sessionImpl) Principal() string { return s.principal }
func (s *sessionImpl) Serial() string    { return s.serial }

func (s *sessionImpl) RegisterFile(volume, path string, file nodefs.File) uint64 {
	fd := s.fdNum.Add(1)
	entry := &FileEntry{File: file, Volume: volume, Path: path, Fd: fd}
	entry.refs.Store(1) // the fd table's own ref
	s.files.Store(fd, entry)
	s.metrics.OpenFilesInc(volume)
	return fd
}

func (s *sessionImpl) GetFile(fd uint64) (*FileEntry, bool) {
	return s.files.Load(fd)
}

func (s *sessionImpl) ReleaseFile(fd uint64) {
	entry, ok := s.files.LoadAndDelete(fd)
	if !ok {
		// Already released (e.g. a retried Release RPC): nothing to close and,
		// crucially, nothing to decrement — the gauge only moves on an actual
		// removal.
		return
	}
	// Drop the table's ref only — the close happens whenever the last
	// in-flight op finishes (possibly right here, if none is running). The
	// gauge therefore counts table membership, not actually-open OS fds: it
	// can decrement up to one op-duration before the real close(2).
	entry.ReleaseRef()
	s.metrics.OpenFilesDec(entry.Volume)
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

// ReleaseAll releases every fd in the session, decrementing the per-volume
// open-files gauge for each one, and returns how many fds it released.
func (s *sessionImpl) ReleaseAll() int {
	released := 0
	s.files.Range(func(fd uint64, entry *FileEntry) bool {
		if e, ok := s.files.LoadAndDelete(fd); ok {
			// Same contract as ReleaseFile: drop the table ref; an in-flight op
			// holding its own ref defers the actual close until it finishes.
			e.ReleaseRef()
			s.metrics.OpenFilesDec(e.Volume)
			released++
		}
		return true
	})
	return released
}

type pendingReap struct {
	cancel context.CancelFunc
}

type sessionManagerImpl struct {
	sessions             *xsync.MapOf[string, *sessionImpl]
	reapers              *xsync.MapOf[string, *pendingReap]
	grace                time.Duration
	idempotencyCacheSize int
	metrics              SessionMetrics
	wg                   sync.WaitGroup
}

func NewSessionManager(opts SessionManagerOptions) SessionManager {
	grace := opts.GracePeriod
	if grace == 0 {
		grace = DefaultGracePeriod
	}
	cacheSize := opts.IdempotencyCacheSize
	if cacheSize == 0 {
		cacheSize = DefaultIdempotencyCacheSize
	}
	m := opts.Metrics
	if m == nil {
		m = noopSessionMetrics{}
	}
	return &sessionManagerImpl{
		sessions:             xsync.NewMapOf[string, *sessionImpl](),
		reapers:              xsync.NewMapOf[string, *pendingReap](),
		grace:                grace,
		idempotencyCacheSize: cacheSize,
		metrics:              m,
	}
}

func (m *sessionManagerImpl) Create(principal, serial string) (string, error) {
	id := uuid.NewString()
	replies, err := lru.New[string, any](m.idempotencyCacheSize)
	if err != nil {
		return "", errors.Wrap(err, "create idempotency cache")
	}
	sess := &sessionImpl{
		id:        id,
		principal: principal,
		serial:    serial,
		files:     xsync.NewMapOf[uint64, *FileEntry](),
		replies:   replies,
		metrics:   m.metrics,
	}
	m.sessions.Store(id, sess)
	m.metrics.SessionsActiveInc()
	return id, nil
}

func (m *sessionManagerImpl) Get(id string) (Session, error) {
	sess, ok := m.sessions.Load(id)
	if !ok {
		// Fingerprint, never the raw id — session ids are bearer tokens and this
		// error can propagate into logs and gRPC error strings.
		return nil, errors.Errorf("session not found: %s", common.FingerprintID(id))
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
		timer := time.NewTimer(m.grace)
		defer timer.Stop()
		select {
		case <-ctx.Done():
			return
		case <-timer.C:
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
				m.reap(sess, "grace_expired")
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
			m.reap(sess, "revoked")
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
			m.reap(sess, "shutdown")
		}
		return true
	})
	return nil
}

// reap finalizes a claimed session: bumps the gauge, releases all fds, and
// emits the one log line that makes a reap — a consequential, normal
// lifecycle event under the grace-period model — visible to operators.
// Callers must have already won the sessions.LoadAndDelete claim.
func (m *sessionManagerImpl) reap(sess *sessionImpl, reason string) {
	m.metrics.SessionsActiveDec()
	released := sess.ReleaseAll()
	log.Log.Info("session reaped",
		zap.String("session_fp", common.FingerprintID(sess.id)),
		zap.String("principal", sess.principal),
		zap.Int("fds_released", released),
		zap.String("reason", reason))
}
