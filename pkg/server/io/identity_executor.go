package io

import (
	"runtime/debug"

	"go.gmountie.dev/gmountie/pkg/utils/log"

	"github.com/pkg/errors"
	"go.uber.org/zap"
)

// identityExecutor runs filesystem ops on OS threads whose fs credentials
// were switched ONCE at startup, replacing the per-op LockOSThread + ~9
// credential syscalls changeIdentity pays (getgroups save, setgroups, set+
// probe fsgid, set+probe fsuid, then the mirror-image restores). Only valid
// for CONSTANT identities (squash / static volume modes) — per-principal
// modes keep the per-op switch.
//
// Lifetime: the production lifetime of an executor is the process. Workers
// permanently pin their OS threads (LockOSThread with no unlock) and hold
// foreign fs credentials forever, so those Ms are dedicated and must never
// re-enter the scheduler pool. shutdown exists for tests only.
type identityExecutor struct {
	tasks chan func()
}

// newIdentityExecutor starts workers OS threads, switches each to id's
// credentials via the shared apply-only path (applyCreds, no save/restore),
// and returns a pool ready to serve do calls.
//
// If ANY worker fails the credential switch, the constructor waits for every
// worker to report, closes the task channel, and returns the first error so
// the caller can fall back to per-op switching. Workers that DID switch
// successfully fall out of their (empty, closed) task loop and exit; their
// locked, credential-switched threads are discarded by the runtime rather
// than reused — exactly the changeIdentity taint policy.
func newIdentityExecutor(id Identity, workers int) (*identityExecutor, error) {
	if workers <= 0 {
		return nil, errors.Errorf("identity executor: workers must be positive, got %d", workers)
	}
	e := &identityExecutor{tasks: make(chan func())}
	// Buffered so every worker can report without blocking even after the
	// constructor has already returned an error.
	startup := make(chan error, workers)
	for i := 0; i < workers; i++ {
		go e.worker(id, startup)
	}
	// Wait for ALL workers, not just the first failure: a worker still inside
	// applyCreds is still reading the syscall seams, and returning early would
	// race with anything that reassigns them (the unprivileged test suites).
	var firstErr error
	for i := 0; i < workers; i++ {
		if err := <-startup; err != nil && firstErr == nil {
			firstErr = err
		}
	}
	if firstErr != nil {
		close(e.tasks)
		return nil, errors.Wrap(firstErr, "identity executor: credential switch failed")
	}
	return e, nil
}

// worker pins its OS thread permanently, applies the executor identity with
// no save/restore (the thread never goes back), reports the outcome on
// startup, then serves tasks until the channel closes. The thread stays
// locked for the goroutine's whole life, so the runtime dedicates the M to
// this credentialed thread and destroys it if the goroutine ever exits.
func (e *identityExecutor) worker(id Identity, startup chan<- error) {
	lockOSThread() // never unlocked: the M is dedicated to this credentialed thread
	if _, err := applyCreds(&id, nil); err != nil {
		// Partially-switched, locked thread: the goroutine exits and the
		// runtime discards the M — same leak policy as a failed rollback.
		startup <- err
		return
	}
	startup <- nil
	for fn := range e.tasks {
		fn()
	}
}

// do submits fn to the pool and blocks until it has run to completion on a
// pre-credentialed worker thread.
//
// Panic safety: a panic inside fn is recovered ON THE WORKER (so the worker
// survives and the pool never silently shrinks) and re-raised here on the
// CALLING goroutine after the wait — the caller observes the same panic it
// would have seen running fn inline. The worker-side stack is logged before
// re-raising because the re-panic only carries the caller's stack.
func (e *identityExecutor) do(fn func()) {
	done := make(chan struct{})
	var (
		panicked any
		stack    []byte
	)
	e.tasks <- func() {
		defer close(done)
		defer func() {
			if p := recover(); p != nil {
				panicked = p
				stack = debug.Stack()
			}
		}()
		fn()
	}
	<-done // also orders the panicked/stack writes before the reads below
	if panicked != nil {
		log.Log.Error("identity executor task panicked; re-raising on caller",
			zap.Any("panic", panicked), zap.ByteString("worker_stack", stack))
		panic(panicked)
	}
}

// shutdown closes the task channel so the workers exit. TESTS ONLY — in
// production the executor lives for the whole process. The exited workers'
// OS threads remain locked and credential-switched; the runtime discards
// those Ms. Not safe to call concurrently with do.
func (e *identityExecutor) shutdown() {
	close(e.tasks)
}
