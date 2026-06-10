package io

import (
	"os"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"

	"github.com/pkg/errors"
	"github.com/stretchr/testify/suite"
)

// IdentityExecutorSuite exercises the executor's pooling, panic and startup
// semantics unprivileged, through the same syscall seams the rollback suite
// stubs. The real credential proof lives in IdentityExecutorVMSuite.
type IdentityExecutorSuite struct {
	suite.Suite

	// Workers apply credentials concurrently at startup, so the counters the
	// stubs bump must be atomic.
	setgroupsCalls atomic.Int64
	setfsgidCalls  atomic.Int64
	setfsuidCalls  atomic.Int64
	getgroupsCalls atomic.Int64

	origSetfsuid     func(int) error
	origSetfsgid     func(int) error
	origSetGroupsRaw func([]uint32) error
	origGetgroups    func() ([]uint32, error)
}

func TestIdentityExecutorSuite(t *testing.T) { suite.Run(t, new(IdentityExecutorSuite)) }

func (s *IdentityExecutorSuite) SetupTest() {
	s.setgroupsCalls.Store(0)
	s.setfsgidCalls.Store(0)
	s.setfsuidCalls.Store(0)
	s.getgroupsCalls.Store(0)
	s.origSetfsuid, s.origSetfsgid = setfsuid, setfsgid
	s.origSetGroupsRaw, s.origGetgroups = setGroupsRaw, getgroups

	setGroupsRaw = func([]uint32) error { s.setgroupsCalls.Add(1); return nil }
	setfsgid = func(int) error { s.setfsgidCalls.Add(1); return nil }
	setfsuid = func(int) error { s.setfsuidCalls.Add(1); return nil }
	getgroups = func() ([]uint32, error) { s.getgroupsCalls.Add(1); return []uint32{0}, nil }
}

// TearDownTest restores the seams. Safe even though worker goroutines may
// still be draining a shutdown: workers only read the seams during startup,
// strictly before their report on the startup channel, which the constructor
// always fully receives before returning — so every seam read happens-before
// this write.
func (s *IdentityExecutorSuite) TearDownTest() {
	setfsuid, setfsgid = s.origSetfsuid, s.origSetfsgid
	setGroupsRaw, getgroups = s.origSetGroupsRaw, s.origGetgroups
}

func (s *IdentityExecutorSuite) testIdentity() Identity {
	return Identity{Uid: 1000, Gid: 1000, Gids: []uint32{1000, 2000}}
}

func (s *IdentityExecutorSuite) newExecutor(workers int) *identityExecutor {
	e, err := newIdentityExecutor(s.testIdentity(), workers)
	s.Require().NoError(err)
	s.T().Cleanup(e.shutdown)
	return e
}

// TestDoRunsTaskToCompletion: do must not return before fn has fully run —
// proven by fn depositing into a buffered channel that must be drainable the
// instant do returns.
func (s *IdentityExecutorSuite) TestDoRunsTaskToCompletion() {
	e := s.newExecutor(2)
	ch := make(chan int, 1)
	e.do(func() { ch <- 42 })
	select {
	case v := <-ch:
		s.Equal(42, v)
	default:
		s.Fail("do returned before fn completed")
	}
}

// TestConcurrentDosAllComplete: 100 concurrent submitters against a small
// pool — every task runs exactly once and every do returns (no lost tasks;
// -race covers the memory side).
func (s *IdentityExecutorSuite) TestConcurrentDosAllComplete() {
	e := s.newExecutor(4)
	var ran atomic.Int64
	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			e.do(func() { ran.Add(1) })
		}()
	}
	wg.Wait()
	s.EqualValues(100, ran.Load())
}

// TestPanicPropagatesToCallerAndPoolSurvives: a panicking fn re-raises on the
// do caller, and the SAME worker (pool of one) keeps serving — a dead worker
// would make the follow-up do hang, silently shrinking the pool in prod.
func (s *IdentityExecutorSuite) TestPanicPropagatesToCallerAndPoolSurvives() {
	e := s.newExecutor(1)
	s.Require().PanicsWithValue("boom", func() {
		e.do(func() { panic("boom") })
	})
	ran := false
	e.do(func() { ran = true })
	s.True(ran, "the worker that recovered the panic must keep serving tasks")
}

// TestStartupFailureReturnsError: any worker failing the credential switch
// fails the whole constructor so the caller can fall back to per-op
// switching.
func (s *IdentityExecutorSuite) TestStartupFailureReturnsError() {
	setGroupsRaw = func([]uint32) error { return errors.New("boom") }
	e, err := newIdentityExecutor(s.testIdentity(), 4)
	s.Require().Error(err)
	s.ErrorContains(err, "boom")
	s.Nil(e)
}

// TestStartupPartialFailureReturnsError: only ONE of four workers fails —
// the constructor must still error (and the survivors exit via the closed
// task channel rather than serving a short-handed pool).
func (s *IdentityExecutorSuite) TestStartupPartialFailureReturnsError() {
	var calls atomic.Int64
	setfsuid = func(int) error {
		if calls.Add(1) == 1 {
			return errors.New("boom")
		}
		return nil
	}
	e, err := newIdentityExecutor(s.testIdentity(), 4)
	s.Require().Error(err)
	s.Nil(e)
}

// TestCredSeamsInvokedOncePerWorkerNotPerDo is the core property: the
// credential syscalls run exactly once per worker at startup, and NEVER per
// do — that is the entire point of the executor versus changeIdentity.
func (s *IdentityExecutorSuite) TestCredSeamsInvokedOncePerWorkerNotPerDo() {
	const workers = 4
	e := s.newExecutor(workers)
	for i := 0; i < 50; i++ {
		e.do(func() {})
	}
	s.EqualValues(workers, s.setgroupsCalls.Load(), "setgroups: once per worker, not per do")
	s.EqualValues(workers, s.setfsgidCalls.Load(), "setfsgid: once per worker, not per do")
	s.EqualValues(workers, s.setfsuidCalls.Load(), "setfsuid: once per worker, not per do")
	s.EqualValues(0, s.getgroupsCalls.Load(),
		"apply-only path must skip the per-op original-groups save entirely")
}

// TestRejectsNonPositiveWorkers: a pool of zero threads can never serve a do.
func (s *IdentityExecutorSuite) TestRejectsNonPositiveWorkers() {
	for _, n := range []int{0, -1} {
		e, err := newIdentityExecutor(s.testIdentity(), n)
		s.Require().Error(err)
		s.Nil(e)
	}
}

// IdentityExecutorVMSuite proves against the real kernel that worker threads
// carry the executor identity while the caller's thread stays untouched.
// Destructive-ish (needs root, switches thread creds), so it is VM-gated per
// repo convention.
type IdentityExecutorVMSuite struct{ suite.Suite }

func TestIdentityExecutorVMSuite(t *testing.T) { suite.Run(t, new(IdentityExecutorVMSuite)) }

func (s *IdentityExecutorVMSuite) SetupSuite() {
	if os.Getenv("GMOUNTIE_E2E_VM") == "" {
		s.T().Skip("set GMOUNTIE_E2E_VM=1 to run real credential-switch tests (root, VM only)")
	}
	if os.Geteuid() != 0 {
		s.T().Skip("requires root to switch fs credentials")
	}
}

// threadStatus returns the fsuid, fsgid and supplementary groups of the
// CALLING thread from /proc/thread-self/status. Callers must be pinned to
// the thread they mean to inspect.
func threadStatus(s *suite.Suite) (fsuid, fsgid int, groups string) {
	b, err := os.ReadFile("/proc/thread-self/status")
	s.Require().NoError(err)
	fsuid, fsgid = -1, -1
	for _, line := range strings.Split(string(b), "\n") {
		f := strings.Fields(line)
		if len(f) == 5 && (f[0] == "Uid:" || f[0] == "Gid:") {
			// Uid:/Gid: lines are "real effective saved fs"; index 4 is the fs id.
			v, err := strconv.Atoi(f[4])
			s.Require().NoError(err)
			if f[0] == "Uid:" {
				fsuid = v
			} else {
				fsgid = v
			}
		}
		if strings.HasPrefix(line, "Groups:") {
			groups = strings.TrimSpace(strings.TrimPrefix(line, "Groups:"))
		}
	}
	s.Require().NotEqual(-1, fsuid, "Uid: line not found in thread status")
	s.Require().NotEqual(-1, fsgid, "Gid: line not found in thread status")
	return fsuid, fsgid, groups
}

func (s *IdentityExecutorVMSuite) TestWorkerThreadsCarryIdentityCallerUntouched() {
	e, err := newIdentityExecutor(Identity{Uid: 7000, Gid: 8000, Gids: []uint32{8000, 6000}}, 2)
	s.Require().NoError(err)
	defer e.shutdown()

	var fsuid, fsgid int
	var groups string
	e.do(func() { fsuid, fsgid, groups = threadStatus(&s.Suite) })
	s.Equal(7000, fsuid, "worker thread fsuid must be the executor identity")
	s.Equal(8000, fsgid, "worker thread fsgid must be the executor identity")
	s.Contains(strings.Fields(groups), "6000", "worker thread supplementary groups must be applied")

	// The calling goroutine's own thread must be untouched: pin it and read.
	runtime.LockOSThread()
	defer runtime.UnlockOSThread()
	cu, cg, _ := threadStatus(&s.Suite)
	s.Equal(syscall.Geteuid(), cu, "caller thread fsuid must remain the process euid")
	s.Equal(syscall.Getegid(), cg, "caller thread fsgid must remain the process egid")
}
