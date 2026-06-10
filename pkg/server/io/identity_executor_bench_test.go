package io

import "testing"

// These benchmarks measure DISPATCH overhead only: the credential syscall
// seams are stubbed to no-ops, so the per-op numbers EXCLUDE the ~9 real
// credential syscalls (setgroups, set+probe fsgid, set+probe fsuid, and the
// mirror-image restores) the per-op path pays in production — real-kernel
// numbers belong to the VM / release perf series (Bencher). What remains is
// the structural cost each path wraps around an op:
//
//   - per-op switch: LockOSThread/UnlockOSThread, the getgroups save, seam
//     call plumbing, and the cleanup closure;
//   - executor: a channel round-trip to a pre-credentialed worker plus the
//     done-channel allocation in do.
func BenchmarkPerOpCredentialSwitch(b *testing.B) {
	restore := stubCredSeamsForBench()
	defer restore()
	id := Identity{Uid: 1000, Gid: 1000, Gids: []uint32{1000, 2000}}
	var x int
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		cleanup, err := changeIdentity(&id)
		if err != nil {
			b.Fatal(err)
		}
		x++ // the trivial "op"
		cleanup()
	}
	_ = x
}

func BenchmarkExecutorDispatch(b *testing.B) {
	restore := stubCredSeamsForBench()
	defer restore()
	id := Identity{Uid: 1000, Gid: 1000, Gids: []uint32{1000, 2000}}
	e, err := newIdentityExecutor(id, 4)
	if err != nil {
		b.Fatal(err)
	}
	defer e.shutdown()
	var x int
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		e.do(func() { x++ }) // the trivial "op"
	}
	_ = x
}

// stubCredSeamsForBench replaces the credential syscall seams with no-ops so
// both benchmarks measure dispatch, not kernel work (which would also fail
// unprivileged). LockOSThread/UnlockOSThread stay REAL — the thread pin is
// genuine per-op dispatch cost the executor eliminates.
func stubCredSeamsForBench() (restore func()) {
	origSetfsuid, origSetfsgid := setfsuid, setfsgid
	origSetGroupsRaw, origGetgroups := setGroupsRaw, getgroups
	setfsuid = func(int) error { return nil }
	setfsgid = func(int) error { return nil }
	setGroupsRaw = func([]uint32) error { return nil }
	getgroups = func() ([]uint32, error) { return []uint32{0}, nil }
	return func() {
		setfsuid, setfsgid = origSetfsuid, origSetfsgid
		setGroupsRaw, getgroups = origSetGroupsRaw, origGetgroups
	}
}
