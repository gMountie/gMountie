package persist

import "sync/atomic"

// diskAccountant tracks the byte total of chunks/ files. Updated by
// WriteChunk / unlinkChunk and seeded on Open by walking chunks/.
// The budget is advisory — eviction is driven by the persist-package
// LRU, not the accountant; the accountant exposes Used for
// observability and provides Over() as the eviction loop's stopping
// condition.
type diskAccountant struct {
	used   int64 // atomic
	budget int64
}

func newDiskAccountant(budget int64) *diskAccountant {
	return &diskAccountant{budget: budget}
}

func (a *diskAccountant) add(n int64)   { atomic.AddInt64(&a.used, n) }
func (a *diskAccountant) Used() int64   { return atomic.LoadInt64(&a.used) }
func (a *diskAccountant) Budget() int64 { return a.budget }

// Over returns the bytes over budget, or 0 if under (or budget<=0).
func (a *diskAccountant) Over() int64 {
	if a.budget <= 0 {
		return 0
	}
	o := a.Used() - a.budget
	if o < 0 {
		return 0
	}
	return o
}
