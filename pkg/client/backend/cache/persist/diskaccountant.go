package persist

import "sync/atomic"

// diskAccountant tracks the byte total of chunks/ files. Updated by
// WriteChunk / unlinkChunk and seeded on Open by walking chunks/.
// WriteChunk calls enforceDiskBudget after crediting bytes; Over() is
// the stopping condition for that eviction loop.
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
