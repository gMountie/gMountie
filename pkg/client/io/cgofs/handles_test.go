package cgofs

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/suite"
	gio "go.gmountie.dev/gmountie/pkg/client/io"
)

// stubHandle is a minimal io.FileHandle for table tests.
type stubHandle struct{ p string }

func (h *stubHandle) Path() string           { return h.p }
func (h *stubHandle) Unwrap() gio.FileHandle { return h }

type HandleTableSuite struct{ suite.Suite }

func TestHandleTableSuite(t *testing.T) { suite.Run(t, new(HandleTableSuite)) }

func (s *HandleTableSuite) TestAddGetRemove() {
	tbl := newHandleTable()
	h := &stubHandle{p: "a/b"}
	id := tbl.add(h)
	got, ok := tbl.get(id)
	s.True(ok)
	s.Equal(h, got)
	removed, ok := tbl.remove(id)
	s.True(ok)
	s.Equal(h, removed)
	_, ok = tbl.get(id)
	s.False(ok)
}

func (s *HandleTableSuite) TestIDsAreUniqueAndConcurrent() {
	tbl := newHandleTable()
	const n = 200
	ids := make(chan uint64, n)
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() { defer wg.Done(); ids <- tbl.add(&stubHandle{}) }()
	}
	wg.Wait()
	close(ids)
	seen := map[uint64]bool{}
	for id := range ids {
		s.False(seen[id], "duplicate id %d", id)
		seen[id] = true
	}
	s.Len(seen, n)
}
