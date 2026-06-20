package cgofs

import (
	"sync"

	gio "go.gmountie.dev/gmountie/pkg/client/io"
)

// handleTable maps cgofuse's uint64 file handles to io.FileHandle objects.
// cgofuse hands back a uint64 per open; go-fuse gave us an object, so the
// cgofuse adapter owns this mapping itself. Safe for concurrent use.
type handleTable struct {
	mu   sync.Mutex
	next uint64
	m    map[uint64]gio.FileHandle
}

func newHandleTable() *handleTable {
	return &handleTable{next: 1, m: make(map[uint64]gio.FileHandle)}
}

// add stores fh and returns a fresh non-zero handle id.
func (t *handleTable) add(fh gio.FileHandle) uint64 {
	t.mu.Lock()
	defer t.mu.Unlock()
	id := t.next
	t.next++
	t.m[id] = fh
	return id
}

func (t *handleTable) get(id uint64) (gio.FileHandle, bool) {
	t.mu.Lock()
	defer t.mu.Unlock()
	fh, ok := t.m[id]
	return fh, ok
}

func (t *handleTable) remove(id uint64) (gio.FileHandle, bool) {
	t.mu.Lock()
	defer t.mu.Unlock()
	fh, ok := t.m[id]
	if ok {
		delete(t.m, id)
	}
	return fh, ok
}
