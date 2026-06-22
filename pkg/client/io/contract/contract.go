// Package contract provides RunBackendContract, a reusable, impl-agnostic
// conformance suite for io.FileSystemBackend. Any layer (the in-memory
// reference, the metrics observer, the cache, and future layers like the WAL)
// can be run through it to prove it honors the FileSystemBackend behavioral
// contract documented on the interface.
//
// Assertions are pinned at close-to-open consistency so that SEMANTIC layers
// (the cache) can satisfy them: a value written and then re-read through the
// same backend instance (with the writing handle still open / just released)
// must be observable. The suite does not assert any caching, retry, or
// invalidation behavior — those are layer-specific and covered by each
// layer's own tests.
package contract

import (
	"context"
	"syscall"
	"testing"

	"go.gmountie.dev/gmountie/pkg/client/io"
	"go.gmountie.dev/gmountie/pkg/proto"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// RunBackendContract runs the FileSystemBackend behavioral-contract assertions
// against the backend produced by newBackend, as a tree of subtests under
// name. newBackend is called ONCE: the backend is shared across the subtests
// (each uses unique path names to stay independent) and Close()d at the end —
// re-constructing per subtest would, for disk-backed layers, re-open the same
// cache directory and deadlock on the file lock.
func RunBackendContract(t *testing.T, name string, newBackend func() io.FileSystemBackend) {
	t.Helper()
	b := newBackend()
	t.Cleanup(func() {
		assert.NoError(t, b.Close(), "Close must return cleanly")
	})

	ctx := context.Background()

	t.Run(name+"/CreateWriteReadRoundtrip", func(t *testing.T) {
		fh, attr, st := b.Create(ctx, "", "rt.txt", uint32(0), 0o644)
		require.Equal(t, proto.FsError_FS_OK, st)
		require.NotNil(t, fh)
		require.NotNil(t, attr)

		payload := []byte("hello, contract")
		n, st := b.Write(ctx, fh, 0, payload)
		require.Equal(t, proto.FsError_FS_OK, st)
		require.Equal(t, uint32(len(payload)), n)

		// Write at an offset extends the file.
		tail := []byte("TAIL")
		n2, st := b.Write(ctx, fh, int64(len(payload)), tail)
		require.Equal(t, proto.FsError_FS_OK, st)
		require.Equal(t, uint32(len(tail)), n2)

		// Read the whole thing back.
		buf := make([]byte, len(payload)+len(tail))
		got, st := b.Read(ctx, fh, 0, buf)
		require.Equal(t, proto.FsError_FS_OK, st)
		require.Equal(t, len(buf), got)
		assert.Equal(t, append(append([]byte{}, payload...), tail...), buf)

		// Read at an offset returns the offset bytes.
		off := int64(len(payload))
		obuf := make([]byte, len(tail))
		got, st = b.Read(ctx, fh, off, obuf)
		require.Equal(t, proto.FsError_FS_OK, st)
		require.Equal(t, len(tail), got)
		assert.Equal(t, tail, obuf)

		require.Equal(t, proto.FsError_FS_OK, b.Flush(ctx, fh))
		require.Equal(t, proto.FsError_FS_OK, b.Release(ctx, fh))
	})

	t.Run(name+"/StatReflectsWrittenSize", func(t *testing.T) {
		fh, _, st := b.Create(ctx, "", "size.txt", uint32(0), 0o644)
		require.Equal(t, proto.FsError_FS_OK, st)
		payload := []byte("0123456789")
		_, st = b.Write(ctx, fh, 0, payload)
		require.Equal(t, proto.FsError_FS_OK, st)
		require.Equal(t, proto.FsError_FS_OK, b.Release(ctx, fh))

		a, st := b.Stat(ctx, "size.txt")
		require.Equal(t, proto.FsError_FS_OK, st)
		require.NotNil(t, a)
		assert.Equal(t, uint64(len(payload)), a.Size)
	})

	t.Run(name+"/LookupCreatedEntry", func(t *testing.T) {
		fh, _, st := b.Create(ctx, "", "lookup.txt", uint32(0), 0o644)
		require.Equal(t, proto.FsError_FS_OK, st)
		require.Equal(t, proto.FsError_FS_OK, b.Release(ctx, fh))

		a, st := b.Lookup(ctx, "", "lookup.txt")
		require.Equal(t, proto.FsError_FS_OK, st)
		require.NotNil(t, a)
	})

	t.Run(name+"/ENOENTOnMissing", func(t *testing.T) {
		_, st := b.Stat(ctx, "does-not-exist")
		assert.Equal(t, proto.FsError_FS_ENOENT, st, "Stat of a missing path")

		_, st = b.Lookup(ctx, "", "does-not-exist")
		assert.Equal(t, proto.FsError_FS_ENOENT, st, "Lookup of a missing name")

		_, st = b.Open(ctx, "does-not-exist", 0)
		assert.Equal(t, proto.FsError_FS_ENOENT, st, "Open of a missing path")
	})

	t.Run(name+"/MkdirThenListDir", func(t *testing.T) {
		_, st := b.Mkdir(ctx, "dir", 0o755)
		require.Equal(t, proto.FsError_FS_OK, st)
		fh, _, st := b.Create(ctx, "dir", "child.txt", uint32(0), 0o644)
		require.Equal(t, proto.FsError_FS_OK, st)
		require.Equal(t, proto.FsError_FS_OK, b.Release(ctx, fh))

		entries, st := b.ListDir(ctx, "dir")
		require.Equal(t, proto.FsError_FS_OK, st)
		names := entryNames(entries)
		assert.Contains(t, names, "child.txt")
	})

	t.Run(name+"/RmdirNonEmptyThenEmpty", func(t *testing.T) {
		_, st := b.Mkdir(ctx, "rmd", 0o755)
		require.Equal(t, proto.FsError_FS_OK, st)
		fh, _, st := b.Create(ctx, "rmd", "occupied.txt", uint32(0), 0o644)
		require.Equal(t, proto.FsError_FS_OK, st)
		require.Equal(t, proto.FsError_FS_OK, b.Release(ctx, fh))

		st = b.Rmdir(ctx, "rmd")
		assert.Equal(t, proto.FsError_FS_ENOTEMPTY, st, "Rmdir of a non-empty dir")

		require.Equal(t, proto.FsError_FS_OK, b.Unlink(ctx, "rmd/occupied.txt"))
		st = b.Rmdir(ctx, "rmd")
		assert.Equal(t, proto.FsError_FS_OK, st, "Rmdir of an empty dir")

		_, st = b.Stat(ctx, "rmd")
		assert.Equal(t, proto.FsError_FS_ENOENT, st, "removed dir is gone")
	})

	t.Run(name+"/RenameMovesFile", func(t *testing.T) {
		fh, _, st := b.Create(ctx, "", "ren-old.txt", uint32(0), 0o644)
		require.Equal(t, proto.FsError_FS_OK, st)
		payload := []byte("rename me")
		_, st = b.Write(ctx, fh, 0, payload)
		require.Equal(t, proto.FsError_FS_OK, st)
		require.Equal(t, proto.FsError_FS_OK, b.Release(ctx, fh))

		require.Equal(t, proto.FsError_FS_OK, b.Rename(ctx, "ren-old.txt", "ren-new.txt"))

		_, st = b.Stat(ctx, "ren-old.txt")
		assert.Equal(t, proto.FsError_FS_ENOENT, st, "old path gone after rename")
		a, st := b.Stat(ctx, "ren-new.txt")
		require.Equal(t, proto.FsError_FS_OK, st, "new path present after rename")
		assert.Equal(t, uint64(len(payload)), a.Size, "contents survive rename")
	})

	t.Run(name+"/UnlinkRemoves", func(t *testing.T) {
		fh, _, st := b.Create(ctx, "", "unlink.txt", uint32(0), 0o644)
		require.Equal(t, proto.FsError_FS_OK, st)
		require.Equal(t, proto.FsError_FS_OK, b.Release(ctx, fh))

		require.Equal(t, proto.FsError_FS_OK, b.Unlink(ctx, "unlink.txt"))
		_, st = b.Stat(ctx, "unlink.txt")
		assert.Equal(t, proto.FsError_FS_ENOENT, st, "unlinked file is gone")
	})

	t.Run(name+"/SymlinkReadlinkRoundtrip", func(t *testing.T) {
		target := "some/target/path"
		_, st := b.Symlink(ctx, target, "link")
		require.Equal(t, proto.FsError_FS_OK, st)

		got, st := b.Readlink(ctx, "link")
		require.Equal(t, proto.FsError_FS_OK, st)
		assert.Equal(t, target, got)
	})

	t.Run(name+"/XAttrRoundtrip", func(t *testing.T) {
		fh, _, st := b.Create(ctx, "", "xattr.txt", uint32(0), 0o644)
		require.Equal(t, proto.FsError_FS_OK, st)
		require.Equal(t, proto.FsError_FS_OK, b.Release(ctx, fh))

		const key = "user.contract"
		val := []byte("xv")
		require.Equal(t, proto.FsError_FS_OK, b.SetXAttr(ctx, "xattr.txt", key, val, 0))

		got, st := b.GetXAttr(ctx, "xattr.txt", key)
		require.Equal(t, proto.FsError_FS_OK, st)
		assert.Equal(t, val, got)

		names, st := b.ListXAttr(ctx, "xattr.txt")
		require.Equal(t, proto.FsError_FS_OK, st)
		assert.Contains(t, names, key)

		require.Equal(t, proto.FsError_FS_OK, b.RemoveXAttr(ctx, "xattr.txt", key))
		_, st = b.GetXAttr(ctx, "xattr.txt", key)
		assert.Equal(t, proto.FsError_FS_ENO_XATTR, st, "removed xattr is gone")
	})

	t.Run(name+"/SetAttrSizeTruncation", func(t *testing.T) {
		fh, _, st := b.Create(ctx, "", "trunc.txt", uint32(0), 0o644)
		require.Equal(t, proto.FsError_FS_OK, st)
		_, st = b.Write(ctx, fh, 0, []byte("0123456789"))
		require.Equal(t, proto.FsError_FS_OK, st)
		require.Equal(t, proto.FsError_FS_OK, b.Release(ctx, fh))

		_, st = b.SetAttr(ctx, "trunc.txt", io.SetAttrIn{Valid: io.FATTR_SIZE, Size: 4})
		require.Equal(t, proto.FsError_FS_OK, st)

		a, st := b.Stat(ctx, "trunc.txt")
		require.Equal(t, proto.FsError_FS_OK, st)
		assert.Equal(t, uint64(4), a.Size, "SetAttr size truncation reflected in Stat")
	})

	t.Run(name+"/CreateOExclOnExistingIsEEXIST", func(t *testing.T) {
		fh, _, st := b.Create(ctx, "", "excl.txt", uint32(0), 0o644)
		require.Equal(t, proto.FsError_FS_OK, st)
		require.Equal(t, proto.FsError_FS_OK, b.Release(ctx, fh))

		_, _, st = b.Create(ctx, "", "excl.txt", uint32(syscall.O_CREAT|syscall.O_EXCL), 0o644)
		assert.Equal(t, proto.FsError_FS_EEXIST, st, "Create O_EXCL on an existing name")
	})

	t.Run(name+"/GetAttrIfChangedDetectsChange", func(t *testing.T) {
		fh, _, st := b.Create(ctx, "", "rev.txt", uint32(0), 0o644)
		require.Equal(t, proto.FsError_FS_OK, st)
		_, st = b.Write(ctx, fh, 0, []byte("v1"))
		require.Equal(t, proto.FsError_FS_OK, st)
		require.Equal(t, proto.FsError_FS_OK, b.Release(ctx, fh))

		// Source the CURRENT version authoritatively from the backend itself:
		// knownVersion 0 won't match, so this returns the real attrs + version.
		// (A cached Stat may serve an optimistic attr whose Version lags the
		// backend's, so we don't use it as the revalidation baseline.)
		cur, notModified, st := b.GetAttrIfChanged(ctx, "rev.txt", 0)
		require.Equal(t, proto.FsError_FS_OK, st)
		require.False(t, notModified, "version 0 never matches -> reports modified with attrs")
		require.NotNil(t, cur)
		baseVersion := cur.Version

		// Same version -> not modified: (nil, true, OK).
		got, notModified, st := b.GetAttrIfChanged(ctx, "rev.txt", baseVersion)
		require.Equal(t, proto.FsError_FS_OK, st)
		assert.True(t, notModified, "unchanged version reports notModified==true")
		assert.Nil(t, got, "not-modified returns nil attrs")

		// Mutate, then the prior knownVersion -> modified: (attr, false, OK).
		fh2, st := b.Open(ctx, "rev.txt", uint32(syscall.O_WRONLY))
		require.Equal(t, proto.FsError_FS_OK, st)
		_, st = b.Write(ctx, fh2, 0, []byte("v2-longer"))
		require.Equal(t, proto.FsError_FS_OK, st)
		require.Equal(t, proto.FsError_FS_OK, b.Release(ctx, fh2))

		got, notModified, st = b.GetAttrIfChanged(ctx, "rev.txt", baseVersion)
		require.Equal(t, proto.FsError_FS_OK, st)
		assert.False(t, notModified, "stale version reports notModified==false (modified)")
		require.NotNil(t, got, "modified returns fresh attrs")

		// Missing path -> ENOENT.
		_, _, st = b.GetAttrIfChanged(ctx, "rev-missing.txt", 0)
		assert.Equal(t, proto.FsError_FS_ENOENT, st)
	})

	t.Run(name+"/HandleUnwrapTerminatesAtLeaf", func(t *testing.T) {
		fh, _, st := b.Create(ctx, "", "unwrap.txt", uint32(0), 0o644)
		require.Equal(t, proto.FsError_FS_OK, st)
		t.Cleanup(func() { _ = b.Release(ctx, fh) })

		cur := fh
		// Walk the chain; it must terminate (cur.Unwrap() == cur) within a
		// sane bound, never loop.
		const maxDepth = 16
		var leaf io.FileHandle
		for i := 0; i < maxDepth; i++ {
			next := cur.Unwrap()
			require.NotNil(t, next, "Unwrap must not return nil")
			if next == cur {
				leaf = cur
				break
			}
			cur = next
		}
		require.NotNil(t, leaf, "Unwrap chain must terminate at a leaf within %d hops", maxDepth)
		assert.Equal(t, leaf, leaf.Unwrap(), "leaf handle's Unwrap returns itself")
		assert.NotEmpty(t, leaf.Path(), "handle reports a path")
	})

	t.Run(name+"/ConcurrentOpsAreSafe", func(t *testing.T) {
		// Asserts the contract's concurrency-safety bullet. Meaningful under
		// `go test -race`: many goroutines hammering create/write/read/stat/
		// unlink on independent paths must not data-race or panic.
		const workers = 16
		done := make(chan struct{}, workers)
		for w := 0; w < workers; w++ {
			go func(id int) {
				defer func() { done <- struct{}{} }()
				name := concurrentName(id)
				fh, _, st := b.Create(ctx, "", name, uint32(0), 0o644)
				if st != proto.FsError_FS_OK {
					return
				}
				_, _ = b.Write(ctx, fh, 0, []byte("concurrent"))
				buf := make([]byte, 10)
				_, _ = b.Read(ctx, fh, 0, buf)
				_, _ = b.Stat(ctx, name)
				_ = b.Release(ctx, fh)
				_ = b.Unlink(ctx, name)
			}(w)
		}
		for w := 0; w < workers; w++ {
			<-done
		}
	})
}

func entryNames(entries []io.DirEntryPlus) []string {
	names := make([]string, 0, len(entries))
	for _, e := range entries {
		names = append(names, e.Name)
	}
	return names
}

func concurrentName(id int) string {
	return "conc-" + string(rune('a'+id%26)) + "-" + itoa(id)
}

func itoa(i int) string {
	if i == 0 {
		return "0"
	}
	var buf [20]byte
	pos := len(buf)
	for i > 0 {
		pos--
		buf[pos] = byte('0' + i%10)
		i /= 10
	}
	return string(buf[pos:])
}
