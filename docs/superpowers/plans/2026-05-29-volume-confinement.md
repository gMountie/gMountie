# Phase 2 — Volume confinement — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Make the server's loopback filesystem **incapable** of resolving a wire path outside the volume root. A symlink-escape (`evil -> /etc`) or `..`-traversal returns `EXDEV` / `EACCES` instead of operating on the server's own filesystem. This is a security prerequisite for Phase 3 (`dac_override`) and an independent latent-bug fix today.

**Architecture:** Replace `pathfs.NewLoopbackFileSystem(path)` with a `ConfinedLoopbackFileSystem` (this package) that opens the volume root once as an `O_PATH | O_DIRECTORY` dirfd at construction and resolves every wire `name` beneath it via `openat2(RESOLVE_BENEATH | RESOLVE_NO_MAGICLINKS)`. A single resolver helper `resolveBeneath(rootFd, name) → (parentFd, leaf, err)` returns the parent dirfd + final component; each pathfs op becomes a thin wrapper around the matching `*at` syscall on `(parentFd, leaf)`. The data path (Read/Write) rides the confined fd returned by `Open`/`Create`, so it inherits the confinement automatically. The pathfs.FileSystem interface is preserved so the identity-bound wrapper (`identityBoundFS`) keeps composing.

**Tech Stack:** Go 1.26, `golang.org/x/sys/unix` (`Openat2`, `OpenHow`, `RESOLVE_BENEATH`, `RESOLVE_NO_MAGICLINKS`, `Fstatat`, `Mkdirat`, `Unlinkat`, `Renameat2`, `Symlinkat`, `Readlinkat`, `Linkat`, `Fchmodat`, `Fchownat`, `UtimesNanoAt`, `Faccessat`), go-fuse v2 `pathfs.FileSystem`, testify suites. Kernel ≥5.6 (VM is 6.8). Runs Linux-only (matches the rest of the server).

**Reference spec:** `docs/superpowers/specs/2026-05-27-identity-permissions-design.md` §3.10.

**Scope:** the `ConfinedLoopbackFileSystem` (~22 pathfs ops fd-relative), wiring into `NewLocalFilesystem`, unit tests for the resolver + each op family, VM e2e symlink-escape + traversal regression test. **Out of scope:** changes to `identityBoundFS` (it composes unchanged), the data path (the fd returned by `Open` is already confined), `dac_override`/Phase 3 (gated on this PR).

**Testing notes:** unit-test the resolver and pure logic in the sandbox; the syscall-level tests work in the sandbox because `openat2` is unprivileged. **The e2e symlink-escape regression test** verifies that `evil -> /etc` and `../../etc` paths return EXDEV/EACCES — runs in the sandbox (no FUSE needed for the loopback unit), and is also exercised by the existing FUSE e2e on the VM.

---

## Task 1: PROOF — `openat2(RESOLVE_BENEATH)` blocks escape and permits in-tree access

**Files:** Create `pkg/server/io/confined_proof_test.go` (sandbox-runnable; no root/FUSE needed).

This pins the foundational assumption empirically before writing the wrapper, in the same spirit as Phase 1a's `BoundFSCredsSuite` proof. If `openat2(RESOLVE_BENEATH)` doesn't block a symlink escape on this kernel/go-version, **stop and reconsider**.

- [ ] **Step 1: Write the proof test** (testify suite):

```go
package io

import (
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/suite"
	"golang.org/x/sys/unix"
)

type ConfinedProofSuite struct{ suite.Suite }

func TestConfinedProofSuite(t *testing.T) { suite.Run(t, new(ConfinedProofSuite)) }

func (s *ConfinedProofSuite) openat2Beneath(rootFd int, name string, flags uint64) (int, error) {
	how := &unix.OpenHow{
		Flags:   flags,
		Resolve: unix.RESOLVE_BENEATH | unix.RESOLVE_NO_MAGICLINKS,
	}
	return unix.Openat2(rootFd, name, how)
}

func (s *ConfinedProofSuite) openRoot(p string) int {
	fd, err := unix.Open(p, unix.O_PATH|unix.O_DIRECTORY|unix.O_CLOEXEC, 0)
	s.Require().NoError(err)
	return fd
}

// TestInTreeFileResolves: a plain file inside the volume root resolves cleanly.
func (s *ConfinedProofSuite) TestInTreeFileResolves() {
	root := s.T().TempDir()
	s.Require().NoError(os.WriteFile(filepath.Join(root, "ok.txt"), []byte("hi"), 0o644))
	rootFd := s.openRoot(root)
	defer unix.Close(rootFd)

	fd, err := s.openat2Beneath(rootFd, "ok.txt", unix.O_RDONLY)
	s.Require().NoError(err)
	unix.Close(fd)
}

// TestAbsoluteSymlinkEscapeBlocked: a symlink that points to an absolute path
// outside the root must fail (EXDEV or ELOOP). This is the core escape attack.
func (s *ConfinedProofSuite) TestAbsoluteSymlinkEscapeBlocked() {
	root := s.T().TempDir()
	s.Require().NoError(os.Symlink("/etc/passwd", filepath.Join(root, "evil")))
	rootFd := s.openRoot(root)
	defer unix.Close(rootFd)

	_, err := s.openat2Beneath(rootFd, "evil", unix.O_RDONLY)
	s.Require().Error(err, "absolute symlink escape must fail")
	s.True(errors.Is(err, unix.EXDEV) || errors.Is(err, unix.ELOOP),
		"expected EXDEV or ELOOP, got %v", err)
}

// TestDotDotTraversalBlocked: ../ that exits the root must fail.
func (s *ConfinedProofSuite) TestDotDotTraversalBlocked() {
	root := s.T().TempDir()
	rootFd := s.openRoot(root)
	defer unix.Close(rootFd)

	_, err := s.openat2Beneath(rootFd, "../../etc/passwd", unix.O_RDONLY)
	s.Require().Error(err)
	s.True(errors.Is(err, unix.EXDEV), "expected EXDEV, got %v", err)
}

// TestRelativeSymlinkWithinTreeWorks: a relative symlink that stays in-tree
// must still resolve (we are not banning all symlinks).
func (s *ConfinedProofSuite) TestRelativeSymlinkWithinTreeWorks() {
	root := s.T().TempDir()
	s.Require().NoError(os.Mkdir(filepath.Join(root, "sub"), 0o755))
	s.Require().NoError(os.WriteFile(filepath.Join(root, "sub", "ok.txt"), []byte("hi"), 0o644))
	s.Require().NoError(os.Symlink("sub/ok.txt", filepath.Join(root, "link")))
	rootFd := s.openRoot(root)
	defer unix.Close(rootFd)

	fd, err := s.openat2Beneath(rootFd, "link", unix.O_RDONLY)
	s.Require().NoError(err)
	unix.Close(fd)
}
```

- [ ] **Step 2: Run** — `go test ./pkg/server/io/ -run ConfinedProofSuite -v`. All four cases must PASS (no root needed; this is unprivileged). If any case fails, **stop and report** — the design assumption doesn't hold and the rest of the plan needs reconsidering.

- [ ] **Step 3: Commit**

```bash
git add pkg/server/io/confined_proof_test.go
git commit -m "test(server/io): prove openat2 RESOLVE_BENEATH blocks symlink/.. escape"
```

---

## Task 2: `resolveBeneath` — the single resolver helper

**Files:** Create `pkg/server/io/confined.go` (will grow over the next tasks; this task adds the file + the resolver only). Test: `pkg/server/io/confined_test.go`.

This helper is the heart of the wrapper: every op calls it to translate a wire `name` into a `(parentFd, leaf)` pair that the matching `*at` syscall consumes.

- [ ] **Step 1: Write the failing test**

```go
package io

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/suite"
	"golang.org/x/sys/unix"
)

type ResolveBeneathSuite struct {
	suite.Suite
	rootDir string
	rootFd  int
}

func TestResolveBeneathSuite(t *testing.T) { suite.Run(t, new(ResolveBeneathSuite)) }

func (s *ResolveBeneathSuite) SetupTest() {
	s.rootDir = s.T().TempDir()
	s.Require().NoError(os.Mkdir(filepath.Join(s.rootDir, "sub"), 0o755))
	s.Require().NoError(os.WriteFile(filepath.Join(s.rootDir, "sub", "f.txt"), []byte("hi"), 0o644))
	fd, err := unix.Open(s.rootDir, unix.O_PATH|unix.O_DIRECTORY|unix.O_CLOEXEC, 0)
	s.Require().NoError(err)
	s.rootFd = fd
}

func (s *ResolveBeneathSuite) TearDownTest() { unix.Close(s.rootFd) }

func (s *ResolveBeneathSuite) TestResolvesTopLevel() {
	parentFd, leaf, err := resolveBeneath(s.rootFd, "sub")
	s.Require().NoError(err)
	defer unix.Close(parentFd)
	s.Equal("sub", leaf)
}

func (s *ResolveBeneathSuite) TestResolvesNestedReturnsParent() {
	parentFd, leaf, err := resolveBeneath(s.rootFd, "sub/f.txt")
	s.Require().NoError(err)
	defer unix.Close(parentFd)
	s.Equal("f.txt", leaf)
	// parentFd points at sub — verify with fstatat against the rootFd later.
}

func (s *ResolveBeneathSuite) TestRejectsDotDotEscape() {
	_, _, err := resolveBeneath(s.rootFd, "../../etc/passwd")
	s.Require().ErrorIs(err, unix.EXDEV)
}

func (s *ResolveBeneathSuite) TestEmptyNameMeansRoot() {
	parentFd, leaf, err := resolveBeneath(s.rootFd, "")
	s.Require().NoError(err)
	defer unix.Close(parentFd)
	s.Equal(".", leaf, "empty name addresses the root itself")
}
```

- [ ] **Step 2: Run, expect FAIL** (undefined `resolveBeneath`).

- [ ] **Step 3: Implement** in `pkg/server/io/confined.go`:

```go
// Package io contains the server-side filesystem layer. The confined loopback
// FS lives here and replaces the unconfined pathfs.NewLoopbackFileSystem so
// every wire path is resolved beneath the volume root via openat2.
package io

import (
	"path"
	"strings"

	"golang.org/x/sys/unix"
)

// resolveHow is the openat2 resolve mask we apply to every wire path. We
// forbid every escape mechanism (mount-namespace, symlink, magic links,
// crossing devices) so a malicious or buggy path can never reach a file
// outside the volume root.
const resolveHow = unix.RESOLVE_BENEATH |
	unix.RESOLVE_NO_MAGICLINKS |
	unix.RESOLVE_NO_XDEV

// resolveBeneath translates a wire path `name` (relative to the volume root
// fd `rootFd`) into a (parentFd, leaf) pair suitable for any `*at` syscall.
// `parentFd` is an O_PATH dirfd the caller must close. `leaf` is the final
// path component, or "." when `name` addresses the root itself.
//
// All escape attempts (".." past root, absolute symlinks, magic links,
// crossing mount points) return unix.EXDEV or unix.ELOOP — never a real fd.
func resolveBeneath(rootFd int, name string) (parentFd int, leaf string, err error) {
	clean := path.Clean("/" + name) // anchor at "/" so Clean can't yield ".."
	clean = strings.TrimPrefix(clean, "/")
	if clean == "" || clean == "." {
		return dup(rootFd)
	}
	dir, leaf := path.Split(clean)
	dir = strings.TrimSuffix(dir, "/")
	if dir == "" {
		fd, err := dup1(rootFd)
		return fd, leaf, err
	}
	how := &unix.OpenHow{
		Flags:   unix.O_PATH | unix.O_DIRECTORY | unix.O_CLOEXEC,
		Resolve: resolveHow,
	}
	parentFd, err = unix.Openat2(rootFd, dir, how)
	return parentFd, leaf, err
}

// dup returns (rootFd-duped, ".") for the "name addresses root" case.
func dup(rootFd int) (int, string, error) {
	fd, err := dup1(rootFd)
	return fd, ".", err
}

// dup1 duplicates rootFd with CLOEXEC so the caller closing it doesn't affect
// the FS's long-lived root handle.
func dup1(rootFd int) (int, error) {
	return unix.FcntlInt(uintptr(rootFd), unix.F_DUPFD_CLOEXEC, 0)
}
```

- [ ] **Step 4: Run, expect PASS.** `go test ./pkg/server/io/ -run ResolveBeneathSuite -v`.

- [ ] **Step 5: Commit**

```bash
git add pkg/server/io/confined.go pkg/server/io/confined_test.go
git commit -m "feat(server/io): resolveBeneath — openat2-anchored path resolver"
```

---

## Task 3: `ConfinedLoopbackFileSystem` skeleton + stat ops (`GetAttr`, `StatFs`, `Readlink`, `Access`)

**Files:** Modify `pkg/server/io/confined.go` (add the struct + 4 ops). Test: `pkg/server/io/confined_stat_test.go`.

`ConfinedLoopbackFileSystem` holds a single `rootFd` (the volume root, opened `O_PATH | O_DIRECTORY` at construction). It embeds `pathfs.NewDefaultFileSystem()` so unimplemented methods (`OnMount`, `OnUnmount`, `String`, `SetDebug`) come from the no-op default. Each op below uses `resolveBeneath` for `(parentFd, leaf)` then issues the matching `*at` syscall.

- [ ] **Step 1: Write failing tests** (testify suite covering all four ops). For each: build a `rootDir` with a known file, instantiate `NewConfinedLoopbackFileSystem(rootDir)`, exercise the op, assert against the real on-disk state. Also one **escape** test per op family asserting EXDEV (`fs.GetAttr("../../etc/passwd", ctx)` returns a non-OK fuse.Status).

(Pattern — for the full code see the existing `pkg/server/io/access_test.go` for the testify-suite style this package uses; one assertion per case.)

- [ ] **Step 2: Run, expect FAIL.**

- [ ] **Step 3: Implement** in `confined.go`:

```go
// ConfinedLoopbackFileSystem is a pathfs.FileSystem that translates every op
// to fd-relative *at syscalls anchored at a single openat2-resolved volume
// root dirfd. It composes safely under identityBoundFS.
type ConfinedLoopbackFileSystem struct {
	pathfs.FileSystem // for the no-op String/SetDebug/OnMount/OnUnmount
	rootFd            int
	rootPath          string // kept for log + StatFs
}

// NewConfinedLoopbackFileSystem opens path as the volume root. It must be a
// directory; ENOTDIR/ENOENT bubble up to the caller.
func NewConfinedLoopbackFileSystem(rootPath string) (*ConfinedLoopbackFileSystem, error) {
	fd, err := unix.Open(rootPath, unix.O_PATH|unix.O_DIRECTORY|unix.O_CLOEXEC, 0)
	if err != nil {
		return nil, fmt.Errorf("open volume root %q: %w", rootPath, err)
	}
	return &ConfinedLoopbackFileSystem{
		FileSystem: pathfs.NewDefaultFileSystem(),
		rootFd:     fd,
		rootPath:   rootPath,
	}, nil
}

func (c *ConfinedLoopbackFileSystem) GetAttr(name string, _ *fuse.Context) (*fuse.Attr, fuse.Status) {
	parentFd, leaf, err := resolveBeneath(c.rootFd, name)
	if err != nil {
		return nil, errnoToStatus(err)
	}
	defer unix.Close(parentFd)
	var st unix.Stat_t
	if err := unix.Fstatat(parentFd, leaf, &st, unix.AT_SYMLINK_NOFOLLOW); err != nil {
		return nil, errnoToStatus(err)
	}
	a := &fuse.Attr{}
	a.FromStat(&st)
	return a, fuse.OK
}

func (c *ConfinedLoopbackFileSystem) StatFs(name string) *fuse.StatfsOut {
	parentFd, leaf, err := resolveBeneath(c.rootFd, name)
	if err != nil {
		return nil
	}
	defer unix.Close(parentFd)
	// fstatfs needs a regular fd; open the leaf via the parent.
	fd, err := unix.Openat2(parentFd, leaf, &unix.OpenHow{
		Flags: unix.O_PATH | unix.O_CLOEXEC, Resolve: resolveHow,
	})
	if err != nil {
		return nil
	}
	defer unix.Close(fd)
	var sf unix.Statfs_t
	if err := unix.Fstatfs(fd, &sf); err != nil {
		return nil
	}
	out := &fuse.StatfsOut{}
	out.FromStatfsT(&sf)
	return out
}

func (c *ConfinedLoopbackFileSystem) Readlink(name string, _ *fuse.Context) (string, fuse.Status) {
	parentFd, leaf, err := resolveBeneath(c.rootFd, name)
	if err != nil {
		return "", errnoToStatus(err)
	}
	defer unix.Close(parentFd)
	buf := make([]byte, 4096)
	n, err := unix.Readlinkat(parentFd, leaf, buf)
	if err != nil {
		return "", errnoToStatus(err)
	}
	return string(buf[:n]), fuse.OK
}

func (c *ConfinedLoopbackFileSystem) Access(name string, mode uint32, _ *fuse.Context) fuse.Status {
	parentFd, leaf, err := resolveBeneath(c.rootFd, name)
	if err != nil {
		return errnoToStatus(err)
	}
	defer unix.Close(parentFd)
	if err := unix.Faccessat(parentFd, leaf, mode, 0); err != nil {
		return errnoToStatus(err)
	}
	return fuse.OK
}

// errnoToStatus maps a unix errno to a fuse.Status. EXDEV/ELOOP from
// resolveBeneath map to fuse.EACCES per spec §3.10 (the client sees a
// permission denial, not a system-internal cross-device hint).
func errnoToStatus(err error) fuse.Status {
	if err == nil {
		return fuse.OK
	}
	var errno syscall.Errno
	if errors.As(err, &errno) {
		switch errno {
		case unix.EXDEV, unix.ELOOP:
			return fuse.EACCES
		}
		return fuse.Status(errno)
	}
	return fuse.EIO
}
```

(Imports: `errors`, `fmt`, `syscall`, `github.com/hanwen/go-fuse/v2/fuse`, `github.com/hanwen/go-fuse/v2/fuse/pathfs`, `golang.org/x/sys/unix`.)

- [ ] **Step 4: Run, PASS.** Tests in the new suite + the earlier `ConfinedProofSuite` + `ResolveBeneathSuite` all green.

- [ ] **Step 5: Commit**

```bash
git add pkg/server/io/confined.go pkg/server/io/confined_stat_test.go
git commit -m "feat(server/io): ConfinedLoopbackFileSystem skeleton + GetAttr/StatFs/Readlink/Access"
```

---

## Task 4: Mode/owner/time/size ops (`Chmod`, `Chown`, `Utimens`, `Truncate`)

**Files:** Modify `pkg/server/io/confined.go`. Test: `pkg/server/io/confined_mode_test.go`.

Pattern: `resolveBeneath` → call the matching `*at` syscall on `(parentFd, leaf)`. Truncate is the odd one — `unix` has no `truncate at` for a relative path, so open with `openat2(O_PATH | O_NOFOLLOW)` then `ftruncate`.

- [ ] **Step 1: Failing tests** mirroring the Task 3 pattern: in-tree change + escape returns EACCES.

- [ ] **Step 2-4: Implement + test.** Each op is the same shape:

```go
func (c *ConfinedLoopbackFileSystem) Chmod(name string, mode uint32, _ *fuse.Context) fuse.Status {
	parentFd, leaf, err := resolveBeneath(c.rootFd, name)
	if err != nil {
		return errnoToStatus(err)
	}
	defer unix.Close(parentFd)
	if err := unix.Fchmodat(parentFd, leaf, mode, 0); err != nil {
		return errnoToStatus(err)
	}
	return fuse.OK
}

func (c *ConfinedLoopbackFileSystem) Chown(name string, uid, gid uint32, _ *fuse.Context) fuse.Status {
	parentFd, leaf, err := resolveBeneath(c.rootFd, name)
	if err != nil {
		return errnoToStatus(err)
	}
	defer unix.Close(parentFd)
	if err := unix.Fchownat(parentFd, leaf, int(uid), int(gid), unix.AT_SYMLINK_NOFOLLOW); err != nil {
		return errnoToStatus(err)
	}
	return fuse.OK
}

func (c *ConfinedLoopbackFileSystem) Utimens(name string, atime, mtime *time.Time, _ *fuse.Context) fuse.Status {
	parentFd, leaf, err := resolveBeneath(c.rootFd, name)
	if err != nil {
		return errnoToStatus(err)
	}
	defer unix.Close(parentFd)
	var ts [2]unix.Timespec
	ts[0] = toTimespec(atime)
	ts[1] = toTimespec(mtime)
	if err := unix.UtimesNanoAt(parentFd, leaf, ts[:], unix.AT_SYMLINK_NOFOLLOW); err != nil {
		return errnoToStatus(err)
	}
	return fuse.OK
}

func (c *ConfinedLoopbackFileSystem) Truncate(name string, size uint64, _ *fuse.Context) fuse.Status {
	parentFd, leaf, err := resolveBeneath(c.rootFd, name)
	if err != nil {
		return errnoToStatus(err)
	}
	defer unix.Close(parentFd)
	fd, err := unix.Openat2(parentFd, leaf, &unix.OpenHow{
		Flags: unix.O_WRONLY | unix.O_NOFOLLOW | unix.O_CLOEXEC, Resolve: resolveHow,
	})
	if err != nil {
		return errnoToStatus(err)
	}
	defer unix.Close(fd)
	if err := unix.Ftruncate(fd, int64(size)); err != nil {
		return errnoToStatus(err)
	}
	return fuse.OK
}

// toTimespec maps *time.Time (nil = UTIME_OMIT) to unix.Timespec.
func toTimespec(t *time.Time) unix.Timespec {
	if t == nil {
		return unix.Timespec{Nsec: unix.UTIME_OMIT}
	}
	return unix.NsecToTimespec(t.UnixNano())
}
```

- [ ] **Step 5: Commit**

```bash
git add pkg/server/io/confined.go pkg/server/io/confined_mode_test.go
git commit -m "feat(server/io): confined Chmod/Chown/Utimens/Truncate"
```

---

## Task 5: Directory + name ops (`Mkdir`, `Mknod`, `Rmdir`, `Unlink`, `Rename`, `Symlink`, `Link`)

**Files:** Modify `pkg/server/io/confined.go`. Test: `pkg/server/io/confined_dir_test.go`.

Rename and Link are two-name ops — resolve both sides.

- [ ] **Step 1: Failing tests** (one in-tree + one escape per op).

- [ ] **Step 2-4: Implement + test.** Patterns:

```go
func (c *ConfinedLoopbackFileSystem) Mkdir(name string, mode uint32, _ *fuse.Context) fuse.Status {
	pFd, leaf, err := resolveBeneath(c.rootFd, name)
	if err != nil { return errnoToStatus(err) }
	defer unix.Close(pFd)
	return errnoToStatus(unix.Mkdirat(pFd, leaf, mode))
}

func (c *ConfinedLoopbackFileSystem) Rmdir(name string, _ *fuse.Context) fuse.Status {
	pFd, leaf, err := resolveBeneath(c.rootFd, name)
	if err != nil { return errnoToStatus(err) }
	defer unix.Close(pFd)
	return errnoToStatus(unix.Unlinkat(pFd, leaf, unix.AT_REMOVEDIR))
}

func (c *ConfinedLoopbackFileSystem) Unlink(name string, _ *fuse.Context) fuse.Status {
	pFd, leaf, err := resolveBeneath(c.rootFd, name)
	if err != nil { return errnoToStatus(err) }
	defer unix.Close(pFd)
	return errnoToStatus(unix.Unlinkat(pFd, leaf, 0))
}

func (c *ConfinedLoopbackFileSystem) Mknod(name string, mode, dev uint32, _ *fuse.Context) fuse.Status {
	pFd, leaf, err := resolveBeneath(c.rootFd, name)
	if err != nil { return errnoToStatus(err) }
	defer unix.Close(pFd)
	return errnoToStatus(unix.Mknodat(pFd, leaf, mode, int(dev)))
}

func (c *ConfinedLoopbackFileSystem) Rename(oldName, newName string, _ *fuse.Context) fuse.Status {
	oldFd, oldLeaf, err := resolveBeneath(c.rootFd, oldName)
	if err != nil { return errnoToStatus(err) }
	defer unix.Close(oldFd)
	newFd, newLeaf, err := resolveBeneath(c.rootFd, newName)
	if err != nil { return errnoToStatus(err) }
	defer unix.Close(newFd)
	return errnoToStatus(unix.Renameat2(oldFd, oldLeaf, newFd, newLeaf, 0))
}

func (c *ConfinedLoopbackFileSystem) Symlink(target, linkName string, _ *fuse.Context) fuse.Status {
	pFd, leaf, err := resolveBeneath(c.rootFd, linkName)
	if err != nil { return errnoToStatus(err) }
	defer unix.Close(pFd)
	// We DO NOT validate `target` — clients can store any string in a symlink;
	// resolveBeneath will block escape attempts when it's later READ via openat2.
	return errnoToStatus(unix.Symlinkat(target, pFd, leaf))
}

func (c *ConfinedLoopbackFileSystem) Link(oldName, newName string, _ *fuse.Context) fuse.Status {
	oldFd, oldLeaf, err := resolveBeneath(c.rootFd, oldName)
	if err != nil { return errnoToStatus(err) }
	defer unix.Close(oldFd)
	newFd, newLeaf, err := resolveBeneath(c.rootFd, newName)
	if err != nil { return errnoToStatus(err) }
	defer unix.Close(newFd)
	return errnoToStatus(unix.Linkat(oldFd, oldLeaf, newFd, newLeaf, 0))
}
```

- [ ] **Step 5: Commit**

```bash
git add pkg/server/io/confined.go pkg/server/io/confined_dir_test.go
git commit -m "feat(server/io): confined Mkdir/Mknod/Rmdir/Unlink/Rename/Symlink/Link"
```

---

## Task 6: File ops (`Open`, `Create`, `OpenDir`)

**Files:** Modify `pkg/server/io/confined.go`. Test: `pkg/server/io/confined_open_test.go`.

These return a `nodefs.File` (or DirEntries). Use go-fuse's existing `nodefs.NewLoopbackFile(fd)` wrapper for read/write — the fd is confined, so the data path inherits confinement automatically (the kernel never sees a server-side path again).

- [ ] **Step 1-4: Implement + test.**

```go
func (c *ConfinedLoopbackFileSystem) Open(name string, flags uint32, _ *fuse.Context) (nodefs.File, fuse.Status) {
	pFd, leaf, err := resolveBeneath(c.rootFd, name)
	if err != nil { return nil, errnoToStatus(err) }
	defer unix.Close(pFd)
	fd, err := unix.Openat2(pFd, leaf, &unix.OpenHow{
		Flags: uint64(flags) | unix.O_CLOEXEC, Resolve: resolveHow,
	})
	if err != nil { return nil, errnoToStatus(err) }
	return nodefs.NewLoopbackFile(fd), fuse.OK
}

func (c *ConfinedLoopbackFileSystem) Create(name string, flags, mode uint32, _ *fuse.Context) (nodefs.File, fuse.Status) {
	pFd, leaf, err := resolveBeneath(c.rootFd, name)
	if err != nil { return nil, errnoToStatus(err) }
	defer unix.Close(pFd)
	fd, err := unix.Openat2(pFd, leaf, &unix.OpenHow{
		Flags:   uint64(flags) | unix.O_CREAT | unix.O_CLOEXEC,
		Mode:    uint64(mode),
		Resolve: resolveHow,
	})
	if err != nil { return nil, errnoToStatus(err) }
	return nodefs.NewLoopbackFile(fd), fuse.OK
}

func (c *ConfinedLoopbackFileSystem) OpenDir(name string, _ *fuse.Context) ([]fuse.DirEntry, fuse.Status) {
	pFd, leaf, err := resolveBeneath(c.rootFd, name)
	if err != nil { return nil, errnoToStatus(err) }
	defer unix.Close(pFd)
	dirFd, err := unix.Openat2(pFd, leaf, &unix.OpenHow{
		Flags: unix.O_DIRECTORY | unix.O_RDONLY | unix.O_CLOEXEC, Resolve: resolveHow,
	})
	if err != nil { return nil, errnoToStatus(err) }
	defer unix.Close(dirFd)
	// /proc/self/fd/<n> is the canonical way to readdir a fd; use the standard
	// library os.File wrapping.
	f := os.NewFile(uintptr(dirFd), "")
	names, err := f.Readdirnames(-1)
	if err != nil { return nil, errnoToStatus(err) }
	out := make([]fuse.DirEntry, 0, len(names))
	for _, n := range names {
		var st unix.Stat_t
		if unix.Fstatat(dirFd, n, &st, unix.AT_SYMLINK_NOFOLLOW) != nil {
			continue // skip races where the entry vanished
		}
		out = append(out, fuse.DirEntry{Name: n, Mode: st.Mode})
	}
	return out, fuse.OK
}
```

(`os.NewFile(uintptr(dirFd), "")` adopts the fd; we don't `defer unix.Close(dirFd)` after that — `f.Readdirnames` doesn't close, so we close `dirFd` once at the end of the function. Verify with `-race`.)

- [ ] **Step 5: Commit**

```bash
git add pkg/server/io/confined.go pkg/server/io/confined_open_test.go
git commit -m "feat(server/io): confined Open/Create/OpenDir"
```

---

## Task 7: Xattr ops (`GetXAttr`, `SetXAttr`, `ListXAttr`, `RemoveXAttr`)

**Files:** Modify `pkg/server/io/confined.go`. Test: `pkg/server/io/confined_xattr_test.go`.

There are no `*at` variants of `getxattr`/`setxattr` in Linux that take a dirfd directly. The standard idiom: open the target with `O_PATH | O_NOFOLLOW` via openat2, then `fgetxattr`/`fsetxattr` via the magic `/proc/self/fd/<n>` path with `lgetxattr`/`lsetxattr`. `x/sys/unix` exposes `Fgetxattr`/`Fsetxattr` operating on an fd directly.

- [ ] **Step 1-4: Implement + test.** Pattern:

```go
func (c *ConfinedLoopbackFileSystem) GetXAttr(name, attr string, _ *fuse.Context) ([]byte, fuse.Status) {
	pFd, leaf, err := resolveBeneath(c.rootFd, name)
	if err != nil { return nil, errnoToStatus(err) }
	defer unix.Close(pFd)
	fd, err := unix.Openat2(pFd, leaf, &unix.OpenHow{
		Flags: unix.O_PATH | unix.O_NOFOLLOW | unix.O_CLOEXEC, Resolve: resolveHow,
	})
	if err != nil { return nil, errnoToStatus(err) }
	defer unix.Close(fd)
	// Use /proc/self/fd/<n> to issue the xattr syscall — fgetxattr operates
	// on the target the O_PATH fd points at.
	procPath := fmt.Sprintf("/proc/self/fd/%d", fd)
	sz, err := unix.Lgetxattr(procPath, attr, nil)
	if err != nil { return nil, errnoToStatus(err) }
	buf := make([]byte, sz)
	if _, err := unix.Lgetxattr(procPath, attr, buf); err != nil {
		return nil, errnoToStatus(err)
	}
	return buf, fuse.OK
}

// SetXAttr, ListXAttr, RemoveXAttr follow the same pattern using
// unix.Lsetxattr / unix.Llistxattr / unix.Lremovexattr via /proc/self/fd/<n>.
```

- [ ] **Step 5: Commit**

```bash
git add pkg/server/io/confined.go pkg/server/io/confined_xattr_test.go
git commit -m "feat(server/io): confined GetXAttr/SetXAttr/ListXAttr/RemoveXAttr"
```

---

## Task 8: Wire `NewLocalFilesystem` to use the confined FS; delete the unconfined loopback

**Files:** Modify `pkg/server/io/filesystem.go`. Confirm `pkg/server/service/volume.go` still compiles (its `NewLocalFilesystem(path)` call site is unchanged signature-wise — but now we return an error too).

`NewLocalFilesystem` currently does `pathfs.NewLoopbackFileSystem(path)` (no error). The confined version returns `(*ConfinedLoopbackFileSystem, error)` because opening the root can fail. Either:
- **A.** Keep the no-error signature by panicking on root-open failure (matches the current behavior, since an unopenable volume is fatal-at-startup).
- **B.** Change the signature to return an error; update the one caller in `pkg/server/service/volume.go`'s `NewVolumeService` to propagate it.

**Choose B** — startup should refuse to serve a volume whose root can't be opened, with a clear error.

- [ ] **Step 1:** Modify `pkg/server/io/filesystem.go`:

```go
package io

import "github.com/hanwen/go-fuse/v2/fuse/pathfs"

// LocalFilesystem wraps the confined loopback so the same type name and
// interface remain stable for callers (it embeds pathfs.FileSystem).
type LocalFilesystem struct {
	pathfs.FileSystem
}

// NewLocalFilesystem opens path as a confined volume root.
func NewLocalFilesystem(path string) (*LocalFilesystem, error) {
	fs, err := NewConfinedLoopbackFileSystem(path)
	if err != nil {
		return nil, err
	}
	return &LocalFilesystem{FileSystem: fs}, nil
}
```

- [ ] **Step 2:** Update the call site in `pkg/server/service/volume.go` `NewVolumeService` to handle the new error. Currently it does (paraphrased):
```go
svc.addFileSystem(v.Name, io.NewLocalFilesystem(v.Path))
```
Change to:
```go
fs, err := io.NewLocalFilesystem(v.Path)
if err != nil {
	return nil, errors.Wrapf(err, "volume %q", v.Name)
}
svc.addFileSystem(v.Name, fs)
```
This requires `NewVolumeService` to return `(VolumeService, error)`. Update its signature, its callers (`pkg/server/app.go`), and any tests. Use a brief grep — likely 1-2 sites.

- [ ] **Step 3:** Build + full server suite green: `go build ./pkg/server/... ./cmd/...` and `go test ./pkg/server/... -count=1`. The existing `BoundFSCredsSuite` (creds path through the bound wrapper around the confined FS) must still PASS — confinement composes under `identityBoundFS`.

- [ ] **Step 4: Commit**

```bash
git add pkg/server/io/filesystem.go pkg/server/service/volume.go pkg/server/app.go
git commit -m "refactor(server): replace pathfs loopback with the confined FS"
```

---

## Task 9: VM e2e — symlink escape regression test

**Files:** Create `test/e2e/fs/confinement_test.go` (testify suite; runs in the sandbox AND the VM — confinement is unprivileged, but the existing FUSE e2e tests already use root for the suite, so this can join them).

A real end-to-end test: mount a volume, create `evil -> /etc/passwd` inside the mount, then `os.Open(mountPath + "/evil")` must fail with EACCES. Also `../../etc/passwd` via path must fail with EACCES.

- [ ] **Step 1: Write the suite.** Use the existing e2e harness (`WithRandomTestVolume` + `WithBasicAuth`, no special mapping). Mount via `c.clientCtx.SingleVolumeMounter.Mount`. Mirror the structure of `identity_rewrite_test.go`. Two test methods: `TestSymlinkEscapeReturnsEACCES`, `TestDotDotTraversalReturnsEACCES`.

- [ ] **Step 2: Build + run on the VM as root** (FUSE mount needs root):
```
go test -c -o /tmp/fs.test ./test/e2e/fs/
cd ~/.../test/e2e/fs && sudo /tmp/fs.test -test.run TestConfinementSuite -test.v
```
Both cases PASS. The existing `TestIdentityRewriteSuite` + `TestWritebackSuite` etc. **still PASS** (confinement composes silently for in-tree access).

- [ ] **Step 3: Commit**

```bash
git add test/e2e/fs/confinement_test.go
git commit -m "test(e2e): symlink/.. escape blocked by the confined loopback"
```

---

## Self-Review

**Spec coverage (§3.10 → task):** open the volume root once + every wire path resolves via `openat2(RESOLVE_BENEATH | RESOLVE_NO_MAGICLINKS)` → T2 + every op in T3–T7. Reject absolute symlinks + `..` with EXDEV/EACCES → `errnoToStatus` maps EXDEV/ELOOP → `fuse.EACCES` (T3) + tests in every task. Composes under `identityBoundFS` → T8 build+test gate.

**Type consistency:** `resolveBeneath(rootFd int, name string) (parentFd int, leaf string, err error)` consumed by every op (T3–T7). `ConfinedLoopbackFileSystem` embeds `pathfs.NewDefaultFileSystem()` so the unimplemented String/SetDebug/OnMount/OnUnmount come from there. `NewLocalFilesystem` returns an error (T8) — sole caller is `NewVolumeService`, updated in T8.

**Open notes for the implementer:**
1. `RESOLVE_NO_XDEV` (in `resolveHow`) — bind-mounted subdirs INSIDE the volume root would be rejected. If a volume legitimately bind-mounts subdirs, drop `NO_XDEV` and rely on RESOLVE_BENEATH alone. **Don't drop it pre-emptively** — the existing e2e harness doesn't bind-mount; only revisit if a regression surfaces.
2. xattr via `/proc/self/fd/<n>` (T7) requires `/proc` mounted — true on Linux always, but document so a future minimal container doesn't surprise us.
3. The data path (Read/Write) doesn't change — the fd returned by `Open`/`Create` is already confined. No file.go controller change is needed.
