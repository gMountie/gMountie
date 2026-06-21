package io

import (
	"context"
	"syscall"

	fserr "go.gmountie.dev/gmountie/pkg/common/fserr"
	"go.gmountie.dev/gmountie/pkg/proto"
	"go.gmountie.dev/gmountie/pkg/utils/log"

	"github.com/google/uuid"
	"go.uber.org/zap"
	"google.golang.org/grpc"
)

// reclaimIfStale reopens this handle's server-side fd when the server has
// restarted (boot epoch changed). It gates reclaim on the server's boot epoch
// to distinguish two very different scenarios that both cause a session-id
// change:
//
//   - Server RESTART: the server process died and came back. Its boot epoch is
//     NEW. While it was down no client could write, so the file is exactly as
//     last left — reopening the fd by path is safe. Reclaim.
//
//   - Same-process REAP: the same server process reaped our idle session (we
//     were disconnected past the grace period). Its boot epoch is UNCHANGED.
//     Other clients may have mutated the file while we were gone, so silently
//     reopening would substitute a possibly-changed file into a live fd. Do NOT
//     reclaim — fail the fd-op cleanly with ESTALE (a stale handle, not ENOENT).
//
// Safe to call on every fd-op attempt: a fresh handle is a cheap compare-and-
// return. reopenMu serializes concurrent callers so the fd is reopened once;
// each re-checks the predicate under the lock. On success a fresh fdState with
// the new fd, session id, and epoch is atomically swapped in via h.state.
func (h *grpcFileHandle) reclaimIfStale(ctx context.Context) proto.FsError {
	cur := h.state.Load()
	// IMPORTANT: read SessionID() BEFORE BootEpoch(). The handshake sets the
	// new epoch before the new session id under its mutex, so observing a
	// changed session id guarantees the matching (new) epoch is already
	// visible. Reading in this order makes the restart/reap classification
	// race-free.
	if cur.sessionID == h.client.SessionID() {
		return proto.FsError_FS_OK // fresh: same session, nothing to do
	}
	// Session changed (Resume failed → Create). Restart or same-process reap?
	if h.client.BootEpoch() == cur.epoch {
		// Same server process reaped our idle session. The server-side fd is
		// dead and the file may have been mutated by another client while we
		// were gone, so we must NOT silently reopen. Fail the fd-op cleanly
		// with ESTALE ("stale file handle") — the honest errno for a dead fd,
		// and never ENOENT (the file exists; it's our handle that's gone).
		// This preserves the "fail cleanly past grace" contract.
		return proto.FsError_FS_ESTALE
	}
	// Boot epoch changed → the server process restarted. While it was down no
	// client could write, so reopening by path is as safe as the original open.
	h.reopenMu.Lock()
	defer h.reopenMu.Unlock()
	cur = h.state.Load()
	live := h.client.SessionID()
	liveEpoch := h.client.BootEpoch()
	if cur.sessionID == live {
		return proto.FsError_FS_OK // a racing caller already reclaimed
	}
	if liveEpoch == cur.epoch {
		return proto.FsError_FS_ESTALE // became a reap under the lock; fail clean
	}
	reply, err := h.client.File().Open(ctx, &proto.OpenRequest{
		Volume:    h.volume,
		Caller:    h.reopenCaller,
		Path:      h.path,
		Flags:     h.reopenFlags,
		SessionId: live,
		RequestId: uuid.NewString(),
	}, grpc.WaitForReady(true))
	if err != nil {
		return statusFromRPCError(err)
	}
	if reply.Status != proto.FsError_FS_OK {
		return reply.Status
	}
	log.Log.Info("reclaimed file handle after server restart",
		zap.String("path", h.path),
		zap.Uint64("old_fd", cur.fd), zap.Uint64("new_fd", reply.Fd))
	h.state.Store(&fdState{fd: reply.Fd, sessionID: live, epoch: liveEpoch})
	return proto.FsError_FS_OK
}

// reclaimError wraps a proto.FsError from a failed reclaim as a non-retryable
// error so retryOp short-circuits and the status reaches userspace unchanged.
type reclaimError struct{ st proto.FsError }

// Error renders the host errno's human-readable string ("stale file handle")
// rather than the bare enum name ("FS_ESTALE"), so logs stay readable.
func (e reclaimError) Error() string { return "reclaim failed: " + fserr.ToErrno(e.st).Error() }

func errFromStatus(st proto.FsError) error { return reclaimError{st} }

// sanitizeReopenFlags returns the open flags to use when REOPENING an
// already-open file during reclaim. The file already exists and already holds
// the application's data, so creation/exclusivity/truncation flags must be
// stripped: O_TRUNC would discard the bytes the app has been writing, and
// O_EXCL would fail because the path now exists. The access mode and O_APPEND
// are preserved so reads/writes keep the same semantics.
func sanitizeReopenFlags(flags uint32) uint32 {
	const strip = uint32(syscall.O_CREAT | syscall.O_EXCL | syscall.O_TRUNC)
	return flags &^ strip
}
