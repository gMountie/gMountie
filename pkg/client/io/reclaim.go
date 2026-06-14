package io

import (
	"context"
	"syscall"

	"go.gmountie.dev/gmountie/pkg/proto"
	"go.gmountie.dev/gmountie/pkg/utils/log"

	"github.com/google/uuid"
	"github.com/hanwen/go-fuse/v2/fuse"
	"go.uber.org/zap"
	"google.golang.org/grpc"
)

// reclaimIfStale reopens this handle's server-side fd against the current
// session when the server has restarted. The predicate is the handle's session
// snapshot (taken at open) vs the client's live session id: they diverge
// exactly when SessionHandshake fell back from Resume to Create — i.e. the
// server lost the original session and every fd under it.
//
// Safe to call on every fd-op attempt: a fresh handle is a cheap compare-and-
// return. reopenMu serializes concurrent callers so the fd is reopened once;
// each re-checks the predicate under the lock. On success h.fd and h.sessionID
// are swapped to the new values. On failure the fuse.Status surfaces (notably
// the unlinked-but-open case: the path no longer resolves and reopen fails).
func (h *grpcFileHandle) reclaimIfStale(ctx context.Context) fuse.Status {
	cur := h.state.Load()
	live := h.client.SessionID()
	if cur.sessionID == live { // lock-free fast path
		return fuse.OK
	}
	h.reopenMu.Lock()
	defer h.reopenMu.Unlock()
	cur = h.state.Load()
	live = h.client.SessionID()
	if cur.sessionID == live { // a racing caller already reclaimed
		return fuse.OK
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
	if fuse.Status(reply.Status) != fuse.OK {
		return fuse.Status(reply.Status)
	}

	log.Log.Info("reclaimed file handle after server restart",
		zap.String("path", h.path),
		zap.Uint64("old_fd", cur.fd), zap.Uint64("new_fd", reply.Fd))
	h.state.Store(&fdState{fd: reply.Fd, sessionID: live})
	return fuse.OK
}

// reclaimError wraps a fuse.Status from a failed reclaim as a non-retryable
// error so retryOp short-circuits and the status reaches userspace unchanged.
type reclaimError struct{ st fuse.Status }

func (e reclaimError) Error() string { return "reclaim failed: " + e.st.String() }

func errFromStatus(st fuse.Status) error { return reclaimError{st} }

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
