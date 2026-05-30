package service

import (
	"context"
	"strconv"
	"strings"
	"time"

	"go.gmountie.dev/gmountie/pkg/utils/log"

	"go.uber.org/zap"
)

const defaultSquashLookupTimeout = 5 * time.Second

type squashResolver struct {
	uid, gid    uint32
	precomputed Identity // resolved once at construction; callers MUST NOT mutate
}

// NewSquashResolver maps every principal to one fixed identity (NFS all_squash).
// Names are looked up once via getent at construction; failures are logged and
// leave the names empty (the numeric ids still serve enforcement correctly).
// The resolved Identity is precomputed so Resolve is allocation-free.
func NewSquashResolver(uid, gid uint32) IdentityResolver {
	return newSquashResolverWithRunner(uid, gid, execRunner, defaultSquashLookupTimeout)
}

func newSquashResolverWithRunner(uid, gid uint32, run commandRunner, timeout time.Duration) *squashResolver {
	r := &squashResolver{uid: uid, gid: gid}
	groupNames := map[uint32]string{}
	var userName string

	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	if name, ok := lookupGetentField(ctx, run, "passwd", uid); ok {
		userName = name
	} else {
		log.Log.Warn("squash resolver: getent passwd lookup failed; user_name will be empty",
			zap.Uint32("uid", uid))
	}
	if name, ok := lookupGetentField(ctx, run, "group", gid); ok {
		groupNames[gid] = name
	} else {
		log.Log.Warn("squash resolver: getent group lookup failed; group_name will be empty",
			zap.Uint32("gid", gid))
	}

	// Precompute once. Gids and GroupNames are immutable after this point;
	// all callers (BindIdentity, toProtoAttr, changeIdentity) are read-only.
	r.precomputed = Identity{
		Uid:        uid,
		Gid:        gid,
		Gids:       []uint32{gid},
		UserName:   userName,
		GroupNames: groupNames,
	}
	return r
}

// lookupGetentField runs `getent <db> <id>` and returns the first colon-delimited
// field (the canonical name) or false on any error / unexpected output.
func lookupGetentField(ctx context.Context, run commandRunner, db string, id uint32) (string, bool) {
	out, err := run(ctx, "getent", db, strconv.FormatUint(uint64(id), 10))
	if err != nil || len(out) == 0 {
		return "", false
	}
	line := strings.TrimRight(string(out), "\n")
	if i := strings.IndexByte(line, ':'); i > 0 {
		return line[:i], true
	}
	return "", false
}

// Resolve returns the precomputed fixed identity, stamping the calling
// principal into the Principal field. No allocations for Gids or GroupNames.
func (r *squashResolver) Resolve(principal string) (Identity, error) {
	id := r.precomputed
	id.Principal = principal
	return id, nil
}
