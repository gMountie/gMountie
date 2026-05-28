package service

import (
	"context"
	"strconv"
	"strings"
	"time"

	"gmountie/pkg/utils/log"

	"go.uber.org/zap"
)

const defaultSquashLookupTimeout = 5 * time.Second

type squashResolver struct {
	uid, gid   uint32
	userName   string
	groupNames map[uint32]string
}

// NewSquashResolver maps every principal to one fixed identity (NFS all_squash).
// Names are looked up once via getent at construction; failures are logged and
// leave the names empty (the numeric ids still serve enforcement correctly).
func NewSquashResolver(uid, gid uint32) IdentityResolver {
	return newSquashResolverWithRunner(uid, gid, execRunner, defaultSquashLookupTimeout)
}

func newSquashResolverWithRunner(uid, gid uint32, run commandRunner, timeout time.Duration) *squashResolver {
	r := &squashResolver{uid: uid, gid: gid, groupNames: map[uint32]string{}}
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	if name, ok := lookupGetentField(ctx, run, "passwd", uid); ok {
		r.userName = name
	} else {
		log.Log.Warn("squash resolver: getent passwd lookup failed; user_name will be empty",
			zap.Uint32("uid", uid))
	}
	if name, ok := lookupGetentField(ctx, run, "group", gid); ok {
		r.groupNames[gid] = name
	} else {
		log.Log.Warn("squash resolver: getent group lookup failed; group_name will be empty",
			zap.Uint32("gid", gid))
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

func (r *squashResolver) Resolve(principal string) (Identity, error) {
	gids := []uint32{r.gid}
	// Copy GroupNames so callers can't mutate the resolver's map.
	groupNames := make(map[uint32]string, len(r.groupNames))
	for k, v := range r.groupNames {
		groupNames[k] = v
	}
	return Identity{
		Principal:  principal,
		Uid:        r.uid,
		Gid:        r.gid,
		Gids:       gids,
		UserName:   r.userName,
		GroupNames: groupNames,
	}, nil
}
