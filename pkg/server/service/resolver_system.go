package service

import (
	"context"
	"os/exec"
	"regexp"
	"strconv"
	"strings"
	"time"

	"go.gmountie.dev/gmountie/pkg/server/config"

	"github.com/pkg/errors"
)

// commandRunner runs an external command (argv, no shell) and returns stdout.
// Injectable for tests.
type commandRunner func(ctx context.Context, name string, args ...string) ([]byte, error)

func execRunner(ctx context.Context, name string, args ...string) ([]byte, error) {
	return exec.CommandContext(ctx, name, args...).Output()
}

// validPrincipal guards the value we pass as an argv element. Even though argv
// avoids shell injection, we keep principals to a sane charset and forbid a
// leading '-' so a principal can never be mistaken for a flag (e.g. "-G").
var validPrincipal = regexp.MustCompile(`^[a-zA-Z0-9_@][a-zA-Z0-9._@-]{0,63}$`)

type systemResolver struct {
	run         commandRunner
	timeout     time.Duration
	adminGroups map[string][]string // cap name -> server-side group names
}

func NewSystemResolver(cfg config.MappingConfig) IdentityResolver {
	return newSystemResolverWithRunner(execRunner, 5*time.Second, cfg)
}

func newSystemResolverWithRunner(run commandRunner, timeout time.Duration, cfg config.MappingConfig) *systemResolver {
	return &systemResolver{run: run, timeout: timeout, adminGroups: cfg.AdminGroups}
}

// Resolve maps a principal to its system identity via a SINGLE `id <user>`
// invocation. The previous implementation issued five subprocess calls and
// zipped `id -G` (gids) with `id -nG` (names) by array index — two separate
// process snapshots, so a group-membership change between the calls (or any
// length disagreement) silently mislabelled group ownership. One invocation
// keeps every gid↔name pair from the same snapshot.
func (r *systemResolver) Resolve(principal string) (Identity, error) {
	if !validPrincipal.MatchString(principal) {
		return Identity{}, errors.Errorf("invalid principal %q", principal)
	}
	ctx, cancel := context.WithTimeout(context.Background(), r.timeout)
	defer cancel()

	out, err := r.run(ctx, "id", principal)
	if err != nil {
		return Identity{}, mapNotFound(err)
	}
	id, err := parseIDOutput(principal, string(out))
	if err != nil {
		return Identity{}, err
	}
	id.Caps = r.capsForMember(memberNamesOf(id))
	return id, nil
}

// idToken matches one `1000(name)` element of `id` output; the name part is
// optional (unresolvable ids print bare numbers).
var idToken = regexp.MustCompile(`^(\d+)(?:\(([^)]*)\))?$`)

// parseIDOutput parses the POSIX `id <user>` line, e.g.
//
//	uid=1000(alice) gid=1000(alice) groups=1000(alice),27(sudo),100(users)
//
// Unknown trailing fields (e.g. SELinux `context=`) are ignored. Group names
// containing whitespace would break the field split; such names are already
// excluded by every sane NSS source and were equally unsupported before.
func parseIDOutput(principal, out string) (Identity, error) {
	id := Identity{Principal: principal, GroupNames: map[uint32]string{}}
	uidSeen, gidSeen := false, false
	for _, field := range strings.Fields(strings.TrimSpace(out)) {
		switch {
		case strings.HasPrefix(field, "uid="):
			n, name, err := parseIDToken(strings.TrimPrefix(field, "uid="))
			if err != nil {
				return Identity{}, errors.Wrapf(err, "parse id output for %q", principal)
			}
			id.Uid, id.UserName, uidSeen = n, name, true
		case strings.HasPrefix(field, "gid="):
			n, _, err := parseIDToken(strings.TrimPrefix(field, "gid="))
			if err != nil {
				return Identity{}, errors.Wrapf(err, "parse id output for %q", principal)
			}
			id.Gid, gidSeen = n, true
		case strings.HasPrefix(field, "groups="):
			for _, tok := range strings.Split(strings.TrimPrefix(field, "groups="), ",") {
				n, name, err := parseIDToken(tok)
				if err != nil {
					return Identity{}, errors.Wrapf(err, "parse id output for %q", principal)
				}
				id.Gids = append(id.Gids, n)
				if name != "" {
					id.GroupNames[n] = name
				}
			}
		}
	}
	if !uidSeen || !gidSeen {
		return Identity{}, errors.Errorf("parse id output for %q: missing uid/gid in %q", principal, strings.TrimSpace(out))
	}
	return id, nil
}

func parseIDToken(tok string) (uint32, string, error) {
	m := idToken.FindStringSubmatch(strings.TrimSpace(tok))
	if m == nil {
		return 0, "", errors.Errorf("malformed id token %q", tok)
	}
	n, err := strconv.ParseUint(m[1], 10, 32)
	if err != nil {
		return 0, "", errors.Wrapf(err, "parse id token %q", tok)
	}
	return uint32(n), m[2], nil
}

// memberNamesOf flattens the identity's group-name map for the admin_groups
// membership check.
func memberNamesOf(id Identity) []string {
	names := make([]string, 0, len(id.GroupNames))
	for _, name := range id.GroupNames {
		names = append(names, name)
	}
	return names
}

// capsForMember returns the caps granted by admin_groups whose group list
// intersects the principal's membership (by name).
func (r *systemResolver) capsForMember(memberNames []string) []string {
	var caps []string
	for capName, groupList := range r.adminGroups {
		if hasMembership(memberNames, groupList) {
			caps = append(caps, capName)
		}
	}
	return caps
}

// hasMembership reports whether any group in groupList appears in memberNames.
func hasMembership(memberNames []string, groupList []string) bool {
	for _, g := range groupList {
		for _, m := range memberNames {
			if m == g {
				return true
			}
		}
	}
	return false
}

// mapNotFound: `id` exits non-zero for an unknown user; treat that as
// fail-closed not-found rather than a transient error.
func mapNotFound(err error) error {
	if errors.Is(err, ErrPrincipalNotFound) {
		return ErrPrincipalNotFound
	}
	var ee *exec.ExitError
	if errors.As(err, &ee) {
		return ErrPrincipalNotFound
	}
	return err
}
