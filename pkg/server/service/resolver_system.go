package service

import (
	"context"
	"os/exec"
	"regexp"
	"strconv"
	"strings"
	"time"

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
	run     commandRunner
	timeout time.Duration
}

func NewSystemResolver() IdentityResolver {
	return newSystemResolverWithRunner(execRunner, 5*time.Second)
}

func newSystemResolverWithRunner(run commandRunner, timeout time.Duration) *systemResolver {
	return &systemResolver{run: run, timeout: timeout}
}

func (r *systemResolver) Resolve(principal string) (Identity, error) {
	if !validPrincipal.MatchString(principal) {
		return Identity{}, errors.Errorf("invalid principal %q", principal)
	}
	ctx, cancel := context.WithTimeout(context.Background(), r.timeout)
	defer cancel()

	uid, err := r.num(ctx, principal, "-u")
	if err != nil {
		return Identity{}, err
	}
	gid, err := r.num(ctx, principal, "-g")
	if err != nil {
		return Identity{}, err
	}
	gout, err := r.run(ctx, "id", "-G", principal)
	if err != nil {
		return Identity{}, mapNotFound(err)
	}
	var gids []uint32
	for _, f := range strings.Fields(string(gout)) {
		if g, perr := strconv.ParseUint(f, 10, 32); perr == nil {
			gids = append(gids, uint32(g))
		}
	}
	return Identity{Principal: principal, Uid: uid, Gid: gid, Gids: gids}, nil
}

func (r *systemResolver) num(ctx context.Context, principal, flag string) (uint32, error) {
	out, err := r.run(ctx, "id", flag, principal)
	if err != nil {
		return 0, mapNotFound(err)
	}
	n, perr := strconv.ParseUint(strings.TrimSpace(string(out)), 10, 32)
	if perr != nil {
		return 0, errors.Wrapf(perr, "parse id %s output", flag)
	}
	return uint32(n), nil
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
