package commands

import (
	"fmt"
	"strings"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// remediate wraps a client-side error with an actionable hint based on its
// kind. addr is the server endpoint, volume the target volume (may be "").
func remediate(err error, addr, volume string) error {
	if err == nil {
		return nil
	}
	switch status.Code(err) {
	case codes.Unauthenticated, codes.PermissionDenied:
		return fmt.Errorf("authentication failed connecting to %s — check username/password "+
			"(server stores argon2id hashes; generate with `gmountie genpass`): %w", addr, err)
	case codes.NotFound:
		return fmt.Errorf("volume %q not found on %s — run `gmountie ls %s` to list available volumes: %w",
			volume, addr, addr, err)
	}
	if isUnreachable(err) {
		return fmt.Errorf("server unreachable at %s — check the address/port, firewall, and that "+
			"`gmountie serve` is running: %w", addr, err)
	}
	return err
}

func isUnreachable(err error) bool {
	if status.Code(err) == codes.Unavailable {
		return true
	}
	msg := strings.ToLower(err.Error())
	for _, frag := range []string{"connection refused", "no route to host", "i/o timeout", "no such host"} {
		if strings.Contains(msg, frag) {
			return true
		}
	}
	return false
}
