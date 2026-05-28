// Package passhash implements argon2id password hashing in the standard
// PHC string format ($argon2id$v=19$m=...,t=...,p=...$<salt>$<hash>) used
// by gMountie's basic-auth credentials and ops endpoint auth. The PHC
// format encodes the algorithm version and tuning parameters so verify
// can recompute the hash with the right knobs even if defaults change
// later.
package passhash

import (
	"crypto/rand"
	"crypto/subtle"
	"encoding/base64"
	"errors"
	"fmt"
	"strconv"
	"strings"

	"golang.org/x/crypto/argon2"
)

// PHC tuning parameters — locked by Phase 7 spec §3.3 to match OWASP 2026
// argon2id guidance. Verify reads these from the stored hash so changing
// the defaults later does not invalidate already-stored credentials.
const (
	defaultTime    uint32 = 3
	defaultMemory  uint32 = 64 * 1024 // KiB = 64 MiB
	defaultThreads uint8  = 4
	keyLen         uint32 = 32
	saltLen        int    = 16
	phcPrefix      string = "$argon2id$"
	phcVersionTag  string = "v=" + "19" // argon2id v1.3
)

var errMalformedHash = errors.New("passhash: malformed PHC string")

// Hash returns a PHC-encoded argon2id hash of password. The salt is read
// from crypto/rand and is fresh per call; two Hashes of the same password
// return different strings.
func Hash(password string) (string, error) {
	salt := make([]byte, saltLen)
	if _, err := rand.Read(salt); err != nil {
		return "", fmt.Errorf("passhash: read salt: %w", err)
	}
	sum := argon2.IDKey([]byte(password), salt, defaultTime, defaultMemory, defaultThreads, keyLen)
	enc := base64.RawStdEncoding
	return fmt.Sprintf("%s%s$m=%d,t=%d,p=%d$%s$%s",
		phcPrefix, phcVersionTag,
		defaultMemory, defaultTime, defaultThreads,
		enc.EncodeToString(salt), enc.EncodeToString(sum)), nil
}

// Verify reports whether password matches the PHC-encoded hash. Returns
// (true, nil) on match, (false, nil) on mismatch, (false, err) on a hash
// that can't be parsed. Constant-time on the final hash compare.
func Verify(phc, password string) (bool, error) {
	if !strings.HasPrefix(phc, phcPrefix) {
		return false, fmt.Errorf("%w: not an argon2id hash", errMalformedHash)
	}
	// Format: $argon2id$v=19$m=65536,t=3,p=4$<salt>$<hash>
	parts := strings.Split(phc, "$")
	if len(parts) != 6 || parts[0] != "" || parts[1] != "argon2id" {
		return false, fmt.Errorf("%w: unexpected segment count", errMalformedHash)
	}
	if parts[2] != phcVersionTag {
		return false, fmt.Errorf("%w: unsupported version %q", errMalformedHash, parts[2])
	}
	mem, time, threads, err := parseParams(parts[3])
	if err != nil {
		return false, err
	}
	enc := base64.RawStdEncoding
	salt, err := enc.DecodeString(parts[4])
	if err != nil {
		return false, fmt.Errorf("%w: decode salt: %w", errMalformedHash, err)
	}
	want, err := enc.DecodeString(parts[5])
	if err != nil {
		return false, fmt.Errorf("%w: decode hash: %w", errMalformedHash, err)
	}
	got := argon2.IDKey([]byte(password), salt, time, mem, threads, uint32(len(want)))
	return subtle.ConstantTimeCompare(got, want) == 1, nil
}

// IsHashed is a cheap prefix check: true when s looks like a $argon2id$ PHC
// string. Used at config-load time to fail closed on cleartext entries —
// it doesn't validate the rest of the format (Verify will).
func IsHashed(s string) bool { return strings.HasPrefix(s, phcPrefix) }

func parseParams(s string) (mem, time uint32, threads uint8, err error) {
	// "m=65536,t=3,p=4"
	fields := strings.Split(s, ",")
	if len(fields) != 3 {
		return 0, 0, 0, fmt.Errorf("%w: params %q", errMalformedHash, s)
	}
	for _, f := range fields {
		kv := strings.SplitN(f, "=", 2)
		if len(kv) != 2 {
			return 0, 0, 0, fmt.Errorf("%w: param %q", errMalformedHash, f)
		}
		v, perr := strconv.ParseUint(kv[1], 10, 32)
		if perr != nil {
			return 0, 0, 0, fmt.Errorf("%w: param %q: %w", errMalformedHash, f, perr)
		}
		switch kv[0] {
		case "m":
			mem = uint32(v)
		case "t":
			time = uint32(v)
		case "p":
			threads = uint8(v)
		default:
			return 0, 0, 0, fmt.Errorf("%w: unknown param key %q", errMalformedHash, kv[0])
		}
	}
	return mem, time, threads, nil
}
