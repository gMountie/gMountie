package common

import (
	"crypto/sha256"
	"encoding/hex"
)

// FingerprintID returns the first 16 hex characters of the SHA-256 hash of
// id (64 bits — enough to avoid birthday collisions in logs at ~10^4 sessions).
// Intended for logging opaque secrets (session ids) without exposing the live
// token. Returns "" for empty input.
func FingerprintID(id string) string {
	if id == "" {
		return ""
	}
	sum := sha256.Sum256([]byte(id))
	return hex.EncodeToString(sum[:])[:16]
}
