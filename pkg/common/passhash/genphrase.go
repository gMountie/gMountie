package passhash

import (
	"crypto/rand"
	"encoding/base32"
	"strings"

	"github.com/pkg/errors"
)

// passphraseBytes is the entropy size; 15 bytes -> 24 base32 chars (no padding).
const passphraseBytes = 15

// GeneratePassphrase returns a crypto-random, human-transcribable passphrase
// (lowercase base32, no padding). Used for the first-run server admin password
// so we never ship a fixed credential.
func GeneratePassphrase() (string, error) {
	buf := make([]byte, passphraseBytes)
	if _, err := rand.Read(buf); err != nil {
		return "", errors.Wrap(err, "read random bytes")
	}
	enc := base32.StdEncoding.WithPadding(base32.NoPadding)
	return strings.ToLower(enc.EncodeToString(buf)), nil
}
