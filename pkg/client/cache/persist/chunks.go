package persist

import (
	"crypto/rand"
	"encoding/hex"
	"os"
	"path/filepath"

	"github.com/pkg/errors"
	"github.com/zeebo/xxh3"
)

// WriteChunk hashes data with xxh3-128, writes it to chunks/aa/bb/<hex>
// via tmp+rename for atomicity, and returns the 16-byte hash. dedup is
// true when the target file already existed (identical bytes had been
// stored before) — callers that ref-count should still incRef on every
// WriteChunk regardless of dedup.
func (p *Persist) WriteChunk(data []byte) (hash [16]byte, dedup bool, err error) {
	h := xxh3.Hash128(data)
	hash[0] = byte(h.Hi >> 56)
	hash[1] = byte(h.Hi >> 48)
	hash[2] = byte(h.Hi >> 40)
	hash[3] = byte(h.Hi >> 32)
	hash[4] = byte(h.Hi >> 24)
	hash[5] = byte(h.Hi >> 16)
	hash[6] = byte(h.Hi >> 8)
	hash[7] = byte(h.Hi)
	hash[8] = byte(h.Lo >> 56)
	hash[9] = byte(h.Lo >> 48)
	hash[10] = byte(h.Lo >> 40)
	hash[11] = byte(h.Lo >> 32)
	hash[12] = byte(h.Lo >> 24)
	hash[13] = byte(h.Lo >> 16)
	hash[14] = byte(h.Lo >> 8)
	hash[15] = byte(h.Lo)

	final := p.chunkPath(hash)
	if _, statErr := os.Stat(final); statErr == nil {
		return hash, true, nil
	}
	if err := os.MkdirAll(filepath.Dir(final), 0o755); err != nil {
		return hash, false, errors.Wrap(err, "mkdir chunk shard")
	}

	var rnd [8]byte
	if _, err := rand.Read(rnd[:]); err != nil {
		return hash, false, errors.Wrap(err, "tmp suffix rand")
	}
	tmp := final + ".tmp-" + hex.EncodeToString(rnd[:])
	if err := os.WriteFile(tmp, data, 0o644); err != nil {
		return hash, false, errors.Wrap(err, "write tmp chunk")
	}
	if err := os.Rename(tmp, final); err != nil {
		_ = os.Remove(tmp)
		return hash, false, errors.Wrap(err, "rename chunk")
	}
	return hash, false, nil
}

// ReadChunk loads the chunk at hash. Returns an error (wrapping
// os.ErrNotExist via pkg/errors) when the chunk is missing.
func (p *Persist) ReadChunk(hash [16]byte) ([]byte, error) {
	data, err := os.ReadFile(p.chunkPath(hash))
	if err != nil {
		return nil, errors.Wrap(err, "read chunk")
	}
	return data, nil
}

// unlinkChunk removes the on-disk file backing hash. Idempotent.
// Called by decRef when the refcount hits zero (added in task 3).
func (p *Persist) unlinkChunk(hash [16]byte) error {
	err := os.Remove(p.chunkPath(hash))
	if err != nil && !os.IsNotExist(err) {
		return errors.Wrap(err, "unlink chunk")
	}
	return nil
}

func (p *Persist) chunkPath(hash [16]byte) string {
	hx := hex.EncodeToString(hash[:])
	return filepath.Join(p.root, "chunks", hx[:2], hx[2:4], hx)
}
