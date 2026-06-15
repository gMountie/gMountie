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
	if fi, statErr := os.Stat(final); statErr == nil {
		if fi.Size() == int64(len(data)) {
			return hash, true, nil // genuine dedup hit
		}
		// Existing file is torn/wrong-size (crash mid-write); fall through to rewrite.
	}
	if err := os.MkdirAll(filepath.Dir(final), 0o755); err != nil {
		return hash, false, errors.Wrap(err, "mkdir chunk shard")
	}
	if err := writeChunkFile(final, data); err != nil {
		return hash, false, err
	}
	p.accMu.Lock()
	p.disk.add(int64(len(data)))
	p.accMu.Unlock()
	if err := p.enforceDiskBudget(); err != nil {
		return hash, false, errors.Wrap(err, "enforce disk budget after WriteChunk")
	}
	return hash, false, nil
}

// writeChunkFile writes data to a temp file, fsyncs it, atomically renames it
// into place, then fsyncs the parent dir — so both the bytes and the rename
// survive a crash that precedes the ~1s background meta-sync which publishes
// the index entry pointing at this chunk.
func writeChunkFile(final string, data []byte) error {
	var rnd [8]byte
	if _, err := rand.Read(rnd[:]); err != nil {
		return errors.Wrap(err, "tmp suffix rand")
	}
	tmp := final + ".tmp-" + hex.EncodeToString(rnd[:])
	f, err := os.OpenFile(tmp, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o644)
	if err != nil {
		return errors.Wrap(err, "create tmp chunk")
	}
	if _, err := f.Write(data); err != nil {
		_ = f.Close()
		_ = os.Remove(tmp)
		return errors.Wrap(err, "write tmp chunk")
	}
	if err := f.Sync(); err != nil {
		_ = f.Close()
		_ = os.Remove(tmp)
		return errors.Wrap(err, "fsync tmp chunk")
	}
	if err := f.Close(); err != nil {
		_ = os.Remove(tmp)
		return errors.Wrap(err, "close tmp chunk")
	}
	if err := os.Rename(tmp, final); err != nil {
		_ = os.Remove(tmp)
		return errors.Wrap(err, "rename chunk")
	}
	return fsyncDir(filepath.Dir(final))
}

func fsyncDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return errors.Wrap(err, "open chunk dir for fsync")
	}
	if err := d.Sync(); err != nil {
		_ = d.Close()
		return errors.Wrap(err, "fsync chunk dir")
	}
	return errors.Wrap(d.Close(), "close chunk dir")
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
// Stats the file first to debit the disk accountant accurately.
func (p *Persist) unlinkChunk(hash [16]byte) error {
	path := p.chunkPath(hash)
	p.accMu.Lock()
	defer p.accMu.Unlock()
	st, err := os.Stat(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return errors.Wrap(err, "stat chunk for unlink")
	}
	if err := os.Remove(path); err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return errors.Wrap(err, "unlink chunk")
	}
	p.disk.add(-st.Size())
	return nil
}

// seedDiskBytes walks chunks/ and sums file sizes into the disk
// accountant. Called on Open after the directory layout is in place.
func (p *Persist) seedDiskBytes() error {
	var total int64
	err := filepath.WalkDir(filepath.Join(p.root, "chunks"), func(_ string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			return nil
		}
		info, err := d.Info()
		if err != nil {
			return err
		}
		total += info.Size()
		return nil
	})
	if err != nil {
		return errors.Wrap(err, "seed disk bytes")
	}
	p.disk.add(total)
	return nil
}

// DiskBytesUsed returns the currently accounted bytes in chunks/.
func (p *Persist) DiskBytesUsed() int64 { return p.disk.Used() }

func (p *Persist) chunkPath(hash [16]byte) string {
	hx := hex.EncodeToString(hash[:])
	return filepath.Join(p.root, "chunks", hx[:2], hx[2:4], hx)
}
