package utils

import (
	"go.gmountie.dev/gmountie/pkg/utils/log"
	"math/rand"
	"os"
	"path"

	"github.com/thanhpk/randstr"
	"go.uber.org/zap"
)

const (
	FilenameSize = 16
)

type RandomFileGenerator struct {
	fileSize int // the size per file.

	fanoutDepth int // how deep the hierarchy goes
	fanoutFiles int // how many files per dir
	fanoutDirs  int // how many dirs per dir

	randomSeed   int64 //nolint:unused // use a random seed. if 0, use a random seed
	randomSize   bool  // randomize file sizes
	randomFanout bool  // randomize fanout numbers

	files []string
}

func (r *RandomFileGenerator) WriteRandomFiles(root string) error {
	return r.writeRandomFiles(root, 0)
}

func (r *RandomFileGenerator) writeRandomFiles(root string, depth int) error {
	numfiles := r.fanoutFiles
	if r.randomFanout {
		numfiles = rand.Intn(numfiles) + 1
	}

	for i := 0; i < numfiles; i++ {
		if err := r.writeRandomFile(root); err != nil {
			return err
		}
	}
	if depth+1 <= r.fanoutDepth {
		numdirs := r.fanoutDirs
		if r.randomFanout {
			numdirs = rand.Intn(numdirs) + 1
		}
		for i := 0; i < numdirs; i++ {
			if err := r.writeRandomDir(root, depth+1); err != nil {
				return err
			}
		}
	}
	return nil
}

func (r *RandomFileGenerator) writeRandomFile(root string) error {
	filesize := int64(r.fileSize)
	if r.randomSize {
		filesize = rand.Int63n(filesize) + 1
	}

	n := rand.Intn(FilenameSize-4) + 4
	name := randstr.String(n)
	filepath := path.Join(root, name)
	f, err := os.Create(filepath)
	if err != nil {
		return err
	}
	defer func() {
		if err := f.Close(); err != nil {
			log.Log.Fatal("failed to close file", zap.Error(err))
		}
	}()
	data := randstr.Bytes(int(filesize))
	// Write data to file
	if _, err := f.Write(data); err != nil {
		return err
	}
	r.files = append(r.files, filepath)
	return nil
}

func (r *RandomFileGenerator) writeRandomDir(root string, depth int) error {
	if depth > r.fanoutDepth {
		return nil
	}
	n := rand.Intn(FilenameSize-4) + 4
	name := randstr.String(n)
	root = path.Join(root, name)
	if err := os.MkdirAll(root, 0755); err != nil {
		return err
	}
	return r.writeRandomFiles(root, depth)
}
