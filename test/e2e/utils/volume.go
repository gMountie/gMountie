package utils

import (
	"go.gmountie.dev/gmountie/pkg/utils/log"
	"os"
	"path/filepath"
	"strings"

	"go.uber.org/zap"
)

const (
	VolumeSrc      = "src"
	VolumeMount    = "mount"
	RandomFilesDir = "random"
)

// TestVolume is a struct that holds the test volume.
type TestVolume struct {
	// Name is the name of the volume.
	Name string
	// GeneratedFiles
	GeneratedFiles []string
	// path is the path of the volume.
	path string
	// srcOverride, when non-empty, replaces the default src subdirectory
	// so that NewTestVolumeWithExistingSrc can point a volume at a
	// caller-managed directory (for restart / dual-mount tests).
	srcOverride string
}

// NewTestVolume creates a new TestVolume.
func NewTestVolume(name string, createRandomFiles bool) (*TestVolume, error) {
	path, err := os.MkdirTemp("", "gmountie-test-*")
	if err != nil {
		return nil, err
	}
	v := &TestVolume{
		Name: name,
		path: path,
	}

	// Create subdirectories
	err = os.Mkdir(v.GetSrcPath(), 0o700)
	if err != nil {
		return nil, err
	}
	err = os.Mkdir(v.GetMountPath(), 0o700)
	if err != nil {
		return nil, err
	}
	log.Log.Info("created test volume", zap.String("name", name), zap.String("path", path))
	if createRandomFiles {
		err = v.createRandomFiles()
		if err != nil {
			return nil, err
		}
	}
	return v, nil
}

// NewTestVolumeWithExistingSrc creates a TestVolume whose server-side
// source directory is an externally managed path (not created here).
// Only the mount subdirectory is created; the caller owns srcPath's
// lifecycle (typically via t.TempDir()). Used by restart / dual-mount
// tests that need to point multiple contexts at the same src and the
// same per-volume cache directory.
func NewTestVolumeWithExistingSrc(name, srcPath string) (*TestVolume, error) {
	root, err := os.MkdirTemp("", "gmountie-test-*")
	if err != nil {
		return nil, err
	}
	v := &TestVolume{
		Name:        name,
		path:        root,
		srcOverride: srcPath,
	}
	if err := os.Mkdir(v.GetMountPath(), 0o700); err != nil {
		return nil, err
	}
	log.Log.Info("created test volume (existing src)", zap.String("name", name), zap.String("src", srcPath), zap.String("root", root))
	return v, nil
}

// Close removes the volume.
func (v *TestVolume) Close() error {
	log.Log.Info("removing test volume", zap.String("name", v.Name), zap.String("path", v.path))
	return os.RemoveAll(v.path)
}

// GetRootPath returns the root path of the volume.
func (v *TestVolume) GetRootPath() string {
	return v.path
}

// GetSrcPath returns the source path of the volume. When a srcOverride
// was set by NewTestVolumeWithExistingSrc, that path is returned
// directly rather than the default <root>/src subdirectory.
func (v *TestVolume) GetSrcPath() string {
	if v.srcOverride != "" {
		return v.srcOverride
	}
	return filepath.Join(v.path, VolumeSrc)
}

// GetMountPath returns the mount path of the volume.
func (v *TestVolume) GetMountPath() string {
	return filepath.Join(v.path, VolumeMount)
}

// GetRandomFilesPath returns the random files path of the volume.
func (v *TestVolume) GetRandomFilesPath() string {
	return filepath.Join(v.GetSrcPath(), RandomFilesDir)
}

// createRandomFiles creates random files in the source path.
func (v *TestVolume) createRandomFiles() error {
	g := &RandomFileGenerator{
		fileSize:     1024 * 1024,
		fanoutDepth:  2,
		fanoutFiles:  2,
		fanoutDirs:   2,
		randomSize:   true,
		randomFanout: true,
	}
	log.Log.Info("creating random files", zap.String("path", v.GetSrcPath()))
	// Create random directory.
	err := os.Mkdir(v.GetRandomFilesPath(), 0o700)
	if err != nil {
		return err
	}
	err = g.WriteRandomFiles(v.GetRandomFilesPath())
	if err != nil {
		return err
	}
	// Remove the source path from the generated files.
	v.GeneratedFiles = make([]string, 0, len(g.files))
	for _, f := range g.files {
		trimmed := strings.TrimPrefix(f, v.GetSrcPath())
		trimmed = strings.TrimPrefix(trimmed, "/")
		v.GeneratedFiles = append(v.GeneratedFiles, trimmed)
	}
	return nil
}
