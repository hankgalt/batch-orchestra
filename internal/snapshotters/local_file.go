package snapshotters

import (
	"context"
	"errors"
	"os"
	"path/filepath"

	"github.com/hankgalt/batch-orchestra/pkg/domain"
)

const LocalFileSnapshotter = "local-file-snapshotter"

type localFileSnapshotter struct {
	path string
}

// Name of the snapshotter.
func (s localFileSnapshotter) Name() string { return LocalFileSnapshotter }

// Close closes the local file snapshotter.
func (s localFileSnapshotter) Close(ctx context.Context) error {
	// No resources to close for local file snapshotter
	return nil
}

func (s localFileSnapshotter) Snapshot(ctx context.Context, key string, snapshot any) error {
	// get current dir path
	dir, err := os.Getwd()
	if err != nil {
		return err
	}

	fp := filepath.Join(dir, s.path, key+".json")
	snapshotBytes, ok := snapshot.([]byte)
	if !ok {
		return errors.New("invalid snapshot format")
	}

	return os.WriteFile(fp, append(snapshotBytes, '\n'), 0o644)
}

type LocalFileSnapshotterConfig struct {
	Path string
}

// Name of the snapshotter.
func (s LocalFileSnapshotterConfig) Name() string { return LocalFileSnapshotter }

func (s LocalFileSnapshotterConfig) BuildSnapshotter(ctx context.Context) (domain.Snapshotter, error) {
	return &localFileSnapshotter{
		path: s.Path,
	}, nil
}
