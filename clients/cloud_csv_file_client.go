package clients

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"cloud.google.com/go/storage"
	bo "github.com/hankgalt/batch-orchestra"
)

const (
	ERR_MISSING_BUCKET = "error missing bucket"
)

var (
	ErrMissingBucket = errors.New(ERR_MISSING_BUCKET)
)

type CloudStorageClientConfig struct {
	CredsPath string `json:"creds_path"`
}

type CloudCSVFileClient struct {
	client *storage.Client
	bo.ChunkReader
	CSVFileDataHandler
}

type GCPStorageReadAtAdaptor struct {
	Reader *storage.Reader
}

func (ra *GCPStorageReadAtAdaptor) ReadAt(p []byte, off int64) (n int, err error) {
	// Seek to the desired offset
	_, err = io.CopyN(io.Discard, ra.Reader, off)
	if err != nil {
		return 0, err
	}

	// Read the requested data
	return ra.Reader.Read(p)
}

func NewCloudCSVFileClient(cfg CloudStorageClientConfig) (*CloudCSVFileClient, error) {
	os.Setenv("GOOGLE_APPLICATION_CREDENTIALS", cfg.CredsPath)
	client, err := storage.NewClient(context.Background())
	if err != nil {
		return nil, err
	}

	return &CloudCSVFileClient{
		client: client,
	}, nil
}

func (fl *CloudCSVFileClient) ReadData(ctx context.Context, fileInfo bo.FileSource, offset, limit int64) (interface{}, int64, error) {
	if fileInfo.FileName == "" {
		return nil, 0, ErrMissingFileName
	}

	if fileInfo.Bucket == "" {
		return nil, 0, ErrMissingBucket
	}

	fPath := fileInfo.FileName
	if fileInfo.FilePath != "" {
		fPath = filepath.Join(fileInfo.FilePath, fileInfo.FileName)
	}

	// check for object existence
	obj := fl.client.Bucket(fileInfo.Bucket).Object(fPath)
	_, err := obj.Attrs(ctx)
	if err != nil {
		return nil, 0, err
	}

	// open a reader for the object in the bucket
	rc, err := obj.NewReader(ctx)
	if err != nil {
		return nil, 0, err
	}
	rcReadAt := &GCPStorageReadAtAdaptor{rc}
	defer func() {
		if err := rcReadAt.Reader.Close(); err != nil {
			fmt.Printf("LocalCSVFileClient:ReadData - error closing cloud file reader - file %s, err %v\n", fPath, err)
		}
	}()

	b := make([]byte, limit)

	n, err := rcReadAt.ReadAt(b, offset)
	if err != nil {
		return nil, 0, err
	}
	return b, int64(n), nil
}

func (fl *CloudCSVFileClient) Close() error {
	err := fl.client.Close()
	if err != nil {
		return err
	}
	return nil
}
