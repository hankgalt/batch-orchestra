package clients

import (
	"bytes"
	"context"
	"encoding/csv"
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

type CloudStorageClientConfig struct {
	CredsPath string `json:"creds_path"`
	FileName  string `json:"file_name"`
	FilePath  string `json:"file_path"`
	Bucket    string `json:"bucket"`
}

type CloudCSVFileClient struct {
	client  *storage.Client
	source  *FileSource
	headers []string
	bo.ChunkReader
	CSVFileDataHandler
}

func NewCloudCSVFileClient(cfg CloudStorageClientConfig) (*CloudCSVFileClient, error) {
	// Check if the file name is provided
	if cfg.FileName == "" {
		return nil, ErrMissingFileName
	}

	// Check if the bucket is provided
	if cfg.Bucket == "" {
		return nil, ErrMissingBucket
	}

	os.Setenv("GOOGLE_APPLICATION_CREDENTIALS", cfg.CredsPath)
	client, err := storage.NewClient(context.Background())
	if err != nil {
		return nil, err
	}

	return &CloudCSVFileClient{
		client: client,
		source: &FileSource{
			FileName: cfg.FileName,
			FilePath: cfg.FilePath,
			Bucket:   cfg.Bucket,
		},
	}, nil
}

func (fl *CloudCSVFileClient) ReadData(ctx context.Context, offset, limit int64) (interface{}, int64, bool, error) {
	if fl.source.FileName == "" {
		return nil, 0, false, ErrMissingFileName
	}

	if fl.source.Bucket == "" {
		return nil, 0, false, ErrMissingBucket
	}

	fPath := fl.source.FileName
	if fl.source.FilePath != "" {
		fPath = filepath.Join(fl.source.FilePath, fl.source.FileName)
	}

	// check for object existence
	obj := fl.client.Bucket(fl.source.Bucket).Object(fPath)
	_, err := obj.Attrs(ctx)
	if err != nil {
		return nil, 0, false, err
	}

	// open a reader for the object in the bucket
	rc, err := obj.NewReader(ctx)
	if err != nil {
		return nil, 0, false, err
	}
	rcReadAt := &GCPStorageReadAtAdaptor{rc}
	defer func() {
		if err := rcReadAt.Reader.Close(); err != nil {
			fmt.Printf("CloudCSVFileClient:ReadData - error closing cloud file reader - file %s, err %v\n", fPath, err)
		}
	}()

	data := make([]byte, limit)
	n, err := rcReadAt.ReadAt(data, offset)
	if err != nil {
		return nil, 0, false, err
	}

	if offset < 1 {
		// Create a buffer and CSV reader to read the headers
		buffer := bytes.NewBuffer(data)
		csvReader := csv.NewReader(buffer)
		csvReader.Comma = '|'
		csvReader.FieldsPerRecord = -1

		// Read the headers from the CSV file
		headers, err := csvReader.Read()
		if err != nil {
			return nil, 0, false, err
		}
		// set CSV file headers
		fl.headers = headers
	}

	var nextOffset int64
	i := bytes.LastIndex(data, []byte{'\n'})
	if i > 0 && n == int(limit) {
		nextOffset = int64(i) + 1
	} else {
		nextOffset = int64(n)
	}

	return data[:nextOffset], nextOffset, n < int(limit), nil
}

func (fl *CloudCSVFileClient) Close() error {
	err := fl.client.Close()
	if err != nil {
		return err
	}
	return nil
}
