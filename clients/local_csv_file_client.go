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

	bo "github.com/hankgalt/batch-orchestra"
)

const (
	ERR_MISSING_FILE      = "error missing file"
	ERR_MISSING_FILE_NAME = "error missing file name"
	ERR_INVALID_DATA      = "error invalid data format"
)

var (
	ErrMissingFile     = errors.New(ERR_MISSING_FILE)
	ErrMissingFileName = errors.New(ERR_MISSING_FILE_NAME)
	ErrInvalidData     = errors.New(ERR_INVALID_DATA)
)

type FileSource struct {
	FileName string
	FilePath string
	Bucket   string
}

type CSVFileDataHandler struct {
	bo.ChunkHandler
}

func (fl *CSVFileDataHandler) HandleData(ctx context.Context, start int64, data interface{}) (<-chan bo.Result, <-chan error, error) {
	chnk, ok := data.([]byte)
	if !ok {
		return nil, nil, ErrInvalidData
	}

	// Create a channel to stream the records and errors
	recStream, errStream := make(chan bo.Result), make(chan error)

	go func() {
		defer func() {
			close(recStream)
			close(errStream)
		}()

		// create csv reader
		buffer := bytes.NewBuffer(chnk)
		csvReader := csv.NewReader(buffer)
		csvReader.Comma = '|'
		csvReader.FieldsPerRecord = -1

		// initialize buffer offset
		bufOffset := csvReader.InputOffset()
		lastOffset := bufOffset
		recCnt := 0

		for {
			// read csv record
			record, err := csvReader.Read()

			// increment chunk record count
			recCnt++

			// handle record read error
			if err != nil {
				if err == io.EOF {
					if lastOffset < bufOffset {
						errStream <- fmt.Errorf("last record was incomplete - start: %d, end: %d", lastOffset, bufOffset)
					}
				} else {
					errStream <- err
				}
				break
			}

			// update current buffer offset
			lastOffset, bufOffset = bufOffset, csvReader.InputOffset()

			// Process CSV record (e.g., print the record)
			recStream <- bo.Result{
				Start:  start + bufOffset,
				End:    start + csvReader.InputOffset(),
				Result: record,
			}
		}
	}()

	return recStream, errStream, nil
}

type LocalCSVFileClient struct {
	source  *FileSource
	headers []string
	bo.ChunkReader
	CSVFileDataHandler
}

func NewLocalCSVFileClient(fileName, filePath string) (*LocalCSVFileClient, error) {
	// Check if the file name is provided
	if fileName == "" {
		return nil, ErrMissingFileName
	}

	return &LocalCSVFileClient{
		source: &FileSource{
			FileName: fileName,
			FilePath: filePath,
		},
	}, nil
}

func (fl *LocalCSVFileClient) ReadData(ctx context.Context, offset, limit int64) (interface{}, int64, bool, error) {
	localFilePath := filepath.Join(fl.source.FilePath, fl.source.FileName)

	// open file
	file, err := os.Open(localFilePath)
	if err != nil {
		return nil, 0, false, ErrMissingFile
	}
	defer func() {
		err := file.Close()
		if err != nil {
			fmt.Printf("LocalCSVFileClient:ReadData - error closing file - file path: %s, err: %v\n", localFilePath, err)
		}
	}()

	// read batch content
	data := make([]byte, limit)
	n, err := file.ReadAt(data, offset)
	if err != nil {
		if err == io.EOF {
			return data[:n], int64(n), true, nil
		} else {
			return nil, 0, false, err
		}
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
