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

type CSVFileDataHandler struct {
	bo.ChunkHandler
}

func (fl *CSVFileDataHandler) HandleData(ctx context.Context, fileSrc bo.FileSource, start int64, data interface{}) (<-chan bo.Result, <-chan error, error) {
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
	bo.ChunkReader
	CSVFileDataHandler
}

func (fl *LocalCSVFileClient) ReadData(ctx context.Context, fileInfo bo.FileSource, offset, limit int64) (interface{}, int64, error) {
	localFilePath := filepath.Join(fileInfo.FilePath, fileInfo.FileName)

	// open file
	file, err := os.Open(localFilePath)
	if err != nil {
		return nil, 0, ErrMissingFile
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
			return data, int64(n), nil
		} else {
			return nil, 0, err
		}
	}

	return data, int64(n), nil
}
