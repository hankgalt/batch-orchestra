package clients

import (
	"bytes"
	"context"
	"encoding/csv"
	"fmt"
	"io"
	"os"
	"path/filepath"

	bo "github.com/hankgalt/batch-orchestra"
)

type CSVFileDataHandler struct {
	bo.ChunkHandler
}

func (fl *CSVFileDataHandler) HandleData(ctx context.Context, fileSrc bo.FileSource, data interface{}) (<-chan interface{}, <-chan error, error) {
	chnk, ok := data.([]byte)
	if !ok {
		return nil, nil, fmt.Errorf("invalid data format")
	}

	// Create a channel to stream the records and errors
	recStream, errStream := make(chan interface{}), make(chan error)

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
			recStream <- record
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
		return nil, 0, err
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
