package batch_orchestra

import (
	"bytes"
	"context"
	"encoding/csv"
	"errors"
	"fmt"
	"io"
	"log/slog"

	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/temporal"
)

/**
 * activities used by batch orchestra workflow.
 */
const (
	GetCSVHeadersActivityName = "GetCSVHeadersActivity"
	GetNextOffsetActivityName = "GetNextOffsetActivity"
	ProcessBatchActivityName  = "ProcessBatchActivity"
)

// Error messages used throughout the activities
const (
	ERR_MISSING_FILE_NAME       = "error missing file name"
	ERR_READING_FILE            = "error reading file"
	ERR_MISSING_START_OFFSET    = "error missing start offset"
	ERR_FETCHING_NEXT_OFFSET    = "error fetching next offset"
	ERR_MISSING_BATCH_START_END = "error missing batch start and/or end"
	ERR_HANDLING_FILE_DATA      = "error handling file data"

	ERR_MISSING_READER_CLIENT = "error missing reader client"
	ERR_MISSING_CLOUD_BUCKET  = "error missing cloud bucket"
	ERR_UNKNOWN_FILE_TYPE     = "error unknown file type"
)

// Standard Go errors for internal use
var (
	ErrMissingFileName      = errors.New(ERR_MISSING_FILE_NAME)
	ErrMissingStartOffset   = errors.New(ERR_MISSING_START_OFFSET)
	ErrMissingBatchStartEnd = errors.New(ERR_MISSING_BATCH_START_END)
	ErrMissingCloudBucket   = errors.New(ERR_MISSING_CLOUD_BUCKET)
	ErrUnknownFileType      = errors.New(ERR_UNKNOWN_FILE_TYPE)

	ErrMissingReaderClient = errors.New(ERR_MISSING_READER_CLIENT)
)

// Temporal application errors for workflow activities
var (
	ErrorMissingFileName      = temporal.NewApplicationErrorWithCause(ERR_MISSING_FILE_NAME, ERR_MISSING_FILE_NAME, ErrMissingFileName)
	ErrorMissingStartOffset   = temporal.NewApplicationErrorWithCause(ERR_MISSING_START_OFFSET, ERR_MISSING_START_OFFSET, ErrMissingStartOffset)
	ErrorMissingBatchStartEnd = temporal.NewApplicationErrorWithCause(ERR_MISSING_BATCH_START_END, ERR_MISSING_BATCH_START_END, ErrMissingBatchStartEnd)

	ErrorMissingReaderClient = temporal.NewApplicationErrorWithCause(ERR_MISSING_READER_CLIENT, ERR_MISSING_READER_CLIENT, ErrMissingReaderClient)
	ErrorMissingCloudBucket  = temporal.NewApplicationErrorWithCause(ERR_MISSING_CLOUD_BUCKET, ERR_MISSING_CLOUD_BUCKET, ErrMissingCloudBucket)
	ErrorUnknownFileType     = temporal.NewApplicationErrorWithCause(ERR_UNKNOWN_FILE_TYPE, ERR_UNKNOWN_FILE_TYPE, ErrUnknownFileType)
)

// GetCSVHeadersActivity populates state with headers info for a CSV file
// This activity reads the headers of a CSV file and updates the FileInfo struct with the header information.
// It performs several checks to ensure the required information is present before proceeding.
func GetCSVHeadersActivity(ctx context.Context, req *FileInfo, batchSize int64) (*FileInfo, error) {
	l := activity.GetLogger(ctx)
	l.Debug("GetCSVHeadersActivity - started", slog.Any("file-info", req), slog.Int64("batch-size", batchSize))

	// check if file name is provided
	if req.FileName == "" {
		l.Error(ERR_MISSING_FILE_NAME)
		return req, ErrorMissingFileName
	}

	// check if cloud bucket is provided for cloud files
	if req.FileType == CLOUD_CSV && req.Bucket == "" {
		l.Error(ERR_MISSING_CLOUD_BUCKET)
		return req, ErrorMissingCloudBucket
	}

	// retrieve reader client from context
	dataSrc := ctx.Value(ReaderClientContextKey).(BatchRequestProcessor)
	if dataSrc == nil {
		l.Error(ERR_MISSING_READER_CLIENT)
		return req, ErrorMissingReaderClient
	}

	select {
	case <-ctx.Done():
		l.Error("GetCSVHeadersActivity - context cancelled or timed out", slog.Any("error", ctx.Err()))
		return req, ctx.Err()
	default:
		// Read data from the source starting at offset 0
		data, _, err := dataSrc.ReadData(ctx, req.FileSource, int64(0), batchSize)
		if err != nil {
			if err != io.EOF {
				return req, temporal.NewApplicationErrorWithCause(ERR_READING_FILE, ERR_READING_FILE, err)
			}
		}

		// Create a buffer and CSV reader to read the headers
		buffer := bytes.NewBuffer(data.([]byte))
		csvReader := csv.NewReader(buffer)
		csvReader.Comma = '|'
		csvReader.FieldsPerRecord = -1

		// Read the headers from the CSV file
		headers, err := csvReader.Read()
		if err != nil {
			l.Error(ERR_READING_FILE, slog.Any("error", err), slog.String("file", req.FileName))
			return req, temporal.NewApplicationErrorWithCause(ERR_READING_FILE, ERR_READING_FILE, err)
		}
		req.Start = csvReader.InputOffset()
		req.Headers = headers

		return req, nil
	}
}

// GetNextOffsetActivity populates state with next offset for given data file.
// This activity calculates the next offset for reading data from a file based on the current state and batch size.
func GetNextOffsetActivity(ctx context.Context, req *FileInfo, batchSize int64) (*FileInfo, error) {
	l := activity.GetLogger(ctx)
	l.Debug(
		"GetNextOffsetActivity - started",
		slog.String("file-name", req.FileName),
		slog.Any("file-type", req.FileType),
		slog.Any("offsets", req.OffSets),
		slog.Any("start", req.Start),
		slog.Any("end", req.End),
	)

	// Check if the file name is provided
	if req.FileName == "" {
		l.Error(ERR_MISSING_FILE_NAME)
		return req, ErrorMissingFileName
	}

	if (req.FileType == CSV || req.FileType == CLOUD_CSV) && req.Start == 0 {
		l.Debug(
			"GetNextOffsetActivity - start check",
			slog.String("file-name", req.FileName),
			slog.Any("file-type", req.FileType),
			slog.Any("start", req.Start),
		)
		l.Error(ERR_MISSING_START_OFFSET)
		return req, ErrorMissingStartOffset
	}

	// check for cloud bucket
	if req.FileType == CLOUD_CSV && req.Bucket == "" {
		l.Error(ERR_MISSING_CLOUD_BUCKET)
		return req, ErrorMissingCloudBucket
	}

	// get reader client from context
	dataSrc := ctx.Value(ReaderClientContextKey).(BatchRequestProcessor)
	if dataSrc == nil {
		l.Error(ERR_MISSING_READER_CLIENT)
		return req, ErrorMissingReaderClient
	}

	// setup batch size
	bufferSize := req.Start
	if batchSize > 0 {
		bufferSize = batchSize
	}
	if len(req.OffSets) < 1 {
		req.OffSets = []int64{req.Start}
	}

	// calculate starting offset
	sOffset := req.OffSets[len(req.OffSets)-1]
	l.Debug("GetNextOffsetActivity - reading next chunk", slog.String("file-name", req.FileName), slog.Any("file-type", req.FileType), slog.Int64("offset", sOffset), slog.Int64("buffer-size", bufferSize))

	select {
	case <-ctx.Done():
		l.Error("GetNextOffsetActivity - context cancelled or timed out", slog.Any("error", ctx.Err()))
		return req, ctx.Err()
	default:
		// read data
		buf, n, err := dataSrc.ReadData(ctx, req.FileSource, sOffset, bufferSize)
		if err != nil {
			l.Error(ERR_READING_FILE, slog.Any("error", err), slog.String("file", req.FileName))
			return req, temporal.NewApplicationErrorWithCause(ERR_READING_FILE, ERR_READING_FILE, err)
		}

		// calculate next offset
		var nextOffset int64
		if req.FileType == CSV || req.FileType == CLOUD_CSV {
			i := bytes.LastIndex(buf.([]byte), []byte{'\n'})
			if i > 0 && n == batchSize {
				nextOffset = sOffset + int64(i) + 1
			} else {
				nextOffset = sOffset + n
			}
		} else if req.FileType == DB_CURSOR {
			nextOffset = sOffset + n
		} else {
			return req, ErrorUnknownFileType
		}

		// update request offsets
		req.OffSets = append(req.OffSets, nextOffset)

		// set end of file is last batch
		l.Debug("GetNextOffsetActivity - done", slog.String("file-name", req.FileName), slog.Any("file-type", req.FileType), slog.Int64("next-offset", nextOffset))
		if n < batchSize {
			l.Debug("GetNextOffsetActivity - last offset", slog.String("file-name", req.FileName), slog.Any("file-type", req.FileType))
			req.End = nextOffset
		}

		return req, nil
	}
}

// ProcessBatchActivity processes a batch of data from a file,
// by passing the batch to reader client data handler.
func ProcessBatchActivity(ctx context.Context, req *Batch) (*Batch, error) {
	l := activity.GetLogger(ctx)
	l.Debug("ProcessBatchActivity - started", slog.String("file-name", req.FileInfo.FileName), slog.Int64("start", req.Start), slog.Int64("end", req.End))

	// Check if the file name is provided
	if req.FileInfo.FileName == "" {
		l.Error(ERR_MISSING_FILE_NAME)
		return req, ErrorMissingFileName
	}

	// Check if the batch start and end offsets are provided
	if req.Start >= req.End || req.End <= 0 {
		l.Error(ERR_MISSING_BATCH_START_END)
		return req, ErrorMissingBatchStartEnd
	}

	// Retrieve the reader client from the context
	dataSrc := ctx.Value(ReaderClientContextKey).(BatchRequestProcessor)
	if dataSrc == nil {
		l.Error(ERR_MISSING_READER_CLIENT)
		return req, ErrorMissingReaderClient
	}

	if req.BatchID == "" {
		req.BatchID = fmt.Sprintf("%s-%d", req.FileName, req.Start)
	}

	if req.Records == nil {
		req.Records = map[string]*Record{}
	}

	// Read data from the source based on the start and end offsets
	buf, _, err := dataSrc.ReadData(ctx, req.FileSource, req.Start, req.End-req.Start)
	if err != nil {
		req.Error = err
		l.Error(ERR_READING_FILE, slog.Any("error", err), slog.String("file", req.FileName))
		return req, temporal.NewApplicationErrorWithCause(ERR_READING_FILE, ERR_READING_FILE, err)
	}

	recStream, errStream, err := dataSrc.HandleData(ctx, req.FileSource, req.Start, buf)
	if err != nil {
		req.Error = err
		l.Error(ERR_HANDLING_FILE_DATA, slog.Any("error", err), slog.String("file", req.FileName))
		return req, temporal.NewApplicationErrorWithCause(ERR_HANDLING_FILE_DATA, ERR_HANDLING_FILE_DATA, err)
	}

	for {
		select {
		case rec, ok := <-recStream:
			if ok {
				// assign a unique record ID
				recordId := fmt.Sprintf("%s-%d", req.FileName, rec.Start)

				// update record state
				l.Debug("ProcessStoreBatchActivity - batch file record", slog.String("file-name", req.FileName), slog.Int64("start", rec.Start), slog.Any("record", rec.Result))
				req.Records[recordId] = &Record{
					RecordID: recordId,
					Start:    rec.Start,
					End:      rec.End,
					Status:   ok,
				}
			} else {
				// record stream closed
				return req, nil
			}
		case err, ok := <-errStream:
			if ok {
				// Log the error from the error stream
				l.Debug("ProcessStoreBatchActivity - error processing record", slog.Any("error", err))
			} else {
				// error stream closed
				return req, nil
			}
		case <-ctx.Done():
			// Context done, return the context error
			return req, ctx.Err()
		}
	}

}
