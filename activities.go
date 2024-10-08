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

var (
	ErrMissingFileName      = errors.New(ERR_MISSING_FILE_NAME)
	ErrMissingStartOffset   = errors.New(ERR_MISSING_START_OFFSET)
	ErrMissingBatchStartEnd = errors.New(ERR_MISSING_BATCH_START_END)
	ErrMissingCloudBucket   = errors.New(ERR_MISSING_CLOUD_BUCKET)
	ErrUnknownFileType      = errors.New(ERR_UNKNOWN_FILE_TYPE)

	ErrMissingReaderClient = errors.New(ERR_MISSING_READER_CLIENT)
)

var (
	ErrorMissingFileName      = temporal.NewApplicationErrorWithCause(ERR_MISSING_FILE_NAME, ERR_MISSING_FILE_NAME, ErrMissingFileName)
	ErrorMissingStartOffset   = temporal.NewApplicationErrorWithCause(ERR_MISSING_START_OFFSET, ERR_MISSING_START_OFFSET, ErrMissingStartOffset)
	ErrorMissingBatchStartEnd = temporal.NewApplicationErrorWithCause(ERR_MISSING_BATCH_START_END, ERR_MISSING_BATCH_START_END, ErrMissingBatchStartEnd)

	ErrorMissingReaderClient = temporal.NewApplicationErrorWithCause(ERR_MISSING_READER_CLIENT, ERR_MISSING_READER_CLIENT, ErrMissingReaderClient)
	ErrorMissingCloudBucket  = temporal.NewApplicationErrorWithCause(ERR_MISSING_CLOUD_BUCKET, ERR_MISSING_CLOUD_BUCKET, ErrMissingCloudBucket)
	ErrorUnknownFileType     = temporal.NewApplicationErrorWithCause(ERR_UNKNOWN_FILE_TYPE, ERR_UNKNOWN_FILE_TYPE, ErrUnknownFileType)
)

// GetCSVHeadersActivity populates state with headers info for a CSV file
func GetCSVHeadersActivity(ctx context.Context, req *FileInfo, batchSize int64) (*FileInfo, error) {
	l := activity.GetLogger(ctx)
	l.Debug("GetCSVHeadersActivity - started", slog.String("file-name", req.FileName))

	// check for file name
	if req.FileName == "" {
		l.Error(ERR_MISSING_FILE_NAME)
		return req, ErrorMissingFileName
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

	data, _, err := dataSrc.ReadData(ctx, req.FileSource, int64(0), batchSize)
	if err != nil {
		if err != io.EOF {
			return req, temporal.NewApplicationErrorWithCause(ERR_READING_FILE, ERR_READING_FILE, err)
		}
	}

	buffer := bytes.NewBuffer(data.([]byte))
	csvReader := csv.NewReader(buffer)
	csvReader.Comma = '|'
	csvReader.FieldsPerRecord = -1

	headers, err := csvReader.Read()
	if err != nil {
		l.Error(ERR_READING_FILE, slog.Any("error", err), slog.String("file", req.FileName))
		return req, temporal.NewApplicationErrorWithCause(ERR_READING_FILE, ERR_READING_FILE, err)
	}
	req.Start = csvReader.InputOffset()
	req.Headers = headers

	return req, nil
}

// GetNextOffsetActivity populates state with next offset for given data file.
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

	// check for file name
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

	// setup starting sample size
	bufferSize := req.Start
	if batchSize > 0 {
		bufferSize = batchSize
	}
	if len(req.OffSets) < 1 {
		req.OffSets = []int64{req.Start}
	}

	// starting offset
	sOffset := req.OffSets[len(req.OffSets)-1]
	l.Debug("GetNextOffsetActivity - reading next chunk", slog.String("file-name", req.FileName), slog.Any("file-type", req.FileType), slog.Int64("offset", sOffset), slog.Int64("buffer-size", bufferSize))

	// read data
	buf, n, err := dataSrc.ReadData(ctx, req.FileSource, sOffset, bufferSize)
	if err != nil {
		l.Error(ERR_READING_FILE, slog.Any("error", err), slog.String("file", req.FileName))
		return req, temporal.NewApplicationErrorWithCause(ERR_READING_FILE, ERR_READING_FILE, err)
	}

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

	req.OffSets = append(req.OffSets, nextOffset)

	l.Debug("GetNextOffsetActivity - done", slog.String("file-name", req.FileName), slog.Any("file-type", req.FileType), slog.Int64("next-offset", nextOffset))
	if n < batchSize {
		l.Debug("GetNextOffsetActivity - last offset", slog.String("file-name", req.FileName), slog.Any("file-type", req.FileType))
		req.End = nextOffset
	}

	return req, nil
}

// ProcessBatchActivity processes records within given start & end indexes,
// resolves file path using configured/default data directory path
func ProcessBatchActivity(ctx context.Context, req *Batch) (*Batch, error) {
	l := activity.GetLogger(ctx)
	l.Debug("ProcessBatchActivity - started", slog.String("file-name", req.FileInfo.FileName), slog.Int64("start", req.Start), slog.Int64("end", req.End))

	// check for file name
	if req.FileInfo.FileName == "" {
		l.Error(ERR_MISSING_FILE_NAME)
		return req, ErrorMissingFileName
	}

	// check for start, end
	if req.Start >= req.End || req.End <= 0 {
		l.Error(ERR_MISSING_BATCH_START_END)
		return req, ErrorMissingBatchStartEnd
	}

	// get reader client from context
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

	// read data
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
				// assign record ID
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
				l.Debug("ProcessStoreBatchActivity - error processing record", slog.Any("error", err))
			} else {
				// error stream closed
				return req, nil
			}
		case <-ctx.Done():
			return req, ctx.Err()
		}
	}

}
