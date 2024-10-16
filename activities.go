package batch_orchestra

import (
	"context"
	"errors"
	"fmt"
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
	ERR_UNKNOWN_FILE_TYPE     = "error unknown file type"
)

// Standard Go errors for internal use
var (
	ErrMissingFileName      = errors.New(ERR_MISSING_FILE_NAME)
	ErrMissingStartOffset   = errors.New(ERR_MISSING_START_OFFSET)
	ErrMissingBatchStartEnd = errors.New(ERR_MISSING_BATCH_START_END)
	ErrMissingReaderClient  = errors.New(ERR_MISSING_READER_CLIENT)
)

// Temporal application errors for workflow activities
var (
	ErrorMissingFileName      = temporal.NewApplicationErrorWithCause(ERR_MISSING_FILE_NAME, ERR_MISSING_FILE_NAME, ErrMissingFileName)
	ErrorMissingStartOffset   = temporal.NewApplicationErrorWithCause(ERR_MISSING_START_OFFSET, ERR_MISSING_START_OFFSET, ErrMissingStartOffset)
	ErrorMissingBatchStartEnd = temporal.NewApplicationErrorWithCause(ERR_MISSING_BATCH_START_END, ERR_MISSING_BATCH_START_END, ErrMissingBatchStartEnd)
	ErrorMissingReaderClient  = temporal.NewApplicationErrorWithCause(ERR_MISSING_READER_CLIENT, ERR_MISSING_READER_CLIENT, ErrMissingReaderClient)
)

// GetNextOffsetActivity populates state with next offset for given data file.
// This activity calculates the next offset for reading data from a file based on the current state and batch size.
func GetNextOffsetActivity(ctx context.Context, req *FileInfo, batchSize int64) (*FileInfo, error) {
	l := activity.GetLogger(ctx)
	l.Debug(
		"GetNextOffsetActivity - started",
		slog.String("file-name", req.FileName),
		slog.Any("offsets", req.OffSets),
		slog.Any("start", req.Start),
		slog.Any("end", req.End),
	)

	// Check if the file name is provided
	if req.FileName == "" {
		l.Error(ERR_MISSING_FILE_NAME)
		return req, ErrorMissingFileName
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
	l.Debug("GetNextOffsetActivity - reading next chunk", slog.String("file-name", req.FileName), slog.Int64("offset", sOffset), slog.Int64("buffer-size", bufferSize))

	select {
	case <-ctx.Done():
		l.Error("GetNextOffsetActivity - context cancelled or timed out", slog.Any("error", ctx.Err()))
		return req, ctx.Err()
	default:
		// read data
		_, n, last, err := dataSrc.ReadData(ctx, sOffset, bufferSize)
		if err != nil {
			l.Error(ERR_READING_FILE, slog.Any("error", err), slog.String("file", req.FileName))
			return req, temporal.NewApplicationErrorWithCause(ERR_READING_FILE, ERR_READING_FILE, err)
		}

		// calculate next offset
		nextOffset := sOffset + n

		// update request offsets
		req.OffSets = append(req.OffSets, nextOffset)

		// TODO update to set data buffer into state for batch activity to read from
		// req.datas[req.FileName] = buf // pointer or actual buffer?

		// set end of file is last batch
		l.Debug("GetNextOffsetActivity - done", slog.String("file-name", req.FileName), slog.Int64("next-offset", nextOffset))
		if last {
			l.Debug("GetNextOffsetActivity - last offset", slog.String("file-name", req.FileName))
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

	// TODO handle activity heartbeating

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
	buf, _, _, err := dataSrc.ReadData(ctx, req.Start, req.End-req.Start)
	if err != nil {
		req.Error = err
		l.Error(ERR_READING_FILE, slog.Any("error", err), slog.String("file", req.FileName))
		return req, temporal.NewApplicationErrorWithCause(ERR_READING_FILE, ERR_READING_FILE, err)
	}

	// TODO update to get data buffer from state instead of reading from source
	// buf, ok := req.datas[req.BatchID]
	// var err error
	// if !ok {
	// 	buf, _, err = dataSrc.ReadData(ctx, req.FileSource, req.Start, req.End-req.Start)
	// 	if err != nil {
	// 		req.Error = err
	// 		l.Error(ERR_READING_FILE, slog.Any("error", err), slog.String("file", req.FileName))
	// 		return req, temporal.NewApplicationErrorWithCause(ERR_READING_FILE, ERR_READING_FILE, err)
	// 	}
	// }
	// delete(req.datas, req.BatchID) // deferred? preserve for retries?

	recStream, errStream, err := dataSrc.HandleData(ctx, req.Start, buf)
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
				// TODO process record result with reader's HandleRecord method
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
