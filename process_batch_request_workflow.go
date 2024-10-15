package batch_orchestra

import (
	"container/list"
	"errors"
	"fmt"
	"log/slog"

	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"
)

const (
	ERR_PROCESS_BATCH_WKFL string = "error process batch request workflow"
	ERR_QUERY_HANDLER      string = "error querying process state"
	ERR_INVALID_BATCH_SIZE        = "error invalid batch size"
	ERR_WORKFLOW_QUEUE     string = "error workflow processing queue"
)

var ErrInvalidBatchSize = errors.New(ERR_INVALID_BATCH_SIZE)
var ErrorInvalidBatchSize = temporal.NewApplicationErrorWithCause(ERR_INVALID_BATCH_SIZE, ERR_INVALID_BATCH_SIZE, ErrInvalidBatchSize)

// ProcessBatchRequestWorkflow processes local/cloud csv file or a db file in batches,
// re-tries for configured number of times on retryable failures
func ProcessBatchRequestWorkflow(ctx workflow.Context, req *BatchRequest) (*BatchRequest, error) {
	l := workflow.GetLogger(ctx)
	l.Debug(
		"ProcessBatchRequestWorkflow - started",
		slog.String("file-name", req.FileName),
	)

	count := 0
	configErr := false
	resp, err := processBatchRequest(ctx, req)
	for err != nil && count < 10 && !configErr {
		count++
		switch wkflErr := err.(type) {
		case *temporal.ServerError:
			l.Error("ProcessBatchRequestWorkflow - temporal server error", slog.Any("error", err), slog.String("type", fmt.Sprintf("%T", err)))
			return req, err
		case *temporal.TimeoutError:
			l.Error("ProcessBatchRequestWorkflow - temporal time out error", slog.Any("error", err), slog.String("type", fmt.Sprintf("%T", err)))
			return req, err
		case *temporal.ApplicationError:
			l.Error("ProcessBatchRequestWorkflow - temporal application error", slog.Any("error", err), slog.String("type", fmt.Sprintf("%T", err)))
			switch wkflErr.Type() {
			case ERR_MISSING_FILE_NAME:
				return req, err
			case ERR_READING_FILE:
				return req, err
			case ERR_MISSING_START_OFFSET:
				return req, err
			case ERR_FETCHING_NEXT_OFFSET:
				return req, err
			case ERR_MISSING_BATCH_START_END:
				return req, err
			default:
				resp, err = processBatchRequest(ctx, resp)
				continue
			}
		case *temporal.PanicError:
			l.Error("ProcessBatchRequestWorkflow - temporal panic error", slog.Any("error", err), slog.String("type", fmt.Sprintf("%T", err)))
			return resp, err
		case *temporal.CanceledError:
			l.Error("ProcessBatchRequestWorkflow - temporal canceled error", slog.Any("error", err), slog.String("type", fmt.Sprintf("%T", err)))
			return resp, err
		default:
			l.Error("ProcessBatchRequestWorkflow - other error", slog.Any("error", err), slog.String("type", fmt.Sprintf("%T", err)))
			resp, err = processBatchRequest(ctx, resp)
		}
	}

	if err != nil {
		l.Error(
			"ProcessBatchRequestWorkflow - failed",
			slog.String("err-msg", err.Error()),
			slog.Int("tries", count),
			slog.String("file", req.FileName),
		)
		return resp, temporal.NewApplicationErrorWithCause(ERR_PROCESS_BATCH_WKFL, ERR_PROCESS_BATCH_WKFL, err)
	}

	l.Debug(
		"ProcessBatchRequestWorkflow - completed",
		slog.String("file", req.FileName),
	)
	return resp, nil
}

// processBatchRequest processes batch of records
// updates state with results & returns updated state
func processBatchRequest(ctx workflow.Context, req *BatchRequest) (*BatchRequest, error) {
	l := workflow.GetLogger(ctx)
	l.Debug(
		"processBatchRequest - workflow execution started",
		slog.String("file", req.FileName),
	)

	// TODO handle workflow state query

	if req.MaxBatches < 2 {
		req.MaxBatches = 2
	}

	if req.BatchSize < 2 {
		return req, ErrorInvalidBatchSize
	}

	// setup file info
	fileInfo := &FileInfo{
		FileName: req.FileName,
	}

	// TODO update file info from state

	batchSize := int64(req.BatchSize)

	// setup batch map
	if req.Batches == nil {
		req.Batches = map[string]*Batch{}
	}

	// get next batch offset
	fileInfo, err := ExecuteGetNextOffsetActivity(ctx, fileInfo, batchSize)
	if err != nil {
		l.Error(
			"processBatchRequest - error getting next offset",
			slog.Any("error", err),
			slog.String("file", req.FileName),
		)
		return req, err
	}
	l.Debug(
		"processBatchRequest - fetched next offset ",
		slog.String("file", req.FileName),
		slog.Any("offsets", fileInfo.OffSets),
	)

	// initiate a new queue
	q := list.New()

	// build batch request
	start, end := fileInfo.OffSets[len(fileInfo.OffSets)-2], fileInfo.OffSets[len(fileInfo.OffSets)-1]
	batReq := &Batch{
		FileInfo: fileInfo,
		BatchID:  fmt.Sprintf("%s-%d", fileInfo.FileName, start),
		Start:    start,
		End:      end,
	}

	req.Batches[batReq.BatchID] = batReq

	// start async execution of process batch activity
	future := AsyncExecuteProcessBatchActivity(ctx, batReq)

	// push future into queue
	q.PushBack(future)

	// while there are items in queue
	for q.Len() > 0 {
		if q.Len() < req.MaxBatches && fileInfo.End < 1 {
			// get next batch offset
			fileInfo, err = ExecuteGetNextOffsetActivity(ctx, fileInfo, batchSize)
			if err != nil {
				l.Error(
					"processBatchRequest - error getting next offset",
					slog.Any("error", err),
					slog.String("file", req.FileName))

				start, end := fileInfo.OffSets[len(fileInfo.OffSets)-1], batchSize
				nextBatReq := &Batch{
					FileInfo: fileInfo,
					BatchID:  fmt.Sprintf("%s-%d", fileInfo.FileName, start),
					Start:    start,
					End:      end,
					Error:    err,
				}

				req.Batches[nextBatReq.BatchID] = nextBatReq
			} else {
				l.Debug(
					"processBatchRequest - fetched next offset ",
					slog.String("file", req.FileName),
					slog.Any("offsets", fileInfo.OffSets),
				)

				// build next batch request
				start, end := fileInfo.OffSets[len(fileInfo.OffSets)-2], fileInfo.OffSets[len(fileInfo.OffSets)-1]
				nextBatReq := &Batch{
					FileInfo: fileInfo,
					BatchID:  fmt.Sprintf("%s-%d", fileInfo.FileName, start),
					Start:    start,
					End:      end,
				}

				req.Batches[nextBatReq.BatchID] = nextBatReq

				// start async execution of process batch activity
				future := AsyncExecuteProcessBatchActivity(ctx, nextBatReq)
				// push future into queue
				q.PushBack(future)
			}
		} else {
			future := q.Remove(q.Front()).(workflow.Future)
			var batResp Batch
			err := future.Get(ctx, &batResp)
			if err != nil {
				req.Batches[batResp.BatchID].Error = err
			} else {
				l.Debug(
					"processBatchRequest - batch result ",
					slog.String("file", batResp.FileInfo.FileName),
					slog.Any("offsets", batResp.FileInfo.OffSets),
					slog.Any("batch-id", batResp.BatchID),
					slog.Any("start", batResp.Start),
					slog.Any("end", batResp.End))

				req.Batches[batResp.BatchID] = &batResp
			}

			// edge case:
			// when max batches allowed is set to 1, queue will be empty after processing last batch
			// if not the last batch, process an additional batch to check for possible remaining records
			if q.Len() == 0 && fileInfo.End < 1 {
				start, end := fileInfo.OffSets[len(fileInfo.OffSets)-1], fileInfo.OffSets[len(fileInfo.OffSets)-1]+int64(req.BatchSize)
				l.Debug(
					"processBatchRequest - queue is empty but file end not set",
					slog.String("file", batResp.FileInfo.FileName),
					slog.Any("offsets", fileInfo.OffSets),
					slog.Any("file-end", fileInfo.End),
					slog.Any("batch-start", start),
					slog.Any("batch-end", end),
				)

				// get next batch offset
				fileInfo, err = ExecuteGetNextOffsetActivity(ctx, fileInfo, int64(req.BatchSize))
				if err != nil {
					return req, temporal.NewApplicationErrorWithCause(ERR_WORKFLOW_QUEUE, ERR_WORKFLOW_QUEUE, err)
				}
				l.Debug(
					"processBatchRequest - file info updated ",
					slog.String("file", fileInfo.FileName),
					slog.Any("offsets", fileInfo.OffSets),
					slog.Any("file-end", fileInfo.End),
				)

				// build next batch request
				nextBatReq := &Batch{
					FileInfo: fileInfo,
					BatchID:  fmt.Sprintf("%s-%d", fileInfo.FileName, start),
					Start:    start,
					End:      fileInfo.OffSets[len(fileInfo.OffSets)-1],
				}

				req.Batches[nextBatReq.BatchID] = nextBatReq

				// start async execution of process batch activity
				future := AsyncExecuteProcessBatchActivity(ctx, nextBatReq)
				// push future into queue
				q.PushBack(future)

			}
		}

	}

	return req, nil
}
