package batch_orchestra

import (
	"container/list"
	"errors"
	"fmt"
	"slices"
	"time"

	"go.temporal.io/api/enums/v1"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"

	"github.com/hankgalt/batch-orchestra/pkg/domain"
	"github.com/hankgalt/batch-orchestra/pkg/utils"
)

const DEFAULT_PAUSE_RECORD_COUNT = uint(1000)
const DEFAULT_PAUSE_DURATION = 15 * time.Second

const (
	FAILED_REMOVE_FUTURE = "failed to remove future from queue"
	ERR_QUERY_HANDLER    = "error setting query handler"
)

var (
	ErrFailedRemoveFuture = errors.New(FAILED_REMOVE_FUTURE)
	ErrQueryHandler       = errors.New(ERR_QUERY_HANDLER)
)

const WorkflowBatchLimit = uint(100)
const MinimumInProcessBatches = uint(2)

// ProcessBatchWorkflow processes a batch of records from a source to a sink.
func ProcessBatchWorkflow[T any, S domain.SourceConfig[T], D domain.SinkConfig[T], SS domain.SnapshotConfig](
	ctx workflow.Context,
	req *domain.BatchProcessingRequest[T, S, D, SS],
) (*domain.BatchProcessingRequest[T, S, D, SS], error) {
	l := workflow.GetLogger(ctx)

	wkflname := workflow.GetInfo(ctx).WorkflowType.Name

	l.Debug(
		"ProcessBatchWorkflow workflow started",
		"source", req.Source.Name(),
		"sink", req.Sink.Name(),
		"workflow", wkflname,
	)

	resp, err := processBatchWorkflow(ctx, req)
	if err != nil {
		var activityErr *temporal.ActivityError
		if errors.As(err, &activityErr) {
			var timeoutErr *temporal.TimeoutError
			if errors.As(activityErr, &timeoutErr) {
				switch timeoutErr.TimeoutType() {
				case enums.TIMEOUT_TYPE_START_TO_CLOSE:
					l.Error("ProcessBatchWorkflow - activity Start to Close Timeout error, will reprocess", "error", err)
					// return processBatchWorkflow(ctx, resp)
				case enums.TIMEOUT_TYPE_SCHEDULE_TO_START:
					l.Error("ProcessBatchWorkflow - activity Schedule to Start Timeout error, will reprocess", "error", err)
					// return processBatchWorkflow(ctx, resp)
				case enums.TIMEOUT_TYPE_SCHEDULE_TO_CLOSE:
					l.Error("ProcessBatchWorkflow - activity Schedule to Close Timeout error, will reprocess", "error", err)
					// return processBatchWorkflow(ctx, resp)
				case enums.TIMEOUT_TYPE_HEARTBEAT:
					l.Error("ProcessBatchWorkflow - activity Heartbeat Timeout error, returning response & error", "error", err)
				default:
					l.Error("ProcessBatchWorkflow - activity Timeout error, returning response & error", "error", err)
				}
				return resp, err
			}
		}

		switch wkflErr := err.(type) {
		case *temporal.ServerError:
			l.Error(
				"ProcessBatchWorkflow - temporal Server error",
				"workflow", wkflname,
				"error", err.Error(),
				"type", fmt.Sprintf("%T", err),
				"snapshot-idx", resp.Snapshot.SnapshotIdx,
			)
		case *temporal.TimeoutError:
			l.Error(
				"ProcessBatchWorkflow - temporal Timeout error",
				"workflow", wkflname,
				"error", err.Error(),
				"type", fmt.Sprintf("%T", err),
				"snapshot-idx", resp.Snapshot.SnapshotIdx,
			)
		case *temporal.ApplicationError:
			l.Error(
				"ProcessBatchWorkflow - temporal Application error",
				"workflow", wkflname,
				"error", err.Error(),
				"type", fmt.Sprintf("%T", err),
				"snapshot-idx", resp.Snapshot.SnapshotIdx,
			)
			switch wkflErr.Type() {
			// TODO check for known application error messages & act
			default:
			}
		default:
			l.Error(
				"ProcessBatchWorkflow - temporal error",
				"workflow", wkflname,
				"error", err.Error(),
				"type", fmt.Sprintf("%T", err),
			)
		}
		return resp, err
	}

	l.Debug(
		"ProcessBatchWorkflow workflow completed",
		"source", resp.Source.Name(),
		"sink", resp.Sink.Name(),
		"workflow", wkflname,
	)
	return resp, nil
}

// ProcessBatchWorkflow processes a batch of records from a source to a sink.
func processBatchWorkflow[T any, S domain.SourceConfig[T], D domain.SinkConfig[T], SS domain.SnapshotConfig](
	ctx workflow.Context,
	req *domain.BatchProcessingRequest[T, S, D, SS],
) (*domain.BatchProcessingRequest[T, S, D, SS], error) {
	l := workflow.GetLogger(ctx)

	// Get the workflow name
	wkflname := workflow.GetInfo(ctx).WorkflowType.Name

	// setup query handler for query type "state"
	if err := workflow.SetQueryHandler(ctx, "state", func(input []byte) (*domain.BatchProcessingRequest[T, S, D, SS], error) {
		return req, nil
	}); err != nil {
		l.Error(
			"ProcessBatchWorkflow - SetQueryHandler failed",
			"source", req.Source.Name(),
			"sink", req.Sink.Name(),
			"start-at", req.StartAt,
			"workflow", wkflname,
			"error", err.Error(),
		)
		return req, temporal.NewApplicationErrorWithCause(ERR_QUERY_HANDLER, ERR_QUERY_HANDLER, ErrQueryHandler)
	}

	// setup request state
	if req.Batches == nil {
		req.Batches = map[string]*domain.BatchProcess{}
	}
	if req.Offsets == nil {
		req.Offsets = []string{}
		req.Offsets = append(req.Offsets, req.StartAt)
	}
	if req.MaxInProcessBatches < MinimumInProcessBatches {
		req.MaxInProcessBatches = MinimumInProcessBatches
	}
	if req.MaxBatches > WorkflowBatchLimit || req.MaxBatches < MinimumInProcessBatches {
		req.MaxBatches = WorkflowBatchLimit
	}
	l.Debug(
		"processBatchWorkflow request state hydrated",
		"source", req.Source.Name(),
		"sink", req.Sink.Name(),
		"start-at", req.StartAt,
		"workflow", wkflname,
	)

	// Get the fetch and write activity aliases based on the source and sink
	fetchActivityAlias := domain.GetFetchActivityName(req.Source)
	writeActivityAlias := domain.GetWriteActivityName(req.Sink)
	snapshotActivityAlias := domain.GetSnapshotActivityName(req.Snapshotter)

	// setup activity options
	fetchAO := buildFetchActivityOptions(req)
	writeAO := buildWriteActivityOptions(req)
	snapshotAO := buildSnapshotActivityOptions(req)

	// setup fetch & write contexts
	fetchCtx := workflow.WithActivityOptions(ctx, fetchAO)
	writeCtx := workflow.WithActivityOptions(ctx, writeAO)
	snapshotCtx := workflow.WithActivityOptions(ctx, snapshotAO)

	// TODO retry case, check for error
	// Initiate a new queue & batch count
	q := list.New()
	batchCount := uint(len(req.Batches))
	l.Debug(
		"processBatchWorkflow context set & queue initiated",
		"source", req.Source.Name(),
		"sink", req.Sink.Name(),
		"workflow", wkflname,
		"start-at", req.StartAt,
		"fetch-activity", fetchActivityAlias,
		"write-activity", writeActivityAlias,
	)

	if !req.Done {
		// Fetch first batch from source
		var fetched domain.FetchOutput[T]
		if err := workflow.ExecuteActivity(fetchCtx, fetchActivityAlias, &domain.FetchInput[T, S]{
			Source:    req.Source,
			Offset:    req.Offsets[len(req.Offsets)-1],
			BatchSize: req.BatchSize,
		}).Get(fetchCtx, &fetched); err != nil {
			// Execute error snapshot activity
			if err := workflow.ExecuteActivity(
				snapshotCtx,
				snapshotActivityAlias,
				&domain.BatchProcessingResult{
					JobID:   req.JobID,
					StartAt: req.StartAt,
					Offsets: req.Offsets,
					Batches: req.Batches,
					Error:   err.Error(),
					Done:    req.Done,
				},
				true, // is an error snapshot
				req.Snapshotter,
			).Get(snapshotCtx, nil); err != nil {
				l.Error(
					"processBatchWorkflow error snapshot activity failed",
					"snapshotter", req.Snapshotter.Name(),
					"source", req.Source.Name(),
					"sink", req.Sink.Name(),
					"start-at", req.StartAt,
					"workflow", wkflname,
					"snapshot-activity", snapshotActivityAlias,
					"error", err.Error(),
				)
			}
			return req, err
		}
		batchCount++

		// Update request state with fetched batch
		req.Offsets = append(req.Offsets, fetched.Batch.NextOffset)
		fetched.Batch.BatchId = domain.GetBatchId(fetched.Batch.StartOffset, fetched.Batch.NextOffset, "", "")
		req.Batches[fetched.Batch.BatchId] = fetched.Batch
		req.Done = fetched.Batch.Done

		// Write first batch to sink (async) & push resulting future/promise to queue
		future := workflow.ExecuteActivity(writeCtx, writeActivityAlias, &domain.WriteInput[T, D]{
			Sink:  req.Sink,
			Batch: fetched.Batch,
		})
		q.PushBack(future)
		l.Debug(
			"processBatchWorkflow first batch pushed to queue",
			"source", req.Source.Name(),
			"sink", req.Sink.Name(),
			"start-at", req.StartAt,
			"workflow", wkflname,
			"fetch-activity", fetchActivityAlias,
			"write-activity", writeActivityAlias,
		)
	}

	// While there are items in queue
	for q.Len() > 0 {
		if q.Len() < int(req.MaxInProcessBatches) && !req.Done && batchCount < req.MaxBatches {
			// If # of items in queue are less than concurrent processing limit,
			// there's more data & max batch limit not reached
			// Fetch the next batch from the source
			var fetched domain.FetchOutput[T]
			if err := workflow.ExecuteActivity(fetchCtx, fetchActivityAlias, &domain.FetchInput[T, S]{
				Source:    req.Source,
				Offset:    req.Offsets[len(req.Offsets)-1],
				BatchSize: req.BatchSize,
			}).Get(fetchCtx, &fetched); err != nil {
				// Execute error snapshot activity
				if err := workflow.ExecuteActivity(
					snapshotCtx,
					snapshotActivityAlias,
					&domain.BatchProcessingResult{
						JobID:   req.JobID,
						StartAt: req.StartAt,
						Offsets: req.Offsets,
						Batches: req.Batches,
						Error:   err.Error(),
						Done:    req.Done,
					},
					true, // is an error snapshot
					req.Snapshotter,
				).Get(snapshotCtx, nil); err != nil {
					l.Error(
						"processBatchWorkflow error snapshot activity failed",
						"snapshotter", req.Snapshotter.Name(),
						"source", req.Source.Name(),
						"sink", req.Sink.Name(),
						"start-at", req.StartAt,
						"workflow", wkflname,
						"snapshot-activity", snapshotActivityAlias,
						"error", err.Error(),
					)
				}
				return req, err
			}
			batchCount++

			// Update request state with fetched batch
			req.Offsets = append(req.Offsets, fetched.Batch.NextOffset)
			fetched.Batch.BatchId = domain.GetBatchId(fetched.Batch.StartOffset, fetched.Batch.NextOffset, "", "")
			req.Batches[fetched.Batch.BatchId] = fetched.Batch
			req.Done = fetched.Batch.Done

			// Write next batch to sink (async) & push resulting future/promise to queue
			future := workflow.ExecuteActivity(writeCtx, writeActivityAlias, &domain.WriteInput[T, D]{
				Sink:  req.Sink,
				Batch: fetched.Batch,
			})
			q.PushBack(future)
		} else {
			// Remove future from queue & get output
			future, ok := q.Remove(q.Front()).(workflow.Future)
			if !ok {
				return req, temporal.NewApplicationErrorWithCause(FAILED_REMOVE_FUTURE, FAILED_REMOVE_FUTURE, ErrFailedRemoveFuture)
			}
			var wOut domain.WriteOutput[T]
			if err := future.Get(writeCtx, &wOut); err != nil {
				// Execute error snapshot activity
				if err := workflow.ExecuteActivity(
					snapshotCtx,
					snapshotActivityAlias,
					&domain.BatchProcessingResult{
						JobID:   req.JobID,
						StartAt: req.StartAt,
						Offsets: req.Offsets,
						Batches: req.Batches,
						Error:   err.Error(),
						Done:    req.Done,
					},
					true, // is an error snapshot
					req.Snapshotter,
				).Get(snapshotCtx, nil); err != nil {
					l.Error(
						"processBatchWorkflow error snapshot activity failed",
						"snapshotter", req.Snapshotter.Name(),
						"source", req.Source.Name(),
						"sink", req.Sink.Name(),
						"start-at", req.StartAt,
						"workflow", wkflname,
						"snapshot-activity", snapshotActivityAlias,
						"error", err.Error(),
					)
				}
				return req, err
			}

			// Update request state
			batchId := domain.GetBatchId(wOut.Batch.StartOffset, wOut.Batch.NextOffset, "", "")
			if _, ok := req.Batches[batchId]; !ok {
				l.Warn(
					"processBatchWorkflow missing batch initial state",
					"source", req.Source.Name(),
					"sink", req.Sink.Name(),
					"start-at", req.StartAt,
					"workflow", wkflname,
					"batch-id", batchId,
					"fetch-activity", fetchActivityAlias,
					"write-activity", writeActivityAlias,
				)
			}
			req.Batches[batchId] = wOut.Batch
		}
	}

	// Job queue is empty, all batches processed
	// If snapshotter configured & snapshot for this start offset not already done
	// build snapshot & execute snapshot activity
	if req.Snapshot == nil || !slices.Contains(req.Snapshot.SnapshotIdx, req.StartAt) {
		// Build batch processing result
		result := &domain.BatchProcessingResult{
			JobID:   req.JobID,
			StartAt: req.StartAt,
			Offsets: req.Offsets,
			Batches: req.Batches,
			Done:    req.Done,
		}

		// Build snapshot
		reqSnapshot := buildRequestSnapshot(req.Batches, req.Snapshot)
		reqSnapshot.SnapshotIdx = append(reqSnapshot.SnapshotIdx, result.StartAt)
		reqSnapshot.Done = req.Done

		// update done percentage if source has size
		if p, ok := any(req.Source).(domain.HasSize); ok {
			if p.Size() > 0 && len(result.Offsets) > 0 {
				lastOffset := result.Offsets[len(result.Offsets)-1]

				lastOffsetInt64, err := utils.ParseInt64(lastOffset)
				if err != nil {
					l.Error(
						"processBatchWorkflow error converting last offset to int64",
						"source", req.Source.Name(),
						"sink", req.Sink.Name(),
						"start-at", req.StartAt,
						"workflow", wkflname,
						"fetch-activity", fetchActivityAlias,
						"write-activity", writeActivityAlias,
						"snapshot-activity", snapshotActivityAlias,
						"error", err.Error(),
					)
				}

				reqSnapshot.DonePercentage = float32(lastOffsetInt64 / p.Size() * 100)
				result.DonePercentage = reqSnapshot.DonePercentage
			}
		}

		// Update request snapshot
		req.Snapshot = reqSnapshot

		// Execute result snapshot activity
		if err := workflow.ExecuteActivity(
			snapshotCtx,
			snapshotActivityAlias,
			result,
			false, // not an error snapshot
			req.Snapshotter,
		).Get(snapshotCtx, nil); err != nil {
			l.Error(
				"processBatchWorkflow snapshot activity failed",
				"snapshotter", req.Snapshotter.Name(),
				"source", req.Source.Name(),
				"sink", req.Sink.Name(),
				"start-at", req.StartAt,
				"workflow", wkflname,
				"snapshot-activity", snapshotActivityAlias,
				"error", err.Error(),
			)
		}
	}

	// get continue as new suggested (for debugging)
	if workflow.GetInfo(ctx).GetContinueAsNewSuggested() {
		l.Warn(
			"processBatchWorkflow continuing as new suggested",
			"curr-start-at", req.StartAt,
			"batches-processed", batchCount,
			"workflow", wkflname,
			"fetch-activity", fetchActivityAlias,
			"write-activity", writeActivityAlias,
			"snapshot-activity", snapshotActivityAlias,
		)
	}

	// Workflow execution pause logic to reduce sink load
	if req.Snapshot != nil {
		pauseRecCnt := DEFAULT_PAUSE_RECORD_COUNT
		pauseDur := DEFAULT_PAUSE_DURATION
		if req.PauseRecordCount > 0 {
			pauseRecCnt = req.PauseRecordCount
		}
		if req.PauseDuration > 0 {
			pauseDur = req.PauseDuration
		}

		if req.Snapshot.NumRecords/pauseRecCnt > req.Snapshot.PauseCount {
			l.Error(
				"processBatchWorkflow pausing to reduce sink load",
				"curr-start-at", req.StartAt,
				"batches-processed", batchCount,
				"workflow", wkflname,
				"fetch-activity", fetchActivityAlias,
				"write-activity", writeActivityAlias,
				"snapshot-activity", snapshotActivityAlias,
				"snapshot-pause-count", req.Snapshot.PauseCount,
				"snapshot-num-records", req.Snapshot.NumRecords,
				"num-processed", req.Snapshot.NumProcessed,
				"error", "pausing to reduce sink load",
			)
			// sleep for pause duration to reduce sink load
			err := workflow.Sleep(ctx, pauseDur)
			if err != nil {
				l.Error("error pausing workflow", "error", err.Error(), "workflow", wkflname)
			}
			req.Snapshot.PauseCount++
		}

	}

	// continue as new if not done & max batch count reached
	if !req.Done && batchCount >= req.MaxBatches {
		startAt := req.Offsets[len(req.Offsets)-1]
		l.Debug(
			"processBatchWorkflow continuing as new",
			"source", req.Source.Name(),
			"sink", req.Sink.Name(),
			"curr-start-at", req.StartAt,
			"new-start-at", startAt,
			"batches-processed", batchCount,
			"workflow", wkflname,
			"fetch-activity", fetchActivityAlias,
			"write-activity", writeActivityAlias,
		)

		// value copy of the struct (shallow)
		next := req
		next.StartAt = startAt
		next.Batches = nil
		next.Offsets = nil
		return nil, workflow.NewContinueAsNewError(ctx, wkflname, next)
	}

	l.Debug(
		"processBatchWorkflow workflow processed",
		"source", req.Source.Name(),
		"sink", req.Sink.Name(),
		"workflow", wkflname,
		"fetch-activity", fetchActivityAlias,
		"write-activity", writeActivityAlias,
		"batch-count", batchCount,
	)
	req.Batches = nil
	req.Offsets = nil
	return req, nil
}

func defaultActivityOptions() workflow.ActivityOptions {
	return workflow.ActivityOptions{
		StartToCloseTimeout: time.Minute * 10,
		RetryPolicy: &temporal.RetryPolicy{
			MaximumAttempts: 5,
			NonRetryableErrorTypes: []string{
				"SomeNonRetryableError",
			},
		},
	}
}

func buildFetchActivityOptions[T any, S domain.SourceConfig[T], D domain.SinkConfig[T], SS domain.SnapshotConfig](
	req *domain.BatchProcessingRequest[T, S, D, SS],
) workflow.ActivityOptions {
	// setup activity options
	ao := defaultActivityOptions()

	// Get the fetch activity alias based on the source
	fetchActivityAlias := domain.GetFetchActivityName(req.Source)

	// merge provided fetch activity options & build fetch context
	fetchAO := ao
	if policy, ok := req.Policies[fetchActivityAlias]; ok {
		if policy.MaximumAttempts > 0 {
			fetchAO.RetryPolicy.MaximumAttempts = policy.MaximumAttempts
		}
		if policy.InitialInterval > 0 {
			fetchAO.RetryPolicy.InitialInterval = policy.InitialInterval
		}
		if policy.BackoffCoefficient > 0 {
			fetchAO.RetryPolicy.BackoffCoefficient = policy.BackoffCoefficient
		}
		if policy.MaximumInterval > 0 {
			fetchAO.RetryPolicy.MaximumInterval = policy.MaximumInterval
		}
		if len(policy.NonRetryableErrorTypes) > 0 {
			fetchAO.RetryPolicy.NonRetryableErrorTypes = policy.NonRetryableErrorTypes
		}
	}

	return fetchAO
}

func buildWriteActivityOptions[T any, S domain.SourceConfig[T], D domain.SinkConfig[T], SS domain.SnapshotConfig](
	req *domain.BatchProcessingRequest[T, S, D, SS],
) workflow.ActivityOptions {
	// setup activity options
	ao := defaultActivityOptions()

	// Get the write activity aliases based on the source and sink
	writeActivityAlias := domain.GetWriteActivityName(req.Sink)

	// merge provided write activity options & build write context
	writeAO := ao
	if policy, ok := req.Policies[writeActivityAlias]; ok {
		if policy.MaximumAttempts > 0 {
			writeAO.RetryPolicy.MaximumAttempts = policy.MaximumAttempts
		}
		if policy.InitialInterval > 0 {
			writeAO.RetryPolicy.InitialInterval = policy.InitialInterval
		}
		if policy.BackoffCoefficient > 0 {
			writeAO.RetryPolicy.BackoffCoefficient = policy.BackoffCoefficient
		}
		if policy.MaximumInterval > 0 {
			writeAO.RetryPolicy.MaximumInterval = policy.MaximumInterval
		}
		if len(policy.NonRetryableErrorTypes) > 0 {
			writeAO.RetryPolicy.NonRetryableErrorTypes = policy.NonRetryableErrorTypes
		}
	}

	return writeAO
}

func buildSnapshotActivityOptions[T any, S domain.SourceConfig[T], D domain.SinkConfig[T], SS domain.SnapshotConfig](
	req *domain.BatchProcessingRequest[T, S, D, SS],
) workflow.ActivityOptions {
	// setup activity options
	ao := defaultActivityOptions()

	// Get the snapshot activity aliases based on the source and sink
	ssActivityAlias := domain.GetSnapshotActivityName(req.Snapshotter)

	// merge provided snapshot activity options & build snapshot context
	ssAO := ao
	if policy, ok := req.Policies[ssActivityAlias]; ok {
		if policy.MaximumAttempts > 0 {
			ssAO.RetryPolicy.MaximumAttempts = policy.MaximumAttempts
		}
		if policy.InitialInterval > 0 {
			ssAO.RetryPolicy.InitialInterval = policy.InitialInterval
		}
		if policy.BackoffCoefficient > 0 {
			ssAO.RetryPolicy.BackoffCoefficient = policy.BackoffCoefficient
		}
		if policy.MaximumInterval > 0 {
			ssAO.RetryPolicy.MaximumInterval = policy.MaximumInterval
		}
		if len(policy.NonRetryableErrorTypes) > 0 {
			ssAO.RetryPolicy.NonRetryableErrorTypes = policy.NonRetryableErrorTypes
		}
	}

	return ssAO
}

func buildRequestSnapshot(batches map[string]*domain.BatchProcess, snapshot *domain.BatchSnapshot) *domain.BatchSnapshot {
	errRecs := map[string][]domain.ErrorRecord{}
	numProcessed := uint(len(batches))
	numRecords := uint(0)
	pauseCount := uint(0)
	snapshotIdx := []string{}

	if snapshot != nil {
		if snapshot.Errors != nil {
			errRecs = snapshot.Errors
		}
		numProcessed += snapshot.NumProcessed
		numRecords += snapshot.NumRecords
		pauseCount = snapshot.PauseCount
		snapshotIdx = snapshot.SnapshotIdx
	}

	// Collect errors from batches
	for id, b := range batches {
		var errs []domain.ErrorRecord
		for _, r := range b.Records {
			if r.BatchResult.Error != "" {
				errs = append(errs, domain.ErrorRecord{
					Start: r.Start,
					End:   r.End,
					Error: r.BatchResult.Error,
				})
			} else {
				numRecords++
			}
		}
		if len(errs) > 0 {
			errRecs[id] = errs
		}
	}

	return &domain.BatchSnapshot{
		NumProcessed: numProcessed,
		NumRecords:   numRecords,
		Errors:       errRecs,
		PauseCount:   pauseCount,
		SnapshotIdx:  snapshotIdx,
	}
}
