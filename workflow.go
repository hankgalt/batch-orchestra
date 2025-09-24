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

const DEFAULT_PAUSE_RECORD_COUNT = int64(1000)
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
// Workflow state can queried using "state" query type.
// Supports concurrent batch processing, configurable by max in-process batches limit.
// Supports continue-as-new for long running workflows, configurable by max batches to process.
// Supports pausing the workflow to reduce sink load, configurable by pause record count & pause duration.
// Supports snapshotting the workflow state using a snapshotter after all batches are processed or on error.
func ProcessBatchWorkflow[T any, S domain.SourceConfig[T], D domain.SinkConfig[T], SS domain.SnapshotConfig](
	ctx workflow.Context,
	req *domain.BatchProcessingRequest[T, S, D, SS],
) (*domain.BatchProcessingRequest[T, S, D, SS], error) {
	l := workflow.GetLogger(ctx)

	wkflname := workflow.GetInfo(ctx).WorkflowType.Name

	l.Debug("ProcessBatchWorkflow workflow started", "source", req.Source.Name(), "sink", req.Sink.Name(), "workflow", wkflname)

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
			l.Error("ProcessBatchWorkflow - temporal Server error", "workflow", wkflname, "error", err.Error(), "type", fmt.Sprintf("%T", err), "snapshot-idx", resp.Snapshot.SnapshotIdx)
		case *temporal.TimeoutError:
			l.Error("ProcessBatchWorkflow - temporal Timeout error", "workflow", wkflname, "error", err.Error(), "type", fmt.Sprintf("%T", err), "snapshot-idx", resp.Snapshot.SnapshotIdx)
		case *temporal.ApplicationError:
			l.Error("ProcessBatchWorkflow - temporal Application error", "workflow", wkflname, "error", err.Error(), "type", fmt.Sprintf("%T", err), "snapshot-idx", resp.Snapshot.SnapshotIdx)
			switch wkflErr.Type() {
			// TODO check for known application error messages & act
			default:
			}
		default:
			l.Error("ProcessBatchWorkflow - temporal error", "workflow", wkflname, "error", err.Error(), "type", fmt.Sprintf("%T", err))
		}
		return resp, err
	}

	l.Debug("ProcessBatchWorkflow workflow completed", "source", resp.Source.Name(), "sink", resp.Sink.Name(), "workflow", wkflname)
	return resp, nil
}

// processBatchWorkflow processes a batch of records from a source to a sink.
func processBatchWorkflow[T any, S domain.SourceConfig[T], D domain.SinkConfig[T], SS domain.SnapshotConfig](
	ctx workflow.Context,
	req *domain.BatchProcessingRequest[T, S, D, SS],
) (*domain.BatchProcessingRequest[T, S, D, SS], error) {
	l := workflow.GetLogger(ctx)

	// Get the workflow name
	wkflname := workflow.GetInfo(ctx).WorkflowType.Name

	// setup query handler for query type "state"
	if err := workflow.SetQueryHandler(ctx, "state", func(input []byte) (*domain.BatchSnapshot, error) {
		l.Debug("ProcessBatchWorkflow - query state", "source", req.Source.Name(), "sink", req.Sink.Name(), "current-start-at", req.StartAt, "workflow", wkflname, "snapshot", req.Snapshot)
		return req.Snapshot, nil
	}); err != nil {
		l.Error("ProcessBatchWorkflow - SetQueryHandler failed", "source", req.Source.Name(), "sink", req.Sink.Name(), "start-at", req.StartAt, "workflow", wkflname, "error", err.Error())
		return req, temporal.NewApplicationErrorWithCause(ERR_QUERY_HANDLER, ERR_QUERY_HANDLER, ErrQueryHandler)
	}

	// setup request state
	if req.Batches == nil {
		req.Batches = map[string]*domain.BatchProcess{}
	}
	if req.Offsets == nil {
		req.Offsets = []any{}
		req.Offsets = append(req.Offsets, req.StartAt)
	}
	if req.MaxInProcessBatches < MinimumInProcessBatches {
		req.MaxInProcessBatches = MinimumInProcessBatches
	}
	if req.MaxBatches > WorkflowBatchLimit || req.MaxBatches < MinimumInProcessBatches {
		req.MaxBatches = WorkflowBatchLimit
	}
	l.Debug("processBatchWorkflow request state hydrated", "source", req.Source.Name(), "sink", req.Sink.Name(), "start-at", req.StartAt, "workflow", wkflname)

	// Get the fetch, write & snapshot activity aliases based on the source, sink & snapshotter
	// fetchActivityAlias := domain.GetFetchActivityName(req.Source)
	writeActivityAlias := domain.GetWriteActivityName(req.Sink)
	snapshotActivityAlias := domain.GetSnapshotActivityName(req.Snapshotter)

	// setup activity options
	// fetchAO := buildFetchActivityOptions(req)
	writeAO := buildWriteActivityOptions(req)
	snapshotAO := buildSnapshotActivityOptions(req)

	// setup fetch, write & snapshot contexts
	// fetchCtx := workflow.WithActivityOptions(ctx, fetchAO)
	writeCtx := workflow.WithActivityOptions(ctx, writeAO)
	snapshotCtx := workflow.WithActivityOptions(ctx, snapshotAO)

	// TODO retry case, check for error

	// Initiate batch count & a new queue
	batchCount := len(req.Batches)
	q := list.New()
	l.Debug(
		"processBatchWorkflow context set & queue initiated",
		"source", req.Source.Name(),
		"sink", req.Sink.Name(),
		"workflow", wkflname,
		"start-at", req.StartAt,
	)

	if !req.Done {
		// Fetch first batch from source
		future, fetchedBatch, err := executeFetchAndWriteActivities(ctx, req)
		if err != nil {
			return req, err
		}
		batchCount++
		req.Offsets = append(req.Offsets, fetchedBatch.NextOffset)

		batchId, err := domain.GetBatchId(fetchedBatch.StartOffset, fetchedBatch.NextOffset, "", "")
		if err != nil {
			l.Error(
				"processBatchWorkflow get batch ID failed",
				"snapshotter", req.Snapshotter.Name(),
				"source", req.Source.Name(),
				"sink", req.Sink.Name(),
				"start-at", req.StartAt,
				"workflow", wkflname,
				"error", err.Error(),
			)
			if execErr := executeErrorSnapshotActivity(snapshotCtx, snapshotActivityAlias, req, err); execErr != nil {
				l.Error(
					"processBatchWorkflow error snapshot activity failed",
					"snapshotter", req.Snapshotter.Name(),
					"start-at", req.StartAt,
					"workflow", wkflname,
					"snapshot-activity", snapshotActivityAlias,
					"error", execErr.Error(),
				)
			}
			return req, err
		}
		fetchedBatch.BatchId = batchId
		req.Batches[fetchedBatch.BatchId] = fetchedBatch
		req.Done = fetchedBatch.Done

		// Push the write future to the queue
		q.PushBack(future)
	}

	// While there are items in queue
	for q.Len() > 0 {
		// If # of items in queue are less than concurrent processing limit,
		// there are remaining items to be processed & max batch limit is not reached
		if q.Len() < int(req.MaxInProcessBatches) && !req.Done && batchCount < int(req.MaxBatches) {
			// Fetch the next batch from the source
			future, fetchedBatch, err := executeFetchAndWriteActivities(ctx, req)
			if err != nil {
				return req, err
			}
			batchCount++
			req.Offsets = append(req.Offsets, fetchedBatch.NextOffset)

			batchId, err := domain.GetBatchId(fetchedBatch.StartOffset, fetchedBatch.NextOffset, "", "")
			if err != nil {
				l.Error(
					"processBatchWorkflow get batch ID failed",
					"snapshotter", req.Snapshotter.Name(),
					"source", req.Source.Name(),
					"sink", req.Sink.Name(),
					"start-at", req.StartAt,
					"workflow", wkflname,
					"error", err.Error(),
				)
				if execErr := executeErrorSnapshotActivity(snapshotCtx, snapshotActivityAlias, req, err); execErr != nil {
					l.Error(
						"processBatchWorkflow error snapshot activity failed",
						"snapshotter", req.Snapshotter.Name(),
						"start-at", req.StartAt,
						"workflow", wkflname,
						"snapshot-activity", snapshotActivityAlias,
						"error", execErr.Error(),
					)
				}
				return req, err
			}
			fetchedBatch.BatchId = batchId
			req.Batches[fetchedBatch.BatchId] = fetchedBatch
			req.Done = fetchedBatch.Done

			// Push the write future to the queue
			q.PushBack(future)
		} else {
			// Remove future from queue & get output
			future, ok := q.Remove(q.Front()).(workflow.Future)
			if !ok {
				return req, temporal.NewApplicationErrorWithCause(FAILED_REMOVE_FUTURE, FAILED_REMOVE_FUTURE, ErrFailedRemoveFuture)
			}
			var wOut domain.WriteOutput[T]
			if err := future.Get(writeCtx, &wOut); err != nil {
				// If error, execute error snapshot activity & return
				if execErr := executeErrorSnapshotActivity(snapshotCtx, snapshotActivityAlias, req, err); execErr != nil {
					l.Error(
						"processBatchWorkflow error snapshot activity failed",
						"snapshotter", req.Snapshotter.Name(),
						"source", req.Source.Name(),
						"sink", req.Sink.Name(),
						"start-at", req.StartAt,
						"workflow", wkflname,
						"snapshot-activity", snapshotActivityAlias,
						"error", execErr.Error(),
					)
				}
				return req, err
			}

			batchId, err := domain.GetBatchId(wOut.Batch.StartOffset, wOut.Batch.NextOffset, "", "")
			if err != nil {
				l.Error(
					"processBatchWorkflow get batch ID failed",
					"snapshotter", req.Snapshotter.Name(),
					"source", req.Source.Name(),
					"sink", req.Sink.Name(),
					"start-at", req.StartAt,
					"workflow", wkflname,
					"error", err.Error(),
				)
				if execErr := executeErrorSnapshotActivity(snapshotCtx, snapshotActivityAlias, req, err); execErr != nil {
					l.Error(
						"processBatchWorkflow error snapshot activity failed",
						"snapshotter", req.Snapshotter.Name(),
						"start-at", req.StartAt,
						"workflow", wkflname,
						"snapshot-activity", snapshotActivityAlias,
						"error", execErr.Error(),
					)
				}
				return req, err
			}

			// Update request state
			if _, ok := req.Batches[batchId]; !ok {
				l.Warn(
					"processBatchWorkflow write activity returned unknown batch ID",
					"batch-id", batchId,
					"start-at", req.StartAt,
					"workflow", wkflname,
					"write-activity", writeActivityAlias,
				)
			}
			req.Batches[batchId] = wOut.Batch
		}
	}

	// Job queue is empty, all batches processed
	// If snapshotter is configured & snapshot for this start offset not already done
	// build snapshot & execute snapshot activity
	if req.Snapshot == nil || !slices.Contains(req.Snapshot.SnapshotIdx, req.StartAt) {
		if reqSnapshot, execErr := executeResultSnapshotActivity(snapshotCtx, snapshotActivityAlias, req); execErr != nil {
			l.Error(
				"processBatchWorkflow snapshot activity failed",
				"snapshotter", req.Snapshotter.Name(),
				"source", req.Source.Name(),
				"sink", req.Sink.Name(),
				"start-at", req.StartAt,
				"workflow", wkflname,
				"snapshot-activity", snapshotActivityAlias,
				"error", execErr.Error(),
			)
		} else {
			// Update request snapshot & state snapshot
			req.Snapshot = reqSnapshot
		}
	}

	// Get continue as new suggested (for debugging)
	if workflow.GetInfo(ctx).GetContinueAsNewSuggested() {
		l.Warn(
			"processBatchWorkflow continuing as new suggested",
			"curr-start-at", req.StartAt,
			"batches-processed", batchCount,
			"workflow", wkflname,
			"write-activity", writeActivityAlias,
			"snapshot-activity", snapshotActivityAlias,
		)
	}

	// Check if workflow needs to be paused to reduce sink load
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
				"write-activity", writeActivityAlias,
				"snapshot-activity", snapshotActivityAlias,
				"snapshot-pause-count", req.Snapshot.PauseCount,
				"snapshot-num-records", req.Snapshot.NumRecords,
				"num-processed", req.Snapshot.NumBatches,
			)
			// Sleep for pause duration to reduce sink load
			err := workflow.Sleep(ctx, pauseDur)
			if err != nil {
				l.Error("error pausing workflow", "error", err.Error(), "workflow", wkflname)
			}
			req.Snapshot.PauseCount++
		}

	}

	// Continue as new if items remaining but max batch count limit is reached
	if !req.Done && batchCount >= int(req.MaxBatches) {
		startAt := req.Offsets[len(req.Offsets)-1]
		l.Debug(
			"processBatchWorkflow continuing as new",
			"source", req.Source.Name(),
			"sink", req.Sink.Name(),
			"curr-start-at", req.StartAt,
			"new-start-at", startAt,
			"batches-processed", batchCount,
			"workflow", wkflname,
			"write-activity", writeActivityAlias,
		)

		// value copy of the request struct for continue as new
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
		"write-activity", writeActivityAlias,
		"batch-count", batchCount,
	)
	return req, nil
}

// executeFetchAndWriteActivities executes the fetch and write activities and returns the write future and fetched batch.
func executeFetchAndWriteActivities[T any, S domain.SourceConfig[T], D domain.SinkConfig[T], SS domain.SnapshotConfig](
	ctx workflow.Context,
	req *domain.BatchProcessingRequest[T, S, D, SS],
) (workflow.Future, *domain.BatchProcess, error) {
	l := workflow.GetLogger(ctx)

	wkflname := workflow.GetInfo(ctx).WorkflowType.Name

	fetchActivityAlias := domain.GetFetchActivityName(req.Source)
	writeActivityAlias := domain.GetWriteActivityName(req.Sink)
	snapshotActivityAlias := domain.GetSnapshotActivityName(req.Snapshotter)

	fetchAO := buildFetchActivityOptions(req)
	writeAO := buildWriteActivityOptions(req)
	snapshotAO := buildSnapshotActivityOptions(req)

	fetchCtx := workflow.WithActivityOptions(ctx, fetchAO)
	writeCtx := workflow.WithActivityOptions(ctx, writeAO)
	snapshotCtx := workflow.WithActivityOptions(ctx, snapshotAO)

	var fetched domain.FetchOutput[T]
	if err := workflow.ExecuteActivity(fetchCtx, fetchActivityAlias, &domain.FetchInput[T, S]{
		Source:    req.Source,
		Offset:    req.Offsets[len(req.Offsets)-1],
		BatchSize: req.BatchSize,
	}).Get(fetchCtx, &fetched); err != nil {
		l.Error(
			"executeFetchAndWriteActivities fetch activity failed",
			"source", req.Source.Name(),
			"start-at", req.StartAt,
			"workflow", wkflname,
			"fetch-activity", fetchActivityAlias,
			"error", err.Error(),
		)
		if execErr := executeErrorSnapshotActivity(snapshotCtx, snapshotActivityAlias, req, err); execErr != nil {
			l.Error(
				"executeFetchAndWriteActivities error snapshot activity failed",
				"snapshotter", req.Snapshotter.Name(),
				"start-at", req.StartAt,
				"workflow", wkflname,
				"snapshot-activity", snapshotActivityAlias,
				"error", execErr.Error(),
			)
		}
		return nil, nil, err
	}
	// Write next batch to sink (async) & push resulting future/promise to queue
	future := workflow.ExecuteActivity(writeCtx, writeActivityAlias, &domain.WriteInput[T, D]{
		Sink:  req.Sink,
		Batch: fetched.Batch,
	})

	return future, fetched.Batch, nil
}

// executeResultSnapshotActivity builds and executes a result snapshot activity
func executeResultSnapshotActivity[T any, S domain.SourceConfig[T], D domain.SinkConfig[T], SS domain.SnapshotConfig](
	ctx workflow.Context,
	snapshotActivityAlias string,
	req *domain.BatchProcessingRequest[T, S, D, SS],
) (*domain.BatchSnapshot, error) {
	l := workflow.GetLogger(ctx)

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

	result.NumRecords = reqSnapshot.NumRecords
	result.NumBatches = reqSnapshot.NumBatches

	// update done percentage if source has size
	if p, ok := any(req.Source).(domain.HasSize); ok {
		if p.Size() > 0 && len(result.Offsets) > 0 {
			lastOffset := result.Offsets[len(result.Offsets)-1]

			if lastOffsetStr, ok := lastOffset.(string); ok {
				if lastOffsetInt64, err := utils.ParseInt64(lastOffsetStr); err == nil {
					reqSnapshot.DonePercentage = float32(lastOffsetInt64 / p.Size() * 100)
					result.DonePercentage = reqSnapshot.DonePercentage
				} else {
					l.Error("executeResultSnapshotActivity error converting last offset to int64 for done percentage", "source", req.Source.Name(), "sink", req.Sink.Name(), "start-at", req.StartAt, "snapshot-activity", snapshotActivityAlias, "last-offset", lastOffset, "size", p.Size(), "error", err.Error())
				}
			}
		}
	}

	// Execute result snapshot activity
	if err := workflow.ExecuteActivity(
		ctx,
		snapshotActivityAlias,
		result,
		false, // not an error snapshot
		req.Snapshotter,
	).Get(ctx, nil); err != nil {
		return reqSnapshot, err
	}
	return reqSnapshot, nil
}

// executeErrorSnapshotActivity builds and executes an error snapshot activity
func executeErrorSnapshotActivity[T any, S domain.SourceConfig[T], D domain.SinkConfig[T], SS domain.SnapshotConfig](
	ctx workflow.Context,
	snapshotActivityAlias string,
	req *domain.BatchProcessingRequest[T, S, D, SS],
	snapshotErr error,
) error {
	// Build snapshot
	reqSnapshot := buildRequestSnapshot(req.Batches, req.Snapshot)

	// build error result
	result := &domain.BatchProcessingResult{
		JobID:      req.JobID,
		StartAt:    req.StartAt,
		Offsets:    req.Offsets,
		Batches:    req.Batches,
		Error:      snapshotErr.Error(),
		Done:       req.Done,
		NumRecords: reqSnapshot.NumRecords,
		NumBatches: reqSnapshot.NumBatches,
	}

	// Execute error snapshot activity
	if err := workflow.ExecuteActivity(
		ctx,
		snapshotActivityAlias,
		result,
		true, // is an error snapshot
		req.Snapshotter,
	).Get(ctx, nil); err != nil {
		return err
	}
	return nil
}

// defaultActivityOptions returns the default activity options.
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

// buildFetchActivityOptions builds the fetch activity options.
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

// buildWriteActivityOptions builds the write activity options.
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

// buildSnapshotActivityOptions builds the snapshot activity options.
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

// buildRequestSnapshot builds a snapshot from the batches and previous snapshot.
func buildRequestSnapshot(batches map[string]*domain.BatchProcess, snapshot *domain.BatchSnapshot) *domain.BatchSnapshot {
	errRecs := map[string][]domain.ErrorRecord{}
	numBatches := int64(len(batches))
	numRecords := int64(0)
	pauseCount := int64(0)
	snapshotIdx := []any{}

	if snapshot != nil {
		if snapshot.Errors != nil {
			errRecs = snapshot.Errors
		}
		numBatches += snapshot.NumBatches
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
		NumBatches:  numBatches,
		NumRecords:  numRecords,
		Errors:      errRecs,
		PauseCount:  pauseCount,
		SnapshotIdx: snapshotIdx,
	}
}
