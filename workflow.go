package batch_orchestra

import (
	"container/list"
	"errors"
	"fmt"
	"time"

	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"

	"github.com/hankgalt/batch-orchestra/pkg/domain"
)

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
		switch wkflErr := err.(type) {
		case *temporal.ApplicationError:
			l.Error(
				"ProcessBatchWorkflow - temporal application error",
				"workflow", wkflname,
				"error", err.Error(),
				"type", fmt.Sprintf("%T", err),
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
		req.Offsets = []uint64{}
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
	snapshotAO := defaultActivityOptions()

	// setup fetch & write contexts
	fetchCtx := workflow.WithActivityOptions(ctx, fetchAO)
	writeCtx := workflow.WithActivityOptions(ctx, writeAO)
	snapshotCtx := workflow.WithActivityOptions(ctx, snapshotAO)

	// TODO retry case, check for error
	// Initiate a new queue & batch count
	q := list.New()
	batchCount := uint(0)
	l.Debug(
		"processBatchWorkflow context set & queue initiated",
		"source", req.Source.Name(),
		"sink", req.Sink.Name(),
		"workflow", wkflname,
		"start-at", req.StartAt,
		"fetch-activity", fetchActivityAlias,
		"write-activity", writeActivityAlias,
	)

	// Fetch first batch from source
	var fetched domain.FetchOutput[T]
	if err := workflow.ExecuteActivity(fetchCtx, fetchActivityAlias, &domain.FetchInput[T, S]{
		Source:    req.Source,
		Offset:    req.Offsets[len(req.Offsets)-1],
		BatchSize: req.BatchSize,
	}).Get(fetchCtx, &fetched); err != nil {
		return req, err
	}
	batchCount++

	// Update request state with fetched batch
	req.Offsets = append(req.Offsets, fetched.Batch.NextOffset)
	req.Batches[domain.GetBatchId(fetched.Batch.StartOffset, fetched.Batch.NextOffset, "", "")] = fetched.Batch
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

	// While there are items in queue
	for q.Len() > 0 {
		if q.Len() < int(req.MaxInProcessBatches) && !req.Done && batchCount < req.MaxBatches {
			// If # of items in queue are less than concurrent processing limit,
			// there's more data & max batch limit not reached
			// Fetch the next batch from the source
			if err := workflow.ExecuteActivity(fetchCtx, fetchActivityAlias, &domain.FetchInput[T, S]{
				Source:    req.Source,
				Offset:    req.Offsets[len(req.Offsets)-1],
				BatchSize: req.BatchSize,
			}).Get(fetchCtx, &fetched); err != nil {
				return req, err
			}
			batchCount++

			// Update request state with fetched batch
			req.Offsets = append(req.Offsets, fetched.Batch.NextOffset)
			req.Batches[domain.GetBatchId(fetched.Batch.StartOffset, fetched.Batch.NextOffset, "", "")] = fetched.Batch
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

	result := &domain.BatchProcessingResult{
		JobID:   req.JobID,
		StartAt: req.StartAt,
		Offsets: req.Offsets,
		Batches: req.Batches,
		Done:    req.Done,
	}

	// Execute snapshot activity
	var sShot domain.BatchSnapshot
	if err := workflow.ExecuteActivity(
		snapshotCtx,
		snapshotActivityAlias,
		result,
		req.Snapshot,
		req.Snapshotter,
	).Get(snapshotCtx, &sShot); err != nil {
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
		// If snapshot was not taken successfully, build it manually & update the request
		req.Snapshot = buildRequestSnapshot(req.Batches, req.Snapshot)
	} else {
		// If snapshot was taken successfully, update the request
		sShot.Done = req.Done
		req.Snapshot = &sShot
	}

	if !req.Done && batchCount >= req.MaxBatches {
		// continue as new
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
