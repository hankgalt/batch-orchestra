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
	ProcessLocalCSVMongoWorkflowAlias   string = "process-local-csv-mongo-workflow-alias"
	ProcessCloudCSVMongoWorkflowAlias   string = "process-cloud-csv-mongo-workflow-alias"
	ProcessLocalCSVSQLLiteWorkflowAlias string = "process-local-csv-sqlite-workflow-alias"
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
func ProcessBatchWorkflow[T any, S domain.SourceConfig[T], D domain.SinkConfig[T]](
	ctx workflow.Context,
	req *domain.BatchProcessingRequest[T, S, D],
) (*domain.BatchProcessingRequest[T, S, D], error) {
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
func processBatchWorkflow[T any, S domain.SourceConfig[T], D domain.SinkConfig[T]](
	ctx workflow.Context,
	req *domain.BatchProcessingRequest[T, S, D],
) (*domain.BatchProcessingRequest[T, S, D], error) {
	l := workflow.GetLogger(ctx)

	// Get the workflow name
	wkflname := workflow.GetInfo(ctx).WorkflowType.Name

	// setup query handler for query type "state"
	if err := workflow.SetQueryHandler(ctx, "state", func(input []byte) (*domain.BatchProcessingRequest[T, S, D], error) {
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

	// Get the fetch and write activity aliases based on the source and sink
	fetchActivityAlias := domain.GetFetchActivityName(req.Source)
	writeActivityAlias := domain.GetWriteActivityName(req.Sink)

	// setup activity options
	ao := workflow.ActivityOptions{
		StartToCloseTimeout: time.Minute * 10,
		RetryPolicy: &temporal.RetryPolicy{
			MaximumAttempts: 5,
			NonRetryableErrorTypes: []string{
				"SomeNonRetryableError",
			},
		},
	}

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
	fetchCtx := workflow.WithActivityOptions(ctx, fetchAO)

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
	writeCtx := workflow.WithActivityOptions(ctx, writeAO)

	// setup request state
	if req.Batches == nil {
		req.Batches = map[string]*domain.BatchProcess[T]{}
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
		"processBatchWorkflow context set with activity options",
		"source", req.Source.Name(),
		"sink", req.Sink.Name(),
		"start-at", req.StartAt,
		"workflow", wkflname,
	)
	// TODO retry case, check for error

	// Initiate a new queue
	batchCount := uint(0)
	q := list.New()
	l.Debug(
		"processBatchWorkflow queue initiated",
		"source", req.Source.Name(),
		"sink", req.Sink.Name(),
		"workflow", wkflname,
	)

	// Fetch first batch from source
	var fetched domain.FetchOutput[T]
	if err := workflow.ExecuteActivity(fetchCtx, fetchActivityAlias, &domain.FetchInput[T, S]{
		Source:    req.Source,
		Offset:    req.Offsets[len(req.Offsets)-1],
		BatchSize: req.BatchSize,
	}).Get(ctx, &fetched); err != nil {
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
		"workflow", wkflname,
	)

	// While there are items in queue
	for q.Len() > 0 {
		if q.Len() < int(req.MaxInProcessBatches) && !req.Done && batchCount < req.MaxBatches {
			// If # of items in queue are less than concurrent processing limit & there's more data
			// Fetch the next batch from the source
			if err := workflow.ExecuteActivity(fetchCtx, fetchActivityAlias, &domain.FetchInput[T, S]{
				Source:    req.Source,
				Offset:    req.Offsets[len(req.Offsets)-1],
				BatchSize: req.BatchSize,
			}).Get(ctx, &fetched); err != nil {
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
				req.Batches[batchId] = wOut.Batch
			} else {
				req.Batches[batchId] = wOut.Batch
			}
		}
	}

	if !req.Done && batchCount >= req.MaxBatches {
		// continue as new
		startAt := req.Offsets[len(req.Offsets)-1]
		lastBatch := req.Batches[domain.GetBatchId(req.Offsets[len(req.Offsets)-2], req.Offsets[len(req.Offsets)-1], "", "")]
		l.Debug(
			"processBatchWorkflow continuing as new",
			"source", req.Source.Name(),
			"sink", req.Sink.Name(),
			"next-offset-offsets", startAt,
			"next-offset-batches", lastBatch.NextOffset,
			"batches-processed", batchCount,
			"workflow", wkflname,
		)

		req.StartAt = startAt

		return nil, workflow.NewContinueAsNewError(ctx, wkflname, req)
	}

	l.Debug(
		"processBatchWorkflow workflow processed",
		"source", req.Source.Name(),
		"sink", req.Sink.Name(),
		"workflow", wkflname,
		"batch-count", batchCount,
	)
	return req, nil
}
