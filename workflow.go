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
	ProcessLocalCSVMongoWorkflowAlias string = "process-local-csv-mongo-workflow-alias"
	ProcessCloudCSVMongoWorkflowAlias string = "process-cloud-csv-mongo-workflow-alias"
)

const (
	FAILED_REMOVE_FUTURE = "failed to remove future from queue"
)

var (
	ErrFailedRemoveFuture = errors.New(FAILED_REMOVE_FUTURE)
)

// ProcessBatchWorkflow processes a batch of records from a source to a sink.
func ProcessBatchWorkflow[T any, S domain.SourceConfig[T], D domain.SinkConfig[T]](
	ctx workflow.Context,
	req *domain.BatchProcessingRequest[T, S, D],
) (*domain.BatchProcessingRequest[T, S, D], error) {
	l := workflow.GetLogger(ctx)
	l.Debug("ProcessBatchWorkflow workflow started", "source", req.Source.Name(), "sink", req.Sink.Name())

	resp, err := processBatchWorkflow(ctx, req)
	if err != nil {
		switch wkflErr := err.(type) {
		case *temporal.ApplicationError:
			l.Error(
				"ProcessBatchWorkflow - temporal application error",
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
				"error", err.Error(),
				"type", fmt.Sprintf("%T", err),
			)
		}
		return resp, err
	}

	l.Debug("ProcessBatchWorkflow workflow completed", "source", resp.Source.Name(), "sink", resp.Sink.Name())
	return resp, nil
}

// ProcessBatchWorkflow processes a batch of records from a source to a sink.
func processBatchWorkflow[T any, S domain.SourceConfig[T], D domain.SinkConfig[T]](
	ctx workflow.Context,
	req *domain.BatchProcessingRequest[T, S, D],
) (*domain.BatchProcessingRequest[T, S, D], error) {
	l := workflow.GetLogger(ctx)

	// setup activity options
	// TODO update activity options
	ao := workflow.ActivityOptions{
		StartToCloseTimeout: time.Minute * 10,
		RetryPolicy: &temporal.RetryPolicy{
			MaximumAttempts: 5,
		},
	}
	ctx = workflow.WithActivityOptions(ctx, ao)
	l.Debug("processBatchWorkflow context set with activity options", "source", req.Source.Name(), "sink", req.Sink.Name())

	// setup request state
	if req.Batches == nil {
		req.Batches = map[string]*domain.BatchProcess[T]{}
	}
	if req.Offsets == nil {
		req.Offsets = []uint64{}
		req.Offsets = append(req.Offsets, req.StartAt)
	}
	if req.MaxBatches < 2 {
		req.MaxBatches = 2
	}

	// Get the fetch and write activity aliases based on the source and sink
	fetchActivityAlias, writeActivityAlias := getFetchActivityName(req), getWriteActivityName(req)

	// TODO retry case, check for error

	// Initiate a new queue
	q := list.New()
	l.Debug("processBatchWorkflow queue initiated", "source", req.Source.Name(), "sink", req.Sink.Name())

	// Fetch first batch from source
	var fetched domain.FetchOutput[T]
	if err := workflow.ExecuteActivity(ctx, fetchActivityAlias, &domain.FetchInput[T, S]{
		Source:    req.Source,
		Offset:    req.Offsets[len(req.Offsets)-1],
		BatchSize: req.BatchSize,
	}).Get(ctx, &fetched); err != nil {
		return req, err
	}

	// Update request state with fetched batch
	req.Offsets = append(req.Offsets, fetched.Batch.NextOffset)
	req.Batches[getBatchId(fetched.Batch.StartOffset, fetched.Batch.NextOffset, "", "")] = fetched.Batch
	req.Done = fetched.Batch.Done

	// Write first batch to sink (async) & push resulting future/promise to queue
	future := workflow.ExecuteActivity(ctx, writeActivityAlias, &domain.WriteInput[T, D]{
		Sink:  req.Sink,
		Batch: fetched.Batch,
	})
	q.PushBack(future)
	l.Debug("processBatchWorkflow first batch pushed to queue", "source", req.Source.Name(), "sink", req.Sink.Name())

	// While there are items in queue
	for q.Len() > 0 {
		if q.Len() < int(req.MaxBatches) && !req.Done {
			// If # of items in queue are less than concurrent processing limit & there's more data
			// Fetch the next batch from the source
			if err := workflow.ExecuteActivity(ctx, fetchActivityAlias, &domain.FetchInput[T, S]{
				Source:    req.Source,
				Offset:    req.Offsets[len(req.Offsets)-1],
				BatchSize: req.BatchSize,
			}).Get(ctx, &fetched); err != nil {
				return req, err
			}

			// Update request state with fetched batch
			req.Offsets = append(req.Offsets, fetched.Batch.NextOffset)
			req.Batches[getBatchId(fetched.Batch.StartOffset, fetched.Batch.NextOffset, "", "")] = fetched.Batch
			req.Done = fetched.Batch.Done

			// Write next batch to sink (async) & push resulting future/promise to queue
			future := workflow.ExecuteActivity(ctx, writeActivityAlias, &domain.WriteInput[T, D]{
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
			if err := future.Get(ctx, &wOut); err != nil {
				return req, err
			}

			// Update request state
			batchId := getBatchId(wOut.Batch.StartOffset, wOut.Batch.NextOffset, "", "")
			if _, ok := req.Batches[batchId]; !ok {
				req.Batches[batchId] = wOut.Batch
			} else {
				req.Batches[batchId] = wOut.Batch
			}
		}
	}

	// TODO setup continue as new

	l.Debug("processBatchWorkflow workflow processed", "source", req.Source.Name(), "sink", req.Sink.Name())
	return req, nil
}

func getFetchActivityName[T any, S domain.SourceConfig[T], D domain.SinkConfig[T]](req *domain.BatchProcessingRequest[T, S, D]) string {
	return "fetch-next-" + req.Source.Name() + "-batch-alias"
}

func getWriteActivityName[T any, S domain.SourceConfig[T], D domain.SinkConfig[T]](req *domain.BatchProcessingRequest[T, S, D]) string {
	return "write-next-" + req.Sink.Name() + "-batch-alias"
}

func getBatchId(start, end uint64, prefix, suffix string) string {
	if prefix == "" && suffix == "" {
		return fmt.Sprintf("batch-%d-%d", start, end)
	}

	if prefix != "" && suffix != "" {
		return fmt.Sprintf("%s-%d-%d-%s", prefix, start, end, suffix)
	}

	if prefix != "" {
		return fmt.Sprintf("%s-%d-%d", prefix, start, end)
	}

	return fmt.Sprintf("%d-%d-%s", start, end, suffix)
}
