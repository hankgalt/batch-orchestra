package batch_orchestra

import (
	"time"

	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"
)

func DefaultActivityOptions() workflow.ActivityOptions {
	return workflow.ActivityOptions{
		ScheduleToStartTimeout: time.Second * 5,
		StartToCloseTimeout:    time.Minute,
		HeartbeatTimeout:       time.Second * 2, // such a short timeout to make sample fail over very fast
		RetryPolicy: &temporal.RetryPolicy{
			InitialInterval:        time.Second,
			BackoffCoefficient:     2.0,
			MaximumInterval:        time.Minute,
			NonRetryableErrorTypes: []string{"bad-error"},
		},
	}
}

func ExecuteGetCSVHeadersActivity(ctx workflow.Context, req *FileInfo, batchSize int64) (*FileInfo, error) {
	// setup activity options
	ao := DefaultActivityOptions()
	ao.RetryPolicy = &temporal.RetryPolicy{
		InitialInterval:    time.Second,
		BackoffCoefficient: 2.0,
		MaximumInterval:    time.Minute,
		MaximumAttempts:    10,
		NonRetryableErrorTypes: []string{
			ERR_MISSING_FILE_NAME,
			ERR_MISSING_CLOUD_BUCKET,
			ERR_MISSING_READER_CLIENT,
		},
	}
	ctx = workflow.WithActivityOptions(ctx, ao)

	var resp FileInfo
	err := workflow.ExecuteActivity(ctx, GetCSVHeadersActivity, req, batchSize).Get(ctx, &resp)
	if err != nil {
		return req, err
	}
	return &resp, nil
}

func ExecuteGetNextOffsetActivity(ctx workflow.Context, req *FileInfo, batchSize int64) (*FileInfo, error) {
	// setup activity options
	ao := DefaultActivityOptions()
	ao.RetryPolicy = &temporal.RetryPolicy{
		InitialInterval:    time.Second,
		BackoffCoefficient: 2.0,
		MaximumInterval:    time.Minute,
		MaximumAttempts:    10,
		NonRetryableErrorTypes: []string{
			ERR_MISSING_FILE_NAME,
			ERR_MISSING_READER_CLIENT,
			ERR_MISSING_CLOUD_BUCKET,
			ERR_MISSING_START_OFFSET,
		},
	}
	ctx = workflow.WithActivityOptions(ctx, ao)

	var resp FileInfo
	err := workflow.ExecuteActivity(ctx, GetNextOffsetActivity, req, batchSize).Get(ctx, &resp)
	if err != nil {
		return req, err
	}
	return &resp, nil
}

func AsyncExecuteProcessBatchActivity(ctx workflow.Context, req *Batch) workflow.Future {
	// setup activity options
	ao := DefaultActivityOptions()
	ao.RetryPolicy = &temporal.RetryPolicy{
		InitialInterval:    time.Second,
		BackoffCoefficient: 2.0,
		MaximumInterval:    time.Minute,
		MaximumAttempts:    10,
		NonRetryableErrorTypes: []string{
			ERR_MISSING_FILE_NAME,
			ERR_MISSING_READER_CLIENT,
			ERR_MISSING_CLOUD_BUCKET,
			ERR_MISSING_START_OFFSET,
			ERR_MISSING_BATCH_START_END,
		},
	}
	ctx = workflow.WithActivityOptions(ctx, ao)

	return workflow.ExecuteActivity(ctx, ProcessBatchActivity, req)
}
