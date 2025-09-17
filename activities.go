package batch_orchestra

import (
	"context"
	"encoding/json"
	"fmt"

	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/temporal"

	"github.com/hankgalt/batch-orchestra/pkg/domain"
)

type ActivityAlias string

// FetchNextActivity fetches the next batch of data from the source.
// It builds the source from the provided configuration, fetches the next batch,
// and records a heartbeat for the activity.
// Returns the fetched batch with read details or an error.
func FetchNextActivity[T any, S domain.SourceConfig[T]](
	ctx context.Context,
	in *domain.FetchInput[T, S],
) (*domain.FetchOutput[T], error) {
	l := activity.GetLogger(ctx)
	l.Debug("FetchNextActivity - started", "input-offset", in.Offset)

	src, err := in.Source.BuildSource(ctx)
	if err != nil {
		l.Error("FetchNextActivity - error building source", "error", err.Error())
		return nil, temporal.NewApplicationErrorWithCause(err.Error(), err.Error(), err)
	}
	// Ensure the source is closed after use
	defer func() {
		if err := src.Close(ctx); err != nil {
			l.Error("FetchNextActivity - error closing source", "error", err.Error())
		}
	}()

	// Fetch the next batch from the source
	b, err := src.Next(ctx, in.Offset, in.BatchSize)
	if err != nil {
		l.Error("FetchNextActivity - error fetching next batch", "error", err.Error())
		return nil, temporal.NewApplicationErrorWithCause(err.Error(), err.Error(), err)
	}

	// record activity heartbeat
	activity.RecordHeartbeat(ctx, in)

	return &domain.FetchOutput[T]{
		Batch: b,
	}, nil
}

// WriteActivity writes a batch of data to the sink.
// It builds the sink from the provided configuration, writes the batch,
// and records a heartbeat for the activity.
// Returns the written batch with result details or an error.
func WriteActivity[T any, D domain.SinkConfig[T]](
	ctx context.Context,
	in *domain.WriteInput[T, D],
) (*domain.WriteOutput[T], error) {
	l := activity.GetLogger(ctx)
	l.Debug("WriteActivity - started", "start-offset", in.Batch.StartOffset, "next-offset", in.Batch.NextOffset)

	sk, err := in.Sink.BuildSink(ctx)
	if err != nil {
		l.Error("WriteActivity - error building sink", "error", err.Error())
		return nil, temporal.NewApplicationErrorWithCause(err.Error(), err.Error(), err)
	}
	defer func() {
		if err := sk.Close(ctx); err != nil {
			l.Error("WriteActivity - error closing sink", "error", err.Error())
		}
	}()

	// Write the batch to the sink
	out, err := sk.Write(ctx, in.Batch)
	if err != nil {
		l.Error("WriteActivity - error writing to sink", "error", err.Error())
		return nil, temporal.NewApplicationErrorWithCause(err.Error(), err.Error(), err)
	}

	// record activity heartbeat
	activity.RecordHeartbeat(ctx, in)

	return &domain.WriteOutput[T]{
		Batch: out,
	}, nil
}

// SnapshotActivity saves a snapshot of the batch processing result using the provided snapshot configuration.
// It builds the snapshotter from the configuration, serializes the result to JSON,
// and saves it with a key based on the job ID and timestamp.
// If isError is true, the key will indicate an error snapshot.
// Returns an error if any step fails.
func SnapshotActivity[SS domain.SnapshotConfig](
	ctx context.Context,
	result *domain.BatchProcessingResult,
	isError bool,
	snapCfg SS,
) error {
	l := activity.GetLogger(ctx)
	l.Debug("SnapshotActivity - started", "name", snapCfg.Name(), "job-id", result.JobID)

	if result == nil {
		l.Error("SnapshotActivity - nil batch processing result provided")
		return nil
	}

	sk, err := snapCfg.BuildSnapshotter(ctx)
	if err != nil {
		l.Error("SnapshotActivity - error building snapshotter", "name", snapCfg.Name(), "error", err.Error())
		return temporal.NewApplicationErrorWithCause(err.Error(), err.Error(), err)
	}
	defer func() {
		if err := sk.Close(ctx); err != nil {
			l.Error("SnapshotActivity - error closing snapshotter", "name", snapCfg.Name(), "error", err.Error())
		}
	}()

	// Serialize batch result summary
	resultJSON, err := json.MarshalIndent(result, "", "  ")
	if err != nil {
		l.Error("SnapshotActivity - error marshalling batch results", "error", err.Error())
		return temporal.NewApplicationErrorWithCause(err.Error(), err.Error(), err)
	}

	// Build snapshot key
	key := fmt.Sprintf("%s-%d", result.JobID, result.StartAt)
	if isError {
		key = fmt.Sprintf("%s-error-%d", result.JobID, result.StartAt)
	}

	// Save batch result summary snapshot
	err = sk.Snapshot(ctx, key, resultJSON)
	if err != nil {
		l.Error("SnapshotActivity - error saving snapshot", "error", err.Error())
		return temporal.NewApplicationErrorWithCause(err.Error(), err.Error(), err)
	}

	return nil
}
