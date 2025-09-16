package batch_orchestra

import (
	"context"
	"encoding/json"
	"fmt"
	"slices"

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
	l.Debug("FetchNextActivity started", "input-offset", in.Offset)

	src, err := in.Source.BuildSource(ctx)
	if err != nil {
		l.Error("error building source", "error", err.Error())
		return nil, temporal.NewApplicationErrorWithCause(err.Error(), err.Error(), err)
	}
	// Ensure the source is closed after use
	defer func() {
		if err := src.Close(ctx); err != nil {
			l.Error("error closing source", "error", err.Error())
		}
	}()

	// Fetch the next batch from the source
	b, err := src.Next(ctx, in.Offset, in.BatchSize)
	if err != nil {
		l.Error("error fetching next batch", "error", err.Error())
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
	l.Debug("WriteActivity started", "start-offset", in.Batch.StartOffset, "next-offset", in.Batch.NextOffset)

	sk, err := in.Sink.BuildSink(ctx)
	if err != nil {
		l.Error("error building sink", "error", err.Error())
		return nil, temporal.NewApplicationErrorWithCause(err.Error(), err.Error(), err)
	}
	defer func() {
		if err := sk.Close(ctx); err != nil {
			l.Error("error closing sink", "error", err.Error())
		}
	}()

	// Write the batch to the sink
	out, err := sk.Write(ctx, in.Batch)
	if err != nil {
		l.Error("error writing to sink", "error", err.Error())
		return nil, temporal.NewApplicationErrorWithCause(err.Error(), err.Error(), err)
	}

	// record activity heartbeat
	activity.RecordHeartbeat(ctx, in)

	return &domain.WriteOutput[T]{
		Batch: out,
	}, nil
}

func SnapshotActivity[SS domain.SnapshotConfig](
	ctx context.Context,
	result *domain.BatchProcessingResult,
	snapshot *domain.BatchSnapshot,
	snapCfg SS,
) (*domain.BatchSnapshot, error) {
	l := activity.GetLogger(ctx)
	l.Debug("SnapshotActivity started", "name", snapCfg.Name(), "job-id", result.JobID)

	if slices.Contains(snapshot.SnapshotIdx, result.StartAt) {
		l.Info("snapshot already exists for this offset, skipping", "offset", result.StartAt)
		return snapshot, nil
	}

	sk, err := snapCfg.BuildSnapshotter(ctx)
	if err != nil {
		l.Error("error building snapshotter", "name", snapCfg.Name(), "error", err.Error())
		return nil, temporal.NewApplicationErrorWithCause(err.Error(), err.Error(), err)
	}
	defer func() {
		if err := sk.Close(ctx); err != nil {
			l.Error("error closing snapshotter", "name", snapCfg.Name(), "error", err.Error())
		}
	}()

	// Build batch snapshot
	updatedSnapshot := buildRequestSnapshot(result.Batches, snapshot)
	updatedSnapshot.SnapshotIdx = append(updatedSnapshot.SnapshotIdx, result.StartAt)

	// Serialize batch result summary
	resultJSON, err := json.MarshalIndent(result, "", "  ")
	if err != nil {
		l.Error("error marshalling batch results", "error", err.Error())
		return snapshot, temporal.NewApplicationErrorWithCause(err.Error(), err.Error(), err)
	}

	// Save batch result summary snapshot
	key := fmt.Sprintf("%s-%d", result.JobID, result.StartAt)
	err = sk.Snapshot(ctx, key, resultJSON)
	if err != nil {
		l.Error("error saving snapshot", "error", err.Error())
		return snapshot, temporal.NewApplicationErrorWithCause(err.Error(), err.Error(), err)
	}

	// return batch snapshot
	return updatedSnapshot, nil
}

func buildRequestSnapshot(batches map[string]*domain.BatchProcess, snapshot *domain.BatchSnapshot) *domain.BatchSnapshot {
	errRecs := map[string][]domain.ErrorRecord{}
	numProcessed := uint(len(batches))
	numRecords := uint(0)
	pauseCount := uint(0)
	snapshotIdx := []uint64{}

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
