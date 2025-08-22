package batch_orchestra

import (
	"context"

	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/temporal"

	"github.com/hankgalt/batch-orchestra/pkg/domain"
	"github.com/hankgalt/batch-orchestra/pkg/sinks"
	"github.com/hankgalt/batch-orchestra/pkg/sources"
)

type ActivityAlias string

const (
	FetchNextLocalCSVSourceBatchAlias string = "fetch-next-" + sources.LocalCSVSource + "-batch-alias"
	FetchNextCloudCSVSourceBatchAlias string = "fetch-next-" + sources.CloudCSVSource + "-batch-alias"
	WriteNextNoopSinkBatchAlias       string = "write-next-" + sinks.NoopSink + "-batch-alias"
	WriteNextMongoSinkBatchAlias      string = "write-next-" + sinks.MongoSink + "-batch-alias"
)

// FetchNextActivity fetches the next batch of data from the source.
// It builds the source from the provided configuration, fetches the next batch,
// and records a heartbeat for the activity.
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
