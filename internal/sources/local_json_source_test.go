package sources_test

import (
	"context"
	"testing"
	"time"

	"github.com/comfforts/logger"
	"github.com/hankgalt/batch-orchestra/internal/sources"
	"github.com/hankgalt/batch-orchestra/pkg/domain"
	"github.com/stretchr/testify/require"
)

func Test_LocalJSONConfig_BuildSource(t *testing.T) {
	l := logger.GetSlogLogger()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	ctx = logger.WithLogger(ctx, l)

	ljsoncfg := sources.LocalJSONConfig{
		Path:    "../../data/scheduler",
		FileKey: "dummy-job-multiple-key",
	}

	// Build the source
	source, err := ljsoncfg.BuildSource(ctx)
	require.NoError(t, err, "error building local json source")
	defer func() {
		require.NoError(t, source.Close(ctx), "error closing local json source")
	}()

	u := domain.JSONOffset{WithId: domain.WithId{Id: "0"}, Value: "0"}
	co := domain.CustomOffset[domain.HasId]{Val: u}

	bp, err := source.Next(ctx, co, 1)
	require.NoError(t, err, "error getting next batch from local json source")
	l.Debug("fetched batch from local json source", "batch", bp)

	for !bp.Done {
		bp, err = source.Next(ctx, bp.NextOffset, 1)
		require.NoError(t, err, "error getting next batch from local json source")
		l.Debug("fetched batch from local json source", "batch", bp)
	}

	l.Debug("all batches processed from local json source", "last-batch", bp)
}
