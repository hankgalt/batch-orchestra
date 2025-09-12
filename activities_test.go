package batch_orchestra_test

import (
	"container/list"
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/converter"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/worker"

	"github.com/comfforts/logger"

	bo "github.com/hankgalt/batch-orchestra"
	sqllite "github.com/hankgalt/batch-orchestra/internal/clients/sql_lite"
	"github.com/hankgalt/batch-orchestra/internal/sinks"
	"github.com/hankgalt/batch-orchestra/internal/snapshotters"
	"github.com/hankgalt/batch-orchestra/internal/sources"
	"github.com/hankgalt/batch-orchestra/pkg/domain"
	"github.com/hankgalt/batch-orchestra/pkg/utils"
)

// Source/Sink tied activity aliases for registration and lookup. These aliases
// are needed to identify the activities for generic implementation.
const (
	FetchNextLocalCSVSourceBatchActivityAlias string = "fetch-next-" + sources.LocalCSVSource + "-batch-activity-alias"
	FetchNextCloudCSVSourceBatchActivityAlias string = "fetch-next-" + sources.CloudCSVSource + "-batch-activity-alias"
	WriteNextNoopSinkBatchActivityAlias       string = "write-next-" + sinks.NoopSink + "-batch-activity-alias"
	WriteNextSQLLiteSinkBatchActivityAlias    string = "write-next-" + sinks.SQLLiteSink + "-batch-activity-alias"
	SnapshotLocalFileBatchActivityAlias       string = "snapshot-" + snapshotters.LocalFileSnapshotter + "-batch-activity-alias"
)

type ETLRequest[T any] struct {
	MaxBatches uint
	BatchSize  uint
	Done       bool
	Offsets    []uint64
	Batches    map[string]*domain.BatchProcess
}

func Test_FetchNext_LocalTempCSV(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestActivityEnvironment()

	l := logger.GetSlogLogger()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	ctx = logger.WithLogger(ctx, l)

	env.SetWorkerOptions(worker.Options{
		BackgroundActivityContext: ctx,
	})

	// Register the generic activity instantiation to call.
	env.RegisterActivityWithOptions(
		bo.FetchNextActivity[domain.CSVRow, sources.LocalCSVConfig],
		activity.RegisterOptions{
			Name: FetchNextLocalCSVSourceBatchActivityAlias,
		},
	)

	// Build a small CSV
	path := writeTempCSV(t,
		[]string{"id", "name"},
		[][]string{
			{"1", "alpha"},
			{"2", "beta"},
			{"3", "gamma"},
			{"4", "delta"},
			{"5", "epsilon"},
		},
	)
	defer func() {
		require.NoError(t, os.Remove(path), "cleanup temp CSV file")
	}()

	// Create a local CSV config
	cfg := sources.LocalCSVConfig{
		Path:      path,
		HasHeader: true,
	}

	// initialize batch size & next offset
	// Use a batch size larger than the largest row size in bytes to ensure more than one row is fetched.
	batchSize := uint(30)
	nextOffset := uint64(0)

	fIn := &domain.FetchInput[domain.CSVRow, sources.LocalCSVConfig]{
		Source:    cfg,
		Offset:    nextOffset,
		BatchSize: batchSize,
	}
	val, err := env.ExecuteActivity(FetchNextLocalCSVSourceBatchActivityAlias, fIn)
	require.NoError(t, err)

	var out domain.FetchOutput[domain.CSVRow]
	require.NoError(t, val.Get(&out))
	require.False(t, out.Batch.Done)
	require.Len(t, out.Batch.Records, 2)
	require.EqualValues(t, 23, out.Batch.NextOffset)
	require.Equal(t, domain.CSVRow{"id": "1", "name": "alpha"}, out.Batch.Records[0].Data)
	require.Equal(t, domain.CSVRow{"id": "2", "name": "beta"}, out.Batch.Records[1].Data)

	// Fetch the next batch of records till done
	for out.Batch.Done == false {
		fIn = &domain.FetchInput[domain.CSVRow, sources.LocalCSVConfig]{
			Source:    cfg,
			Offset:    out.Batch.NextOffset,
			BatchSize: batchSize,
		}
		val, err := env.ExecuteActivity(FetchNextLocalCSVSourceBatchActivityAlias, fIn)
		require.NoError(t, err)
		require.NoError(t, val.Get(&out))
		require.Equal(t, true, out.Batch.NextOffset > 0, "next offset should be greater than 0")
	}

	require.True(t, out.Batch.Done)
	require.EqualValues(t, 49, out.Batch.NextOffset)
}

// Add local csv file to data directory, before running this test
func Test_FetchNext_LocalCSV(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestActivityEnvironment()

	l := logger.GetSlogLogger()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	ctx = logger.WithLogger(ctx, l)

	env.SetWorkerOptions(worker.Options{
		BackgroundActivityContext: ctx,
	})

	// Register the generic activity instantiation we will call.
	// In test env, this is optional for function activities, but explicit is fine.
	env.RegisterActivityWithOptions(
		bo.FetchNextActivity[domain.CSVRow, sources.LocalCSVConfig],
		activity.RegisterOptions{
			Name: FetchNextLocalCSVSourceBatchActivityAlias,
		},
	)

	fileName := utils.BuildFileName()
	filePath, err := utils.BuildFilePath()
	require.NoError(t, err, "error building file path for test CSV")

	path := filepath.Join(filePath, fileName)

	cfg := sources.LocalCSVConfig{
		Path:         path,
		Delimiter:    '|',
		HasHeader:    true,
		MappingRules: domain.BuildBusinessModelTransformRules(),
	}

	batchSize := uint(400) // larger than the largest row size in bytes
	nextOffset := uint64(0)

	fIn := &domain.FetchInput[domain.CSVRow, sources.LocalCSVConfig]{
		Source:    cfg,
		Offset:    nextOffset,
		BatchSize: batchSize,
	}
	val, err := env.ExecuteActivity(FetchNextLocalCSVSourceBatchActivityAlias, fIn)
	require.NoError(t, err)

	var out domain.FetchOutput[domain.CSVRow]
	require.NoError(t, val.Get(&out))
	require.Equal(t, true, out.Batch.NextOffset > 0, "next offset should be greater than 0")

	for out.Batch.Done == false {
		fIn = &domain.FetchInput[domain.CSVRow, sources.LocalCSVConfig]{
			Source:    cfg,
			Offset:    out.Batch.NextOffset,
			BatchSize: batchSize,
		}
		val, err = env.ExecuteActivity(FetchNextLocalCSVSourceBatchActivityAlias, fIn)
		require.NoError(t, err)

		require.NoError(t, val.Get(&out))
		require.Equal(t, true, out.Batch.NextOffset > 0, "next offset should be greater than 0")
	}
}

// setup cloud credentials in test environment before running this test
func Test_FetchNext_CloudCSV(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestActivityEnvironment()

	l := logger.GetSlogLogger()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	ctx = logger.WithLogger(ctx, l)

	env.SetWorkerOptions(worker.Options{
		BackgroundActivityContext: ctx,
	})

	// Register activity
	env.RegisterActivityWithOptions(
		bo.FetchNextActivity[domain.CSVRow, sources.CloudCSVConfig],
		activity.RegisterOptions{
			Name: FetchNextCloudCSVSourceBatchActivityAlias,
		},
	)

	envCfg, err := utils.BuildCloudFileConfig()
	require.NoError(t, err, "error building cloud CSV config for test environment")

	path := filepath.Join(envCfg.Path, envCfg.Name)

	cfg := sources.CloudCSVConfig{
		Path:         path,
		Bucket:       envCfg.Bucket,
		Provider:     string(sources.CloudSourceGCS),
		Delimiter:    '|',
		HasHeader:    true,
		MappingRules: domain.BuildBusinessModelTransformRules(),
	}

	batchSize := uint(400) // larger than the largest row size in bytes
	nextOffset := uint64(0)

	fIn := &domain.FetchInput[domain.CSVRow, sources.CloudCSVConfig]{
		Source:    cfg,
		Offset:    nextOffset,
		BatchSize: batchSize,
	}
	val, err := env.ExecuteActivity(FetchNextCloudCSVSourceBatchActivityAlias, fIn)
	require.NoError(t, err)

	var out domain.FetchOutput[domain.CSVRow]
	require.NoError(t, val.Get(&out))
	require.Equal(t, true, out.Batch.NextOffset > 0, "next offset should be greater than 0")

	for out.Batch.Done == false {
		fIn = &domain.FetchInput[domain.CSVRow, sources.CloudCSVConfig]{
			Source:    cfg,
			Offset:    out.Batch.NextOffset,
			BatchSize: batchSize,
		}
		val, err := env.ExecuteActivity(FetchNextCloudCSVSourceBatchActivityAlias, fIn)
		require.NoError(t, err)

		require.NoError(t, val.Get(&out))
		require.Equal(t, true, out.Batch.NextOffset > 0, "next offset should be greater than 0")
	}
}

func Test_Write_NoopSink(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestActivityEnvironment()

	l := logger.GetSlogLogger()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	ctx = logger.WithLogger(ctx, l)

	env.SetWorkerOptions(worker.Options{
		BackgroundActivityContext: ctx,
	})

	env.RegisterActivityWithOptions(
		bo.WriteActivity[domain.CSVRow, sinks.NoopSinkConfig[domain.CSVRow]],
		activity.RegisterOptions{
			Name: WriteNextNoopSinkBatchActivityAlias,
		},
	)

	recs := []*domain.BatchRecord{
		{
			Start: 0,
			End:   10,
			Data:  domain.CSVRow{"id": "1", "name": "alpha"},
		},
		{
			Start: 10,
			End:   20,
			Data:  domain.CSVRow{"id": "2", "name": "beta"},
		},
	}
	b := &domain.BatchProcess{
		Records:    recs,
		NextOffset: 30,
		Done:       false,
	}

	in := &domain.WriteInput[domain.CSVRow, sinks.NoopSinkConfig[domain.CSVRow]]{
		Sink:  sinks.NoopSinkConfig[domain.CSVRow]{},
		Batch: b,
	}

	val, err := env.ExecuteActivity(WriteNextNoopSinkBatchActivityAlias, in)
	require.NoError(t, err)

	var out domain.WriteOutput[domain.CSVRow]
	require.NoError(t, val.Get(&out))
	require.Equal(t, 2, len(out.Batch.Records))
}

func Test_Write_SQLLiteSink(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestActivityEnvironment()

	l := logger.GetSlogLogger()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	ctx = logger.WithLogger(ctx, l)

	env.SetWorkerOptions(worker.Options{
		BackgroundActivityContext: ctx,
		DeadlockDetectionTimeout:  24 * time.Hour, // set a long timeout to avoid deadlock detection during tests
	})

	env.SetTestTimeout(24 * time.Hour)

	env.RegisterActivityWithOptions(
		bo.WriteActivity[domain.CSVRow, sinks.SQLLiteSinkConfig[domain.CSVRow]],
		activity.RegisterOptions{
			Name: WriteNextSQLLiteSinkBatchActivityAlias,
		},
	)

	dbFile := "data/__deleteme.db"
	dbClient, err := sqllite.NewSQLLiteDBClient(dbFile)
	require.NoError(t, err)

	res := dbClient.ExecuteSchema(sqllite.AgentSchema)
	n, err := res.LastInsertId()
	require.NoError(t, err)
	require.Equal(t, int64(0), n)

	cfg := sinks.SQLLiteSinkConfig[domain.CSVRow]{
		DBFile: dbFile,
		Table:  "agent",
	}

	recs := []*domain.BatchRecord{
		{
			Start: 0,
			End:   10,
			Data: domain.CSVRow{
				"entity_id":   "1",
				"entity_name": "Entity 1",
				"address":     "Address 1",
				"agent_type":  "sales",
				"name":        "alpha",
			},
		},
		{
			Start: 10,
			End:   20,
			Data: domain.CSVRow{
				"entity_id":   "2",
				"entity_name": "Entity 2",
				"address":     "Address 2",
				"agent_type":  "support",
				"name":        "beta",
			},
		},
	}

	b := &domain.BatchProcess{
		Records:    recs,
		NextOffset: 30,
		Done:       false,
	}

	in := &domain.WriteInput[domain.CSVRow, sinks.SQLLiteSinkConfig[domain.CSVRow]]{
		Sink:  cfg,
		Batch: b,
	}

	val, err := env.ExecuteActivity(WriteNextSQLLiteSinkBatchActivityAlias, in)
	require.NoError(t, err)

	var out domain.WriteOutput[domain.CSVRow]
	require.NoError(t, val.Get(&out))
	require.Equal(t, 2, len(out.Batch.Records))

	ags, err := dbClient.FetchRecords(context.Background(), "agent", 0, 10)
	require.NoError(t, err)
	require.Equal(t, len(ags), 2)

	err = os.Remove(dbFile)
	require.NoError(t, err)
}

func Test_FetchAndWrite_LocalCSVSource_SQLLiteSink_Queue(t *testing.T) {
	// setup test environment
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestActivityEnvironment()

	// setup logger & global activity execution context
	l := logger.GetSlogLogger()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	ctx = logger.WithLogger(ctx, l)

	env.SetWorkerOptions(worker.Options{
		BackgroundActivityContext: ctx,
		DeadlockDetectionTimeout:  24 * time.Hour, // set a long timeout to avoid deadlock detection during tests
	})

	env.SetTestTimeout(24 * time.Hour)

	// Register activities.
	env.RegisterActivityWithOptions(
		bo.FetchNextActivity[domain.CSVRow, sources.LocalCSVConfig],
		activity.RegisterOptions{
			Name: FetchNextLocalCSVSourceBatchActivityAlias,
		},
	)
	env.RegisterActivityWithOptions(
		bo.WriteActivity[domain.CSVRow, sinks.SQLLiteSinkConfig[domain.CSVRow]],
		activity.RegisterOptions{
			Name: WriteNextSQLLiteSinkBatchActivityAlias,
		},
	)

	// Build source & sink configurations
	// Source - local CSV
	fileName := utils.BuildFileName()
	filePath, err := utils.BuildFilePath()
	require.NoError(t, err, "error building file path for test CSV")

	path := filepath.Join(filePath, fileName)

	sourceCfg := sources.LocalCSVConfig{
		Path:         path,
		Delimiter:    '|',
		HasHeader:    true,
		MappingRules: domain.BuildBusinessModelTransformRules(),
	}

	// Sink - SQL-Lite
	dbFile := "data/__deleteme.db"
	dbClient, err := sqllite.NewSQLLiteDBClient(dbFile)
	require.NoError(t, err)

	defer func(cl *sqllite.SQLLiteDBClient, fpath string) {
		ags, err := cl.FetchRecords(context.Background(), "agent", 0, 25)
		require.NoError(t, err)
		require.Equal(t, len(ags), 20)

		require.NoError(t, cl.Close(ctx), "close db client")
		require.NoError(t, os.Remove(fpath), "cleanup temp db file")
	}(dbClient, dbFile)

	res := dbClient.ExecuteSchema(sqllite.AgentSchema)
	n, err := res.LastInsertId()
	require.NoError(t, err)
	require.Equal(t, int64(0), n)

	sinkCfg := sinks.SQLLiteSinkConfig[domain.CSVRow]{
		DBFile: dbFile,
		Table:  "agent",
	}

	// Build ETL request
	// batchSize should be larger than the largest row size in bytes
	etlReq := &ETLRequest[domain.CSVRow]{
		MaxBatches: 2,
		BatchSize:  400,
		Done:       false,
		Offsets:    []uint64{},
		Batches:    map[string]*domain.BatchProcess{},
	}

	etlReq.Offsets = append(etlReq.Offsets, uint64(0))

	// initiate a new queue
	q := list.New()

	// setup first batch request
	fIn := &domain.FetchInput[domain.CSVRow, sources.LocalCSVConfig]{
		Source:    sourceCfg,
		Offset:    etlReq.Offsets[len(etlReq.Offsets)-1],
		BatchSize: etlReq.BatchSize,
	}

	// Execute the fetch activity for first batch
	fVal, err := env.ExecuteActivity(FetchNextLocalCSVSourceBatchActivityAlias, fIn)
	require.NoError(t, err)

	var fOut domain.FetchOutput[domain.CSVRow]
	require.NoError(t, fVal.Get(&fOut))
	require.Equal(t, true, len(fOut.Batch.Records) > 0)

	// Store the fetched batch in ETL request
	etlReq.Offsets = append(etlReq.Offsets, fOut.Batch.NextOffset)
	etlReq.Done = fOut.Batch.Done

	// setup first write request
	wIn := &domain.WriteInput[domain.CSVRow, sinks.SQLLiteSinkConfig[domain.CSVRow]]{
		Sink:  sinkCfg,
		Batch: fOut.Batch,
	}

	// Execute async the write activity for first batch
	wVal, err := env.ExecuteActivity(WriteNextSQLLiteSinkBatchActivityAlias, wIn)
	require.NoError(t, err)

	// Push the write activity future to the queue
	q.PushBack(wVal)

	// while there are items in queue
	count := 0
	for q.Len() > 0 {
		// if queue has less than max batches and batches are not done
		if q.Len() < int(etlReq.MaxBatches) && !etlReq.Done {
			fIn = &domain.FetchInput[domain.CSVRow, sources.LocalCSVConfig]{
				Source:    sourceCfg,
				Offset:    etlReq.Offsets[len(etlReq.Offsets)-1],
				BatchSize: etlReq.BatchSize,
			}

			fVal, err := env.ExecuteActivity(FetchNextLocalCSVSourceBatchActivityAlias, fIn)
			require.NoError(t, err)

			var fOut domain.FetchOutput[domain.CSVRow]
			require.NoError(t, fVal.Get(&fOut))
			require.Equal(t, true, len(fOut.Batch.Records) > 0)

			etlReq.Done = fOut.Batch.Done
			etlReq.Offsets = append(etlReq.Offsets, fOut.Batch.NextOffset)
			etlReq.Batches[fmt.Sprintf("batch-%d-%d", fOut.Batch.StartOffset, fOut.Batch.NextOffset)] = fOut.Batch

			wIn = &domain.WriteInput[domain.CSVRow, sinks.SQLLiteSinkConfig[domain.CSVRow]]{
				Sink:  sinkCfg,
				Batch: fOut.Batch,
			}

			wVal, err := env.ExecuteActivity(WriteNextSQLLiteSinkBatchActivityAlias, wIn)
			require.NoError(t, err)

			q.PushBack(wVal)
		} else {
			if count < int(etlReq.MaxBatches) {
				count++
			} else {
				count = 0
				// Pause execution for 1 second
				time.Sleep(1 * time.Second)
			}
			wVal := q.Remove(q.Front()).(converter.EncodedValue)
			var wOut domain.WriteOutput[domain.CSVRow]
			require.NoError(t, wVal.Get(&wOut))
			require.Equal(t, true, len(wOut.Batch.Records) > 0)

			batchId := fmt.Sprintf("batch-%d-%d", wOut.Batch.StartOffset, wOut.Batch.NextOffset)
			if _, ok := etlReq.Batches[batchId]; !ok {
				etlReq.Batches[batchId] = wOut.Batch
			} else {
				etlReq.Batches[batchId] = wOut.Batch
			}
		}
	}

	l.Debug("ETL request done.", "offsets", etlReq.Offsets, "num-batches", len(etlReq.Batches))
	require.Equal(t, true, len(etlReq.Offsets) > 0)
}

func Test_FetchAndWrite_Temp_LocalCSVSource_SQLLiteSink_Queue(t *testing.T) {
	// setup test environment
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestActivityEnvironment()

	// setup logger & global activity execution context
	l := logger.GetSlogLogger()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	ctx = logger.WithLogger(ctx, l)

	env.SetWorkerOptions(worker.Options{
		BackgroundActivityContext: ctx,
		DeadlockDetectionTimeout:  24 * time.Hour, // set a long timeout to avoid deadlock detection during tests
	})

	env.SetTestTimeout(24 * time.Hour)

	// Register activities.
	env.RegisterActivityWithOptions(
		bo.FetchNextActivity[domain.CSVRow, sources.LocalCSVConfig],
		activity.RegisterOptions{
			Name: FetchNextLocalCSVSourceBatchActivityAlias,
		},
	)
	env.RegisterActivityWithOptions(
		bo.WriteActivity[domain.CSVRow, sinks.SQLLiteSinkConfig[domain.CSVRow]],
		activity.RegisterOptions{
			Name: WriteNextSQLLiteSinkBatchActivityAlias,
		},
	)

	// Build source & sink configurations
	// Source - local CSV
	// Build a small CSV
	path := writeTempCSV(t,
		[]string{"entity_id", "entity_name", "name", "address", "agent_type"},
		[][]string{
			{"A7SD456", "alpha", "John Doe", "Address 1", "sales"},
			{"A7SD457", "beta", "Jane Smith", "Address 2", "support"},
			{"A7SD458", "gamma", "Jim Brown", "Address 3", "marketing"},
			{"A7SD459", "delta", "Jack White", "Address 4", "sales"},
			{"A7SD460", "epsilon", "Jill Black", "Address 5", "support"},
			{"A7SD461", "zeta", "Jake Green", "Address 6", "marketing"},
			{"A7SD462", "eta", "Jasmine Blue", "Address 7", "support"},
			{"A7SD463", "theta", "Jordan Black", "Address 8", "sales"},
			{"A7SD464", "iota", "Jessica Pink", "Address 9", "support"},
			{"A7SD465", "kappa", "Jeremy Gray", "Address 10", "marketing"},
			{"A7SD466", "lambda", "Jasmine White", "Address 11", "support"},
			{"A7SD467", "mu", "Jordan White", "Address 12", "sales"},
			{"A7SD468", "nu", "Jasmine Green", "Address 13", "support"},
		},
	)
	defer func() {
		require.NoError(t, os.Remove(path), "cleanup temp CSV file")
	}()
	sourceCfg := sources.LocalCSVConfig{
		Path:         path,
		HasHeader:    true,
		MappingRules: domain.BuildBusinessModelTransformRules(),
	}

	// Sink - SQL-Lite
	dbFile := "data/__deleteme.db"
	dbClient, err := sqllite.NewSQLLiteDBClient(dbFile)
	require.NoError(t, err)

	defer func(cl *sqllite.SQLLiteDBClient, fpath string) {
		ags, err := cl.FetchRecords(context.Background(), "agent", 0, 15)
		require.NoError(t, err)
		require.Equal(t, len(ags), 13)

		require.NoError(t, cl.Close(ctx), "close db client")
		require.NoError(t, os.Remove(fpath), "cleanup temp db file")
	}(dbClient, dbFile)

	res := dbClient.ExecuteSchema(sqllite.AgentSchema)
	n, err := res.LastInsertId()
	require.NoError(t, err)
	require.Equal(t, int64(0), n)

	sinkCfg := sinks.SQLLiteSinkConfig[domain.CSVRow]{
		DBFile: dbFile,
		Table:  "agent",
	}

	// Build ETL request
	// batchSize should be larger than the largest row size in bytes
	etlReq := &ETLRequest[domain.CSVRow]{
		MaxBatches: 2,
		BatchSize:  200,
		Done:       false,
		Offsets:    []uint64{},
		Batches:    map[string]*domain.BatchProcess{},
	}

	etlReq.Offsets = append(etlReq.Offsets, uint64(0))

	// initiate a new queue
	q := list.New()

	// setup first batch request
	fIn := &domain.FetchInput[domain.CSVRow, sources.LocalCSVConfig]{
		Source:    sourceCfg,
		Offset:    etlReq.Offsets[len(etlReq.Offsets)-1],
		BatchSize: etlReq.BatchSize,
	}

	// Execute the fetch activity for first batch
	fVal, err := env.ExecuteActivity(FetchNextLocalCSVSourceBatchActivityAlias, fIn)
	require.NoError(t, err)

	var fOut domain.FetchOutput[domain.CSVRow]
	require.NoError(t, fVal.Get(&fOut))
	require.Equal(t, true, len(fOut.Batch.Records) > 0)

	// Store the fetched batch in ETL request
	etlReq.Offsets = append(etlReq.Offsets, fOut.Batch.NextOffset)
	etlReq.Done = fOut.Batch.Done

	// setup first write request
	wIn := &domain.WriteInput[domain.CSVRow, sinks.SQLLiteSinkConfig[domain.CSVRow]]{
		Sink:  sinkCfg,
		Batch: fOut.Batch,
	}

	// Execute async the write activity for first batch
	wVal, err := env.ExecuteActivity(WriteNextSQLLiteSinkBatchActivityAlias, wIn)
	require.NoError(t, err)

	// Push the write activity future to the queue
	q.PushBack(wVal)

	// while there are items in queue
	count := 0
	for q.Len() > 0 {
		// if queue has less than max batches and batches are not done
		if q.Len() < int(etlReq.MaxBatches) && !etlReq.Done {
			fIn = &domain.FetchInput[domain.CSVRow, sources.LocalCSVConfig]{
				Source:    sourceCfg,
				Offset:    etlReq.Offsets[len(etlReq.Offsets)-1],
				BatchSize: etlReq.BatchSize,
			}

			fVal, err := env.ExecuteActivity(FetchNextLocalCSVSourceBatchActivityAlias, fIn)
			require.NoError(t, err)

			var fOut domain.FetchOutput[domain.CSVRow]
			require.NoError(t, fVal.Get(&fOut))
			require.Equal(t, true, len(fOut.Batch.Records) > 0)

			etlReq.Done = fOut.Batch.Done
			etlReq.Offsets = append(etlReq.Offsets, fOut.Batch.NextOffset)
			etlReq.Batches[fmt.Sprintf("batch-%d-%d", fOut.Batch.StartOffset, fOut.Batch.NextOffset)] = fOut.Batch

			wIn = &domain.WriteInput[domain.CSVRow, sinks.SQLLiteSinkConfig[domain.CSVRow]]{
				Sink:  sinkCfg,
				Batch: fOut.Batch,
			}

			wVal, err := env.ExecuteActivity(WriteNextSQLLiteSinkBatchActivityAlias, wIn)
			require.NoError(t, err)

			q.PushBack(wVal)
		} else {
			if count < int(etlReq.MaxBatches) {
				count++
			} else {
				count = 0
				// Pause execution for 1 second
				time.Sleep(1 * time.Second)
			}
			wVal := q.Remove(q.Front()).(converter.EncodedValue)
			var wOut domain.WriteOutput[domain.CSVRow]
			require.NoError(t, wVal.Get(&wOut))
			require.Equal(t, true, len(wOut.Batch.Records) > 0)

			batchId := fmt.Sprintf("batch-%d-%d", wOut.Batch.StartOffset, wOut.Batch.NextOffset)
			if _, ok := etlReq.Batches[batchId]; !ok {
				etlReq.Batches[batchId] = wOut.Batch
			} else {
				etlReq.Batches[batchId] = wOut.Batch
			}
		}
	}

	l.Debug("ETL request done.", "offsets", etlReq.Offsets, "num-batches", len(etlReq.Batches))
	require.Equal(t, true, len(etlReq.Offsets) > 0)
}

func writeTempCSV(t *testing.T, header []string, rows [][]string) string {
	t.Helper()

	// get current dir path
	dir, err := os.Getwd()
	require.NoError(t, err)

	fp := filepath.Join(dir, "data", "data.csv")
	f, err := os.Create(fp)
	require.NoError(t, err)
	defer f.Close()

	write := func(cols []string) {
		for i, c := range cols {
			if i > 0 {
				_, _ = f.WriteString(",")
			}
			_, _ = f.WriteString(c)
		}
		_, _ = f.WriteString("\n")
	}

	if len(header) > 0 {
		write(header)
	}
	for _, r := range rows {
		write(r)
	}
	require.NoError(t, f.Sync())
	return fp
}
