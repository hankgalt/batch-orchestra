package batch_orchestra_test

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"go.temporal.io/api/enums/v1"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/converter"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"

	"github.com/comfforts/logger"

	bo "github.com/hankgalt/batch-orchestra"
	sqllite "github.com/hankgalt/batch-orchestra/internal/clients/sql_lite"
	"github.com/hankgalt/batch-orchestra/internal/sinks"
	"github.com/hankgalt/batch-orchestra/internal/snapshotters"
	"github.com/hankgalt/batch-orchestra/internal/sources"
	"github.com/hankgalt/batch-orchestra/pkg/domain"
	"github.com/hankgalt/batch-orchestra/pkg/utils"
)

const (
	ProcessLocalCSVSQLLiteWorkflowAlias string = "process-local-csv-sqlite-workflow-alias"
	ProcessCloudCSVSQLLiteWorkflowAlias string = "process-cloud-csv-sqlite-workflow-alias"
)

type LocalCSVNoopBatchRequest domain.BatchProcessingRequest[domain.CSVRow, sources.LocalCSVConfig, sinks.NoopSinkConfig[domain.CSVRow], snapshotters.LocalFileSnapshotterConfig]
type CloudCSVSQLLiteBatchRequest domain.BatchProcessingRequest[domain.CSVRow, sources.CloudCSVConfig, sinks.SQLLiteSinkConfig[domain.CSVRow], snapshotters.LocalFileSnapshotterConfig]
type LocalCSVSQLLiteBatchRequest domain.BatchProcessingRequest[domain.CSVRow, sources.LocalCSVConfig, sinks.SQLLiteSinkConfig[domain.CSVRow], snapshotters.LocalFileSnapshotterConfig]

type ProcessBatchWorkflowTestSuite struct {
	suite.Suite
	testsuite.WorkflowTestSuite

	env *testsuite.TestWorkflowEnvironment
}

func TestProcessBatchWorkflowTestSuite(t *testing.T) {
	suite.Run(t, new(ProcessBatchWorkflowTestSuite))
}

func (s *ProcessBatchWorkflowTestSuite) SetupTest() {
	// get test logger
	l := logger.GetSlogMultiLogger("data")

	// set environment logger
	s.SetLogger(l)
}

func (s *ProcessBatchWorkflowTestSuite) TearDownTest() {
	s.env.AssertExpectations(s.T())

	// err := os.RemoveAll(TEST_DIR)
	// s.NoError(err)

}

// Setup cloud test environment, before running this test.
func (s *ProcessBatchWorkflowTestSuite) Test_ProcessBatchWorkflow_CloudCSV_SQLLite_HappyPath() {
	l := s.GetLogger()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	ctx = logger.WithLogger(ctx, l)

	s.env = s.NewTestWorkflowEnvironment()
	s.env.SetWorkerOptions(worker.Options{
		BackgroundActivityContext: ctx,
		DeadlockDetectionTimeout:  24 * time.Hour, // set a long timeout to avoid deadlock detection during tests
	})

	s.env.SetTestTimeout(24 * time.Hour)

	s.Run("valid cloud csv to mongo migration request", func() {
		// Register batch process workflow for cloud csv to SQLLite with batch processing snapshots saved as local files.
		s.env.RegisterWorkflowWithOptions(
			bo.ProcessBatchWorkflow[domain.CSVRow, sources.CloudCSVConfig, sinks.SQLLiteSinkConfig[domain.CSVRow], snapshotters.LocalFileSnapshotterConfig],
			workflow.RegisterOptions{
				Name: ProcessCloudCSVSQLLiteWorkflowAlias,
			},
		)
		// Register fetch activity cloud csv source
		s.env.RegisterActivityWithOptions(
			bo.FetchNextActivity[domain.CSVRow, sources.CloudCSVConfig],
			activity.RegisterOptions{
				Name: FetchNextCloudCSVSourceBatchActivityAlias,
			},
		)
		// Register write activity for SQLLite sink
		s.env.RegisterActivityWithOptions(
			bo.WriteActivity[domain.CSVRow, sinks.SQLLiteSinkConfig[domain.CSVRow]],
			activity.RegisterOptions{
				Name: WriteNextSQLLiteSinkBatchActivityAlias,
			},
		)
		// Register snapshot activity for local file snapshotter
		s.env.RegisterActivityWithOptions(
			bo.SnapshotActivity[snapshotters.LocalFileSnapshotterConfig],
			activity.RegisterOptions{
				Name: SnapshotLocalFileBatchActivityAlias,
			},
		)

		// Build source & sink configurations
		// Source - cloud CSV
		envCfg, err := utils.BuildCloudFileConfig()
		s.NoError(err, "error building cloud CSV config for test environment")
		path := filepath.Join(envCfg.Path, envCfg.Name)
		sourceCfg := sources.CloudCSVConfig{
			Path:         path,
			Bucket:       envCfg.Bucket,
			Provider:     string(sources.CloudSourceGCS),
			Delimiter:    '|',
			HasHeader:    true,
			MappingRules: domain.BuildBusinessModelTransformRules(),
		}

		// Sink - SQLLite
		dbFile := "data/__deleteme.db"
		dbClient, err := sqllite.NewSQLLiteDBClient(dbFile)
		s.NoError(err)

		defer func(cl *sqllite.SQLLiteDBClient, fpath string) {
			ags, err := cl.FetchRecords(context.Background(), "agent", 0, 30)
			s.NoError(err)
			s.Equal(len(ags), 25)

			s.NoError(cl.Close(ctx), "close db client")
			s.NoError(os.Remove(fpath), "cleanup temp db file")
		}(dbClient, dbFile)

		res := dbClient.ExecuteSchema(sqllite.AgentSchema)
		n, err := res.LastInsertId()
		s.NoError(err)
		s.Equal(int64(0), n)

		sinkCfg := sinks.SQLLiteSinkConfig[domain.CSVRow]{
			DBFile: dbFile,
			Table:  "agent",
		}

		// Snapshotter - local file
		filePath, err := utils.BuildFilePath()
		s.NoError(err, "error building csv file path for test")
		ssCfg := snapshotters.LocalFileSnapshotterConfig{
			Path: filePath,
		}

		// Build the cloud csv to sqllite request
		req := &CloudCSVSQLLiteBatchRequest{
			JobID:               "job-cloud-csv-sqllite-happy",
			BatchSize:           400,
			MaxInProcessBatches: 2,
			StartAt:             0,
			Source:              sourceCfg,
			Sink:                sinkCfg,
			Snapshotter:         ssCfg,
		}

		s.env.SetOnActivityStartedListener(
			func(
				activityInfo *activity.Info,
				ctx context.Context,
				args converter.EncodedValues,
			) {
				activityType := activityInfo.ActivityType.Name
				if strings.HasPrefix(activityType, "internalSession") {
					return
				}

				l.Debug(
					"Test_ProcessBatchWorkflow_CloudCSV_SQLLite_HappyPath - Activity started",
					"activityType", activityType,
				)

			})

		defer func() {
			if err := recover(); err != nil {
				l.Error(
					"Test_ProcessBatchWorkflow_CloudCSV_SQLLite_HappyPath - panicked",
					"workflow", ProcessCloudCSVSQLLiteWorkflowAlias, "error", err,
				)
			}

			err := s.env.GetWorkflowError()
			if err != nil {
				l.Error(
					"Test_ProcessBatchWorkflow_CloudCSV_SQLLite_HappyPath - error",
					"workflow", ProcessCloudCSVSQLLiteWorkflowAlias, "error", err,
				)
			} else {
				var result CloudCSVSQLLiteBatchRequest
				err := s.env.GetWorkflowResult(&result)
				if err != nil {
					l.Error(
						"Test_ProcessBatchWorkflow_CloudCSV_SQLLite_HappyPath - error",
						"workflow", ProcessCloudCSVSQLLiteWorkflowAlias, "error", err,
					)
				} else {
					l.Debug(
						"Test_ProcessBatchWorkflow_CloudCSV_SQLLite_HappyPath - success",
						"num-batches-processed", result.Snapshot.NumProcessed,
						"num-records-processed", result.Snapshot.NumRecords,
					)

					s.EqualValues(10, uint(result.Snapshot.NumProcessed))
					errorCount := 0
					for _, errs := range result.Snapshot.Errors {
						errorCount += len(errs)
					}
					s.EqualValues(25, uint(result.Snapshot.NumRecords))
					s.EqualValues(0, errorCount)
				}
			}

		}()

		l.Debug("Test_ProcessBatchWorkflow_CloudCSV_SQLLite_HappyPath - Starting workflow test")
		s.env.ExecuteWorkflow(ProcessCloudCSVSQLLiteWorkflowAlias, req)
		s.True(s.env.IsWorkflowCompleted())
		l.Debug("Test_ProcessBatchWorkflow_CloudCSV_SQLLite_HappyPath - test completed")
	})
}

// Setup local csv file before running this test.
func Test_ProcessBatchWorkflow_LocalCSV_SQLLite_ContinueAsNewError(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()

	l := logger.GetSlogLogger()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	ctx = logger.WithLogger(ctx, l)

	env.SetWorkerOptions(worker.Options{
		BackgroundActivityContext: ctx,
		DeadlockDetectionTimeout:  24 * time.Hour, // set a long timeout to avoid deadlock detection during tests
	})

	env.SetTestTimeout(24 * time.Hour)

	// Register the workflow
	env.RegisterWorkflowWithOptions(
		bo.ProcessBatchWorkflow[domain.CSVRow, sources.LocalCSVConfig, sinks.SQLLiteSinkConfig[domain.CSVRow], snapshotters.LocalFileSnapshotterConfig],
		workflow.RegisterOptions{
			Name: ProcessLocalCSVSQLLiteWorkflowAlias,
		},
	)

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
	env.RegisterActivityWithOptions(
		bo.SnapshotActivity[snapshotters.LocalFileSnapshotterConfig],
		activity.RegisterOptions{
			Name: SnapshotLocalFileBatchActivityAlias,
		},
	)

	// Build source & sink configurations
	// Source - local CSV
	fileName := utils.BuildFileName()
	filePath, err := utils.BuildFilePath()
	require.NoError(t, err, "error building csv file path for test")
	path := filepath.Join(filePath, fileName)
	sourceCfg := sources.LocalCSVConfig{
		Path:         path,
		Delimiter:    '|',
		HasHeader:    true,
		MappingRules: domain.BuildBusinessModelTransformRules(),
	}

	// Sink - MongoDB
	dbFile := "data/__deleteme.db"
	dbClient, err := sqllite.NewSQLLiteDBClient(dbFile)
	require.NoError(t, err)

	defer func(cl *sqllite.SQLLiteDBClient, fpath string) {
		ags, err := cl.FetchRecords(context.Background(), "agent", 0, 15)
		require.NoError(t, err)
		require.Equal(t, len(ags), 11)

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

	ssCfg := snapshotters.LocalFileSnapshotterConfig{
		Path: filePath,
	}

	req := &LocalCSVSQLLiteBatchRequest{
		JobID:               "job-local-csv-sqllite-continue-as-new-error-happy",
		BatchSize:           400,
		MaxInProcessBatches: 2,
		MaxBatches:          6,
		StartAt:             0,
		Source:              sourceCfg,
		Sink:                sinkCfg,
		Snapshotter:         ssCfg,
	}

	env.SetOnActivityStartedListener(
		func(
			activityInfo *activity.Info,
			ctx context.Context,
			args converter.EncodedValues,
		) {
			activityType := activityInfo.ActivityType.Name
			if strings.HasPrefix(activityType, "internalSession") {
				return
			}

			l.Debug(
				"Test_ProcessBatchWorkflow_LocalCSV_SQLLite_ContinueAsNewError - Activity started",
				"activity-type", activityType,
			)

		})

	defer func() {
		if err := recover(); err != nil {
			l.Error(
				"Test_ProcessBatchWorkflow_LocalCSV_SQLLite_ContinueAsNewError - panicked",
				"workflow", ProcessLocalCSVSQLLiteWorkflowAlias,
				"error", err,
			)
		}

		err := env.GetWorkflowError()
		var ca *workflow.ContinueAsNewError
		require.True(t, errors.As(err, &ca), "expected ContinueAsNewError, got: %v", err)
		require.Equal(t, ProcessLocalCSVSQLLiteWorkflowAlias, ca.WorkflowType.Name, "expected workflow type to match")

		var next LocalCSVSQLLiteBatchRequest
		ok, decErr := extractContinueAsNewInput(err, converter.GetDefaultDataConverter(), &next)
		require.True(t, ok, "expected to extract continue-as-new input, got error: %v", decErr)
		require.NoError(t, decErr, "error extracting continue-as-new input")
		require.NotNil(t, &next, "expected non-nil continue-as-new input")
		require.True(t, next.StartAt > req.StartAt, "expected next.StartAt > req.StartAt")
	}()

	env.ExecuteWorkflow(ProcessLocalCSVSQLLiteWorkflowAlias, req)
	require.True(t, env.IsWorkflowCompleted())

}

// Integration test for ProcessBatchWorkflow on a dev Temporal server.
// Start temporal dev server, setup local csv file &
// setup temporal test environment, before running this test.
func Test_ProcessBatchWorkflow_LocalCSV_SQLLite_HappyPath_Server(t *testing.T) {
	// get test logger
	l := logger.GetSlogLogger()

	// setup context with logger
	ctx := logger.WithLogger(context.Background(), l)

	// Temporal client connection
	c, err := client.Dial(
		client.Options{
			HostPort: client.DefaultHostPort,
			Logger:   l,
		},
	)
	require.NoError(t, err, "failed to connect to temporal server")
	defer c.Close()

	// workflow task queue
	const tq = "process-batch-workflow-local-csv-sqllite-tq"

	// Temporal worker for the task queue
	w := worker.New(c, tq, worker.Options{
		BackgroundActivityContext: ctx,            // Global worker context for activities
		DeadlockDetectionTimeout:  24 * time.Hour, // set a long timeout to avoid deadlock detection during tests
	})

	// Register the workflow function
	w.RegisterWorkflowWithOptions(
		bo.ProcessBatchWorkflow[domain.CSVRow, sources.LocalCSVConfig, sinks.SQLLiteSinkConfig[domain.CSVRow], snapshotters.LocalFileSnapshotterConfig],
		workflow.RegisterOptions{
			Name: ProcessLocalCSVSQLLiteWorkflowAlias,
		},
	)

	// Register activities.
	w.RegisterActivityWithOptions(
		bo.FetchNextActivity[domain.CSVRow, sources.LocalCSVConfig],
		activity.RegisterOptions{
			Name: FetchNextLocalCSVSourceBatchActivityAlias,
		},
	)
	w.RegisterActivityWithOptions(
		bo.WriteActivity[domain.CSVRow, sinks.SQLLiteSinkConfig[domain.CSVRow]],
		activity.RegisterOptions{
			Name: WriteNextSQLLiteSinkBatchActivityAlias,
		},
	)
	w.RegisterActivityWithOptions(
		bo.SnapshotActivity[snapshotters.LocalFileSnapshotterConfig],
		activity.RegisterOptions{
			Name: SnapshotLocalFileBatchActivityAlias,
		},
	)

	// Start the worker in a goroutine
	go func() {
		// Stop worker when test ends.
		if err := w.Run(worker.InterruptCh()); err != nil {
			l.Error("Test_ProcessBatchWorkflow_LocalCSV_SQLLite_HappyPath_Server - worker stopped:", "error", err.Error())
		}
	}()

	// Build source & sink configurations
	// Source - local CSV
	fileName := utils.BuildFileName()
	filePath, err := utils.BuildFilePath()
	require.NoError(t, err, "error building csv file path for test")
	path := filepath.Join(filePath, fileName)
	sourceCfg := sources.LocalCSVConfig{
		Path:         path,
		Delimiter:    '|',
		HasHeader:    true,
		MappingRules: domain.BuildBusinessModelTransformRules(),
	}

	// Sink - SQLLite
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

	ssCfg := snapshotters.LocalFileSnapshotterConfig{
		Path: filePath,
	}

	req := &LocalCSVSQLLiteBatchRequest{
		JobID:               "process-batch-workflow-local-csv-sqllite-server-happy",
		BatchSize:           400,
		MaxInProcessBatches: 3,
		MaxBatches:          3,
		StartAt:             0,
		Source:              sourceCfg,
		Sink:                sinkCfg,
		Snapshotter:         ssCfg,
		Policies: map[string]domain.RetryPolicySpec{
			domain.GetFetchActivityName(sourceCfg): {
				MaximumAttempts:    3,
				InitialInterval:    100 * time.Millisecond,
				BackoffCoefficient: 2.0,
				MaximumInterval:    5 * time.Second,
				NonRetryableErrorTypes: []string{
					sources.ErrMsgLocalCSVPathRequired,
					sources.ErrMsgLocalCSVSizeMustBePositive,
					sources.ErrMsgLocalCSVFileNotFound,
					sources.ErrMsgLocalCSVTransformerNil,
				},
			},
			domain.GetWriteActivityName(sinkCfg): {
				MaximumAttempts:    5,
				InitialInterval:    200 * time.Millisecond,
				BackoffCoefficient: 1.5,
				MaximumInterval:    10 * time.Second,
				NonRetryableErrorTypes: []string{
					sinks.ErrMsgSQLLiteSinkNil,
					sinks.ErrMsgSQLLiteSinkNilClient,
					sinks.ErrMsgSQLLiteSinkEmptyTable,
					sinks.ErrMsgSQLLiteSinkDBFileRequired,
				},
			},
		},
	}

	// Create workflow execution context
	ctx, cancel := context.WithCancel(ctx)
	t.Cleanup(cancel)

	// Kick off the workflow.
	run, err := c.ExecuteWorkflow(
		ctx, // context
		client.StartWorkflowOptions{ // workflow options
			ID:        "pbwlcmsh-" + time.Now().Format("150405.000"),
			TaskQueue: tq,
		},
		ProcessLocalCSVSQLLiteWorkflowAlias, // workflow function
		req,                                 // workflow input arg1
	)
	require.NoError(t, err)

	// With a real server, Continue-As-New chains transparently; Get waits for the final run.
	var result LocalCSVSQLLiteBatchRequest
	require.NoError(t, run.Get(ctx, &result))
	require.NotNil(t, result)
	require.True(t, result.Done, "workflow should be marked as done")

	require.EqualValues(t, 11, int(result.Snapshot.NumProcessed))
	errorCount := 0
	for _, errs := range result.Snapshot.Errors {
		errorCount += len(errs)
	}
	require.EqualValues(t, 20, int(result.Snapshot.NumRecords))
	require.EqualValues(t, 0, errorCount)

	l.Debug("Test_ProcessBatchWorkflow_LocalCSV_SQLLite_HappyPath_Server - workflow completed successfully",
		"workflow-id", run.GetID(),
		"workflow-run-id", run.GetRunID(),
		"num-batches-processed", result.Snapshot.NumProcessed,
		"num-records-processed", result.Snapshot.NumRecords,
	)
}

func Test_ProcessBatchWorkflow_Temp_LocalCSV_SQLLite_HappyPath(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()

	l := logger.GetSlogLogger()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	ctx = logger.WithLogger(ctx, l)

	env.SetWorkerOptions(worker.Options{
		BackgroundActivityContext: ctx,
		DeadlockDetectionTimeout:  24 * time.Hour, // set a long timeout to avoid deadlock detection during tests
	})

	env.SetTestTimeout(24 * time.Hour)

	// Register the workflow
	env.RegisterWorkflowWithOptions(
		bo.ProcessBatchWorkflow[domain.CSVRow, sources.LocalCSVConfig, sinks.SQLLiteSinkConfig[domain.CSVRow], snapshotters.LocalFileSnapshotterConfig],
		workflow.RegisterOptions{
			Name: ProcessLocalCSVSQLLiteWorkflowAlias,
		},
	)

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
	env.RegisterActivityWithOptions(
		bo.SnapshotActivity[snapshotters.LocalFileSnapshotterConfig],
		activity.RegisterOptions{
			Name: SnapshotLocalFileBatchActivityAlias,
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

	// Sink - MongoDB
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

	filePath, err := utils.BuildFilePath()
	require.NoError(t, err, "error building csv file path for test")
	ssCfg := snapshotters.LocalFileSnapshotterConfig{
		Path: filePath,
	}

	req := &LocalCSVSQLLiteBatchRequest{
		JobID:               "job-temp-local-csv-sqllite-happy",
		BatchSize:           200,
		MaxInProcessBatches: 2,
		StartAt:             0,
		Source:              sourceCfg,
		Sink:                sinkCfg,
		Snapshotter:         ssCfg,
		Policies: map[string]domain.RetryPolicySpec{
			domain.GetFetchActivityName(sourceCfg): {
				MaximumAttempts:    3,
				InitialInterval:    100 * time.Millisecond,
				BackoffCoefficient: 2.0,
				MaximumInterval:    5 * time.Second,
				NonRetryableErrorTypes: []string{
					sources.ErrMsgLocalCSVPathRequired,
					sources.ErrMsgLocalCSVSizeMustBePositive,
					sources.ErrMsgLocalCSVFileNotFound,
					sources.ErrMsgLocalCSVTransformerNil,
				},
			},
			domain.GetWriteActivityName(sinkCfg): {
				MaximumAttempts:    5,
				InitialInterval:    200 * time.Millisecond,
				BackoffCoefficient: 1.5,
				MaximumInterval:    10 * time.Second,
				NonRetryableErrorTypes: []string{
					sinks.ErrMsgSQLLiteSinkNil,
					sinks.ErrMsgSQLLiteSinkNilClient,
					sinks.ErrMsgSQLLiteSinkEmptyTable,
					sinks.ErrMsgSQLLiteSinkDBFileRequired,
					sqllite.ERR_SQLITE_DB_CONNECTION,
				},
			},
		},
	}

	env.SetOnActivityStartedListener(
		func(
			activityInfo *activity.Info,
			ctx context.Context,
			args converter.EncodedValues,
		) {
			activityType := activityInfo.ActivityType.Name
			if strings.HasPrefix(activityType, "internalSession") {
				return
			}

			l.Debug(
				"Test_ProcessBatchWorkflow_Temp_LocalCSV_SQLLite_HappyPath - Activity started",
				"activity-type", activityType,
			)

		})

	defer func() {
		if err := recover(); err != nil {
			l.Error(
				"Test_ProcessBatchWorkflow_Temp_LocalCSV_SQLLite_HappyPath - panicked",
				"workflow", ProcessLocalCSVSQLLiteWorkflowAlias,
				"error", err,
			)
		}

		err := env.GetWorkflowError()
		if err != nil {
			l.Error(
				"Test_ProcessBatchWorkflow_Temp_LocalCSV_SQLLite_HappyPath - error",
				"error", err.Error(),
			)
		} else {
			var result LocalCSVSQLLiteBatchRequest
			err := env.GetWorkflowResult(&result)
			require.NoError(t, err)
			l.Debug(
				"Test_ProcessBatchWorkflow_Temp_LocalCSV_SQLLite_HappyPath - success",
				"num-batches-processed", result.Snapshot.NumProcessed,
				"num-records-processed", result.Snapshot.NumRecords,
			)
			require.EqualValues(t, 4, uint(result.Snapshot.NumProcessed))
			errorCount := 0
			for _, errs := range result.Snapshot.Errors {
				errorCount += len(errs)
			}
			require.EqualValues(t, 13, uint(result.Snapshot.NumRecords))
			require.EqualValues(t, 0, errorCount)
		}

	}()

	env.ExecuteWorkflow(ProcessLocalCSVSQLLiteWorkflowAlias, req)

	require.True(t, env.IsWorkflowCompleted())

}

func Test_ProcessBatchWorkflow_LocalCSV_SQLLite_HappyPath(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()

	l := logger.GetSlogLogger()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	ctx = logger.WithLogger(ctx, l)

	env.SetWorkerOptions(worker.Options{
		BackgroundActivityContext: ctx,
		DeadlockDetectionTimeout:  24 * time.Hour, // set a long timeout to avoid deadlock detection during tests
	})

	env.SetTestTimeout(24 * time.Hour)

	// Register the workflow
	env.RegisterWorkflowWithOptions(
		bo.ProcessBatchWorkflow[domain.CSVRow, sources.LocalCSVConfig, sinks.SQLLiteSinkConfig[domain.CSVRow], snapshotters.LocalFileSnapshotterConfig],
		workflow.RegisterOptions{
			Name: ProcessLocalCSVSQLLiteWorkflowAlias,
		},
	)

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
	env.RegisterActivityWithOptions(
		bo.SnapshotActivity[snapshotters.LocalFileSnapshotterConfig],
		activity.RegisterOptions{
			Name: SnapshotLocalFileBatchActivityAlias,
		},
	)

	// Build source & sink configurations
	// Source - local CSV
	fileName := utils.BuildFileName()
	filePath, err := utils.BuildFilePath()
	require.NoError(t, err, "error building csv file path for test")
	path := filepath.Join(filePath, fileName)
	sourceCfg := sources.LocalCSVConfig{
		Path:         path,
		Delimiter:    '|',
		HasHeader:    true,
		MappingRules: domain.BuildBusinessModelTransformRules(),
	}

	// Sink - MongoDB
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

	ssCfg := snapshotters.LocalFileSnapshotterConfig{
		Path: filePath,
	}

	req := &LocalCSVSQLLiteBatchRequest{
		JobID:               "job-local-csv-sqllite-happy",
		BatchSize:           400,
		MaxInProcessBatches: 2,
		StartAt:             0,
		Source:              sourceCfg,
		Sink:                sinkCfg,
		Snapshotter:         ssCfg,
		Policies: map[string]domain.RetryPolicySpec{
			domain.GetFetchActivityName(sourceCfg): {
				MaximumAttempts:    3,
				InitialInterval:    100 * time.Millisecond,
				BackoffCoefficient: 2.0,
				MaximumInterval:    5 * time.Second,
				NonRetryableErrorTypes: []string{
					sources.ErrMsgLocalCSVPathRequired,
					sources.ErrMsgLocalCSVSizeMustBePositive,
					sources.ErrMsgLocalCSVFileNotFound,
					sources.ErrMsgLocalCSVTransformerNil,
				},
			},
			domain.GetWriteActivityName(sinkCfg): {
				MaximumAttempts:    5,
				InitialInterval:    200 * time.Millisecond,
				BackoffCoefficient: 1.5,
				MaximumInterval:    10 * time.Second,
				NonRetryableErrorTypes: []string{
					sinks.ErrMsgSQLLiteSinkNil,
					sinks.ErrMsgSQLLiteSinkNilClient,
					sinks.ErrMsgSQLLiteSinkEmptyTable,
					sinks.ErrMsgSQLLiteSinkDBFileRequired,
					sqllite.ERR_SQLITE_DB_CONNECTION,
				},
			},
		},
	}

	env.SetOnActivityStartedListener(
		func(
			activityInfo *activity.Info,
			ctx context.Context,
			args converter.EncodedValues,
		) {
			activityType := activityInfo.ActivityType.Name
			if strings.HasPrefix(activityType, "internalSession") {
				return
			}

			l.Debug(
				"Test_ProcessBatchWorkflow_LocalCSV_SQLLite_HappyPath - Activity started",
				"activity-type", activityType,
			)

		})

	defer func() {
		if err := recover(); err != nil {
			l.Error(
				"Test_ProcessBatchWorkflow_LocalCSV_SQLLite_HappyPath - panicked",
				"workflow", ProcessLocalCSVSQLLiteWorkflowAlias,
				"error", err,
			)
		}

		err := env.GetWorkflowError()
		if err != nil {
			l.Error(
				"Test_ProcessBatchWorkflow_LocalCSV_SQLLite_HappyPath - error",
				"error", err.Error(),
			)
		} else {
			var result LocalCSVSQLLiteBatchRequest
			err := env.GetWorkflowResult(&result)
			require.NoError(t, err)
			l.Debug(
				"Test_ProcessBatchWorkflow_LocalCSV_SQLLite_HappyPath - success",
				"num-batches-processed", result.Snapshot.NumProcessed,
				"num-records-processed", result.Snapshot.NumRecords,
			)
			require.EqualValues(t, 11, uint(result.Snapshot.NumProcessed))
			errorCount := 0
			for _, errs := range result.Snapshot.Errors {
				errorCount += len(errs)
			}
			require.EqualValues(t, 20, uint(result.Snapshot.NumRecords))
			require.EqualValues(t, 0, errorCount)
		}

	}()

	env.ExecuteWorkflow(ProcessLocalCSVSQLLiteWorkflowAlias, req)

	require.True(t, env.IsWorkflowCompleted())

}

func Test_ProcessBatchWorkflow_LocalCSV_SQLLite_TimeoutError(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()

	l := logger.GetSlogLogger()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	ctx = logger.WithLogger(ctx, l)

	env.SetWorkerOptions(worker.Options{
		BackgroundActivityContext: ctx,
		DeadlockDetectionTimeout:  6 * time.Hour, // set a long timeout to avoid deadlock detection during tests
	})

	env.SetTestTimeout(6 * time.Hour)

	// Register the workflow
	env.RegisterWorkflowWithOptions(
		bo.ProcessBatchWorkflow[domain.CSVRow, sources.LocalCSVConfig, sinks.SQLLiteSinkConfig[domain.CSVRow], snapshotters.LocalFileSnapshotterConfig],
		workflow.RegisterOptions{
			Name: ProcessLocalCSVSQLLiteWorkflowAlias,
		},
	)

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
	env.RegisterActivityWithOptions(
		bo.SnapshotActivity[snapshotters.LocalFileSnapshotterConfig],
		activity.RegisterOptions{
			Name: SnapshotLocalFileBatchActivityAlias,
		},
	)

	// Build source & sink configurations
	// Source - local CSV
	fileName := utils.BuildFileName()
	filePath, err := utils.BuildFilePath()
	require.NoError(t, err, "error building csv file path for test")
	path := filepath.Join(filePath, fileName)
	sourceCfg := sources.LocalCSVConfig{
		Path:         path,
		Delimiter:    '|',
		HasHeader:    true,
		MappingRules: domain.BuildBusinessModelTransformRules(),
	}

	// Sink - MongoDB
	dbFile := "data/__deleteme.db"
	dbClient, err := sqllite.NewSQLLiteDBClient(dbFile)
	require.NoError(t, err)

	defer func(cl *sqllite.SQLLiteDBClient, fpath string) {
		ags, err := cl.FetchRecords(context.Background(), "agent", 0, 25)
		require.NoError(t, err)
		require.Equal(t, len(ags), 3)

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

	ssCfg := snapshotters.LocalFileSnapshotterConfig{
		Path: filePath,
	}

	req := &LocalCSVSQLLiteBatchRequest{
		JobID:               "job-local-csv-sqllite-timeout-error",
		BatchSize:           400,
		MaxInProcessBatches: 2,
		PauseRecordCount:    5,
		PauseDuration:       2 * time.Minute,
		StartAt:             0,
		Source:              sourceCfg,
		Sink:                sinkCfg,
		Snapshotter:         ssCfg,
		Policies: map[string]domain.RetryPolicySpec{
			domain.GetFetchActivityName(sourceCfg): {
				MaximumAttempts:    3,
				InitialInterval:    100 * time.Millisecond,
				BackoffCoefficient: 2.0,
				MaximumInterval:    5 * time.Second,
				NonRetryableErrorTypes: []string{
					sources.ErrMsgLocalCSVPathRequired,
					sources.ErrMsgLocalCSVSizeMustBePositive,
					sources.ErrMsgLocalCSVFileNotFound,
					sources.ErrMsgLocalCSVTransformerNil,
				},
			},
			domain.GetWriteActivityName(sinkCfg): {
				MaximumAttempts:    5,
				InitialInterval:    200 * time.Millisecond,
				BackoffCoefficient: 1.5,
				MaximumInterval:    10 * time.Second,
				NonRetryableErrorTypes: []string{
					sinks.ErrMsgSQLLiteSinkNil,
					sinks.ErrMsgSQLLiteSinkNilClient,
					sinks.ErrMsgSQLLiteSinkEmptyTable,
					sinks.ErrMsgSQLLiteSinkDBFileRequired,
					sqllite.ERR_SQLITE_DB_CONNECTION,
				},
			},
			domain.GetSnapshotActivityName(ssCfg): {
				MaximumAttempts:    3,
				InitialInterval:    100 * time.Millisecond,
				BackoffCoefficient: 2.0,
				MaximumInterval:    15 * time.Minute,
				NonRetryableErrorTypes: []string{
					snapshotters.ERR_MISSING_OBJECT_PATH,
					snapshotters.ERR_MISSING_KEY,
				},
			},
		},
	}

	env.SetOnActivityStartedListener(
		func(
			activityInfo *activity.Info,
			ctx context.Context,
			args converter.EncodedValues,
		) {
			activityType := activityInfo.ActivityType.Name
			if strings.HasPrefix(activityType, "internalSession") {
				return
			}

			l.Debug(
				"Test_ProcessBatchWorkflow_LocalCSV_SQLLite_TimeoutError - Activity started",
				"activity-type", activityType,
			)

		},
	)

	env.OnActivity(
		FetchNextLocalCSVSourceBatchActivityAlias,
		mock.Anything, mock.Anything,
	).Return(
		func(
			ctx context.Context,
			in *domain.FetchInput[domain.CSVRow, sources.LocalCSVConfig],
		) (*domain.FetchOutput[domain.CSVRow], error) {
			if in.Offset >= 682 {
				// return nil & activity timeout error
				return nil, temporal.NewTimeoutError(enums.TIMEOUT_TYPE_START_TO_CLOSE, errors.New("simulated start to close timeout error for testing"))
			}
			return bo.FetchNextActivity(ctx, in)
		},
	)

	defer func() {
		if err := recover(); err != nil {
			l.Error(
				"Test_ProcessBatchWorkflow_LocalCSV_SQLLite_TimeoutError - panicked",
				"workflow", ProcessLocalCSVSQLLiteWorkflowAlias,
				"error", err,
			)
		}

		err := env.GetWorkflowError()
		require.Error(t, err, "workflow should error out due to activity timeout")
		require.Contains(t, err.Error(), "simulated start to close timeout error for testing", "expected timeout error message")

	}()

	env.ExecuteWorkflow(ProcessLocalCSVSQLLiteWorkflowAlias, req)

	require.True(t, env.IsWorkflowCompleted())

}

func Test_ProcessBatchWorkflow_Err_NonRetryable(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()

	l := logger.GetSlogLogger()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	ctx = logger.WithLogger(ctx, l)

	ctx = context.WithValue(ctx, "fail-source", true)

	env.SetWorkerOptions(worker.Options{
		BackgroundActivityContext: ctx,
		DeadlockDetectionTimeout:  24 * time.Hour, // set a long timeout to avoid deadlock detection during tests
	})

	env.SetTestTimeout(24 * time.Hour)

	// Register the workflow
	env.RegisterWorkflowWithOptions(
		bo.ProcessBatchWorkflow[domain.CSVRow, *fakeSrcConfig, *fakeSinkConfig[domain.CSVRow], *snapshotters.LocalFileSnapshotterConfig],
		workflow.RegisterOptions{
			Name: ProcessFakeSrcSinkWorkflowAlias,
		},
	)

	// Register activities.
	env.RegisterActivityWithOptions(
		bo.FetchNextActivity[domain.CSVRow, *fakeSrcConfig],
		activity.RegisterOptions{
			Name: FetchNextFakeSourceBatchAlias,
		},
	)
	env.RegisterActivityWithOptions(
		bo.WriteActivity[domain.CSVRow, *fakeSinkConfig[domain.CSVRow]],
		activity.RegisterOptions{
			Name: WriteNextFakeSinkBatchAlias,
		},
	)

	sourceCfg := &fakeSrcConfig{}
	sinkCfg := &fakeSinkConfig[domain.CSVRow]{}

	req := &FakeSourceSinkBatchRequest{
		JobID:               "job-retryable-error",
		BatchSize:           400,
		MaxInProcessBatches: 2,
		StartAt:             0,
		Source:              sourceCfg,
		Sink:                sinkCfg,
		Policies: map[string]domain.RetryPolicySpec{
			domain.GetFetchActivityName(sourceCfg): {
				MaximumAttempts:    3,
				InitialInterval:    100 * time.Millisecond,
				BackoffCoefficient: 2.0,
				MaximumInterval:    5 * time.Second,
				NonRetryableErrorTypes: []string{
					ErrMsgBuildSource,
					ErrMsgSourceNext,
				},
			},
			domain.GetWriteActivityName(sinkCfg): {
				MaximumAttempts:    5,
				InitialInterval:    200 * time.Millisecond,
				BackoffCoefficient: 1.5,
				MaximumInterval:    10 * time.Second,
				NonRetryableErrorTypes: []string{
					ErrMsgBuildSink,
					ErrMsgSinkWrite,
				},
			},
		},
	}

	env.SetOnActivityStartedListener(
		func(
			activityInfo *activity.Info,
			ctx context.Context,
			args converter.EncodedValues,
		) {
			activityType := activityInfo.ActivityType.Name
			if strings.HasPrefix(activityType, "internalSession") {
				return
			}

			l.Debug(
				"Test_ProcessBatchWorkflow_Err_NonRetryable - Activity started",
				"activity-type", activityType,
			)

		})

	defer func() {
		if err := recover(); err != nil {
			l.Error(
				"Test_ProcessBatchWorkflow_Err_NonRetryable - panicked",
				"workflow", ProcessFakeSrcSinkWorkflowAlias,
				"error", err,
			)
		}

		err := env.GetWorkflowError()
		require.Error(t, err, "workflow should fail fast due to policy non-retryable")
		// require.Equal(t, 1, sourceCfg.calls, "policy should prevent retries, only 1 attempt")

	}()
	env.ExecuteWorkflow(ProcessFakeSrcSinkWorkflowAlias, req)

	require.True(t, env.IsWorkflowCompleted())
}

func Test_ProcessBatchWorkflow_Err_Retryable(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()

	l := logger.GetSlogLogger()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	ctx = logger.WithLogger(ctx, l)

	ctx = context.WithValue(ctx, "fail-retryable", true)

	env.SetWorkerOptions(worker.Options{
		BackgroundActivityContext: ctx,
		DeadlockDetectionTimeout:  24 * time.Hour, // set a long timeout to avoid deadlock detection during tests
	})

	env.SetTestTimeout(24 * time.Hour)

	// Register the workflow
	env.RegisterWorkflowWithOptions(
		bo.ProcessBatchWorkflow[domain.CSVRow, *fakeSrcConfig, *fakeSinkConfig[domain.CSVRow], *snapshotters.LocalFileSnapshotterConfig],
		workflow.RegisterOptions{
			Name: ProcessFakeSrcSinkWorkflowAlias,
		},
	)

	// Register activities.
	env.RegisterActivityWithOptions(
		bo.FetchNextActivity[domain.CSVRow, *fakeSrcConfig],
		activity.RegisterOptions{
			Name: FetchNextFakeSourceBatchAlias,
		},
	)
	env.RegisterActivityWithOptions(
		bo.WriteActivity[domain.CSVRow, *fakeSinkConfig[domain.CSVRow]],
		activity.RegisterOptions{
			Name: WriteNextFakeSinkBatchAlias,
		},
	)

	sourceCfg := &fakeSrcConfig{}
	sinkCfg := &fakeSinkConfig[domain.CSVRow]{}

	nextMaxAttempts := 3
	req := &FakeSourceSinkBatchRequest{
		JobID:               "job-retryable-error",
		BatchSize:           400,
		MaxInProcessBatches: 2,
		StartAt:             0,
		Source:              sourceCfg,
		Sink:                sinkCfg,
		Policies: map[string]domain.RetryPolicySpec{
			domain.GetFetchActivityName(sourceCfg): {
				MaximumAttempts:    int32(nextMaxAttempts),
				InitialInterval:    100 * time.Millisecond,
				BackoffCoefficient: 2.0,
				MaximumInterval:    5 * time.Second,
				NonRetryableErrorTypes: []string{
					ErrMsgBuildSource,
					ErrMsgSourceNext,
				},
			},
			domain.GetWriteActivityName(sinkCfg): {
				MaximumAttempts:    5,
				InitialInterval:    200 * time.Millisecond,
				BackoffCoefficient: 1.5,
				MaximumInterval:    10 * time.Second,
				NonRetryableErrorTypes: []string{
					ErrMsgBuildSink,
					ErrMsgSinkWrite,
				},
			},
		},
	}

	env.SetOnActivityStartedListener(
		func(
			activityInfo *activity.Info,
			ctx context.Context,
			args converter.EncodedValues,
		) {
			activityType := activityInfo.ActivityType.Name
			if strings.HasPrefix(activityType, "internalSession") {
				return
			}

			l.Debug(
				"Test_ProcessBatchWorkflow_Err_Retryable - Activity started",
				"activity-type", activityType,
			)

		})

	defer func() {
		if err := recover(); err != nil {
			l.Error(
				"Test_ProcessBatchWorkflow_Err_Retryable - panicked",
				"workflow", ProcessFakeSrcSinkWorkflowAlias,
				"error", err,
			)
		}

		err := env.GetWorkflowError()
		require.Error(t, err, "workflow should fail after retrying for max attempts")
		// require.Equal(t, nextMaxAttempts, sourceCfg.calls, "should retry up to MaximumAttempts")

	}()
	env.ExecuteWorkflow(ProcessFakeSrcSinkWorkflowAlias, req)

	require.True(t, env.IsWorkflowCompleted())
}

// TODO add tests for pauseRecCount, pauseDuration

func extractContinueAsNewInput[T any](err error, dc converter.DataConverter, out *T) (ok bool, _ error) {
	var cae *workflow.ContinueAsNewError
	if !errors.As(err, &cae) || cae == nil {
		return false, nil
	}
	// Decode the single argument you continued with.
	if e := dc.FromPayloads(cae.Input, out); e != nil {
		return true, e
	}
	return true, nil
}

const FakeSource = "fake-source"
const FakeSink = "fake-sink"

const ErrMsgBuildSource = "failed to build source"
const ErrMsgBuildSink = "failed to build sink"
const ErrMsgBuildSourceRetryable = "failed to build source retryable error"
const ErrMsgSourceNext = "failed to get next batch from source"
const ErrMsgSinkWrite = "failed to write batch to sink"
const ErrMsgBuildSinkRetryable = "failed to build sink retryable error"

const ProcessFakeSrcSinkWorkflowAlias string = "process-Fake-src-sink-workflow-alias"
const FetchNextFakeSourceBatchAlias string = "fetch-next-" + FakeSource + "-batch-alias"
const WriteNextFakeSinkBatchAlias string = "write-next-" + FakeSink + "-batch-alias"

type FakeSourceSinkBatchRequest domain.BatchProcessingRequest[domain.CSVRow, *fakeSrcConfig, *fakeSinkConfig[domain.CSVRow], *snapshotters.LocalFileSnapshotterConfig]

type fakeSrcConfig struct {
	calls int
}

func (c *fakeSrcConfig) Name() string { return FakeSource }
func (c *fakeSrcConfig) BuildSource(ctx context.Context) (domain.Source[domain.CSVRow], error) {
	c.calls++
	if ctx != nil {
		if fail, ok := ctx.Value("fail-source").(bool); ok && fail {
			return nil, errors.New(ErrMsgBuildSource)
		}
		if fail, ok := ctx.Value("fail-retryable").(bool); ok && fail {
			return nil, errors.New(ErrMsgBuildSourceRetryable)
		}
		if fail, ok := ctx.Value("fail-application-err").(bool); ok && fail {
			return nil, temporal.NewNonRetryableApplicationError("ErrValidation", "ErrValidation", nil)
		}
	}
	return &fakeSource{}, nil
}

type fakeSource struct{}

func (s *fakeSource) Name() string                    { return FakeSource }
func (s *fakeSource) Close(ctx context.Context) error { return nil }
func (s *fakeSource) Next(ctx context.Context, offset uint64, size uint) (*domain.BatchProcess, error) {
	if ctx != nil {
		if fail, ok := ctx.Value("fail-next").(bool); ok && fail {
			return nil, errors.New(ErrMsgSourceNext)
		}
	}

	bp := &domain.BatchProcess{
		Records:     nil,
		NextOffset:  offset,
		StartOffset: offset,
		Done:        false,
	}

	return bp, nil
}

type fakeSinkConfig[T any] struct {
	calls int
}

func (c fakeSinkConfig[T]) Name() string { return FakeSink }
func (c fakeSinkConfig[T]) BuildSink(ctx context.Context) (domain.Sink[T], error) {
	c.calls++
	if ctx != nil {
		if fail, ok := ctx.Value("fail-sink").(bool); ok && fail {
			return nil, errors.New(ErrMsgBuildSink)
		}
		if fail, ok := ctx.Value("fail-retryable").(bool); ok && fail {
			return nil, errors.New(ErrMsgBuildSinkRetryable)
		}
	}
	return &fakeSink[T]{}, nil
}

type fakeSink[T any] struct{}

func (s *fakeSink[T]) Name() string { return FakeSink }
func (s *fakeSink[T]) Write(ctx context.Context, b *domain.BatchProcess) (*domain.BatchProcess, error) {
	if ctx != nil {
		if fail, ok := ctx.Value("fail-write").(bool); ok && fail {
			return nil, errors.New(ErrMsgSinkWrite)
		}

	}

	for _, rec := range b.Records {
		rec.BatchResult.Result = rec.Data // echo the record as result
	}
	b.Done = true // mark as done
	return b, nil
}
func (s *fakeSink[T]) Close(ctx context.Context) error {
	return nil
}
