package batch_orchestra_test

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/converter"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"

	"github.com/comfforts/logger"

	bo "github.com/hankgalt/batch-orchestra"
	"github.com/hankgalt/batch-orchestra/internal/clients/mongodb"
	sqllite "github.com/hankgalt/batch-orchestra/internal/clients/sql_lite"
	"github.com/hankgalt/batch-orchestra/internal/sinks"
	"github.com/hankgalt/batch-orchestra/internal/sources"
	"github.com/hankgalt/batch-orchestra/pkg/domain"
	"github.com/hankgalt/batch-orchestra/pkg/utils"
)

type LocalCSVMongoBatchRequest domain.BatchProcessingRequest[domain.CSVRow, sources.LocalCSVConfig, sinks.MongoSinkConfig[domain.CSVRow]]
type LocalCSVNoopBatchRequest domain.BatchProcessingRequest[domain.CSVRow, sources.LocalCSVConfig, sinks.NoopSinkConfig[domain.CSVRow]]
type CloudCSVMongoBatchRequest domain.BatchProcessingRequest[domain.CSVRow, sources.CloudCSVConfig, sinks.MongoSinkConfig[domain.CSVRow]]
type LocalCSVSQLLiteBatchRequest domain.BatchProcessingRequest[domain.CSVRow, sources.LocalCSVConfig, sinks.SQLLiteSinkConfig[domain.CSVRow]]

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

// Setup mongo cluster & setup mongo & cloud test environment, before running this test.
func (s *ProcessBatchWorkflowTestSuite) Test_ProcessBatchWorkflow_CloudCSV_Mongo_HappyPath() {
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
		// Register the workflow
		s.env.RegisterWorkflowWithOptions(
			bo.ProcessBatchWorkflow[domain.CSVRow, sources.CloudCSVConfig, sinks.MongoSinkConfig[domain.CSVRow]],
			workflow.RegisterOptions{
				Name: bo.ProcessCloudCSVMongoWorkflowAlias,
			},
		)

		// Register activities.
		s.env.RegisterActivityWithOptions(
			bo.FetchNextActivity[domain.CSVRow, sources.CloudCSVConfig],
			activity.RegisterOptions{
				Name: bo.FetchNextCloudCSVSourceBatchAlias,
			},
		)
		s.env.RegisterActivityWithOptions(
			bo.WriteActivity[domain.CSVRow, sinks.MongoSinkConfig[domain.CSVRow]],
			activity.RegisterOptions{
				Name: bo.WriteNextMongoSinkBatchAlias,
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

		// Sink - MongoDB
		mCfg := utils.BuildMongoStoreConfig()
		s.Require().NotEmpty(mCfg.DBName, "MongoDB DBName should not be empty")
		s.Require().NotEmpty(mCfg.Host, "MongoDB host should not be empty")
		sinkCfg := sinks.MongoSinkConfig[domain.CSVRow]{
			Protocol:   mCfg.Protocol,
			Host:       mCfg.Host,
			DBName:     mCfg.DBName,
			User:       mCfg.User,
			Pwd:        mCfg.Pwd,
			Params:     mCfg.Params,
			Collection: "vypar.agents",
		}

		req := &CloudCSVMongoBatchRequest{
			JobID:               "cloud-csv-mongo-happy",
			BatchSize:           400,
			MaxInProcessBatches: 2,
			StartAt:             0,
			Source:              sourceCfg,
			Sink:                sinkCfg,
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
					"Test_ProcessBatchWorkflow_CloudCSV_Mongo_HappyPath - Activity started",
					"activityType", activityType,
				)

			})

		defer func() {
			if err := recover(); err != nil {
				l.Error(
					"Test_ProcessBatchWorkflow_CloudCSV_Mongo_HappyPath - panicked",
					"workflow", bo.ProcessCloudCSVMongoWorkflowAlias, "error", err,
				)
			}

			err := s.env.GetWorkflowError()
			if err != nil {
				l.Error(
					"Test_ProcessBatchWorkflow_CloudCSV_Mongo_HappyPath - error",
					"workflow", bo.ProcessCloudCSVMongoWorkflowAlias, "error", err,
				)
			} else {
				var result CloudCSVMongoBatchRequest
				err := s.env.GetWorkflowResult(&result)
				if err != nil {
					l.Error(
						"Test_ProcessBatchWorkflow_CloudCSV_Mongo_HappyPath - error",
						"workflow", bo.ProcessCloudCSVMongoWorkflowAlias, "error", err,
					)
				} else {
					l.Debug(
						"Test_ProcessBatchWorkflow_CloudCSV_Mongo_HappyPath - success",
						"offsets", result.Offsets,
						"num-batches-processed", len(result.Offsets),
					)
				}
			}

		}()

		l.Debug("Test_ProcessBatchWorkflow_CloudCSV_Mongo_HappyPath - Starting workflow test")
		s.env.ExecuteWorkflow(bo.ProcessCloudCSVMongoWorkflowAlias, req)
		s.True(s.env.IsWorkflowCompleted())
		l.Debug("Test_ProcessBatchWorkflow_CloudCSV_Mongo_HappyPath - test completed")
	})
}

// Setup mongo cluster, setup local csv file,
// & setup mongo test environment, before running this test.
func Test_ProcessBatchWorkflow_LocalCSV_Mongo_HappyPath(t *testing.T) {
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
		bo.ProcessBatchWorkflow[domain.CSVRow, sources.LocalCSVConfig, sinks.MongoSinkConfig[domain.CSVRow]],
		workflow.RegisterOptions{
			Name: bo.ProcessLocalCSVMongoWorkflowAlias,
		},
	)

	// Register activities.
	env.RegisterActivityWithOptions(
		bo.FetchNextActivity[domain.CSVRow, sources.LocalCSVConfig],
		activity.RegisterOptions{
			Name: bo.FetchNextLocalCSVSourceBatchAlias,
		},
	)
	env.RegisterActivityWithOptions(
		bo.WriteActivity[domain.CSVRow, sinks.MongoSinkConfig[domain.CSVRow]],
		activity.RegisterOptions{
			Name: bo.WriteNextMongoSinkBatchAlias,
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
	mCfg := utils.BuildMongoStoreConfig()
	require.NotEmpty(t, mCfg.DBName, "MongoDB name should not be empty")
	require.NotEmpty(t, mCfg.Host, "MongoDB host should not be empty")
	sinkCfg := sinks.MongoSinkConfig[domain.CSVRow]{
		Protocol:   mCfg.Protocol,
		Host:       mCfg.Host,
		DBName:     mCfg.DBName,
		User:       mCfg.User,
		Pwd:        mCfg.Pwd,
		Params:     mCfg.Params,
		Collection: "vypar.agents",
	}

	req := &LocalCSVMongoBatchRequest{
		JobID:               "job-happy",
		BatchSize:           400,
		MaxInProcessBatches: 2,
		StartAt:             0,
		Source:              sourceCfg,
		Sink:                sinkCfg,
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
				"Test_ProcessBatchWorkflow_LocalCSV_Mongo_HappyPath - Activity started",
				"activity-type", activityType,
			)

		})

	defer func() {
		if err := recover(); err != nil {
			l.Error(
				"Test_ProcessBatchWorkflow_LocalCSV_Mongo_HappyPath - panicked",
				"workflow", bo.ProcessLocalCSVMongoWorkflowAlias,
				"error", err,
			)
		}

		err := env.GetWorkflowError()
		if err != nil {
			l.Error(
				"Test_ProcessBatchWorkflow_LocalCSV_Mongo_HappyPath - error",
				"error", err.Error(),
			)
		} else {
			var result LocalCSVMongoBatchRequest
			err := env.GetWorkflowResult(&result)
			if err != nil {
				l.Error(
					"Test_ProcessBatchWorkflow_LocalCSV_Mongo_HappyPath - error",
					"error", err.Error(),
				)
			} else {
				l.Debug(
					"Test_ProcessBatchWorkflow_LocalCSV_Mongo_HappyPath - success",
					"result-offsets", result.Offsets,
					"num-batches-processed", len(result.Offsets),
				)
			}
		}

	}()

	env.ExecuteWorkflow(bo.ProcessLocalCSVMongoWorkflowAlias, req)

	require.True(t, env.IsWorkflowCompleted())

}

// Setup mongo cluster, setup local csv file & setup mongo test env vars, before running this test.
func Test_ProcessBatchWorkflow_LocalCSV_Mongo_ContinueAsNewError(t *testing.T) {
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
		bo.ProcessBatchWorkflow[domain.CSVRow, sources.LocalCSVConfig, sinks.MongoSinkConfig[domain.CSVRow]],
		workflow.RegisterOptions{
			Name: bo.ProcessLocalCSVMongoWorkflowAlias,
		},
	)

	// Register activities.
	env.RegisterActivityWithOptions(
		bo.FetchNextActivity[domain.CSVRow, sources.LocalCSVConfig],
		activity.RegisterOptions{
			Name: bo.FetchNextLocalCSVSourceBatchAlias,
		},
	)
	env.RegisterActivityWithOptions(
		bo.WriteActivity[domain.CSVRow, sinks.MongoSinkConfig[domain.CSVRow]],
		activity.RegisterOptions{
			Name: bo.WriteNextMongoSinkBatchAlias,
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
	mCfg := utils.BuildMongoStoreConfig()
	require.NotEmpty(t, mCfg.DBName, "MongoDB name should not be empty")
	require.NotEmpty(t, mCfg.Host, "MongoDB host should not be empty")
	sinkCfg := sinks.MongoSinkConfig[domain.CSVRow]{
		Protocol:   mCfg.Protocol,
		Host:       mCfg.Host,
		DBName:     mCfg.DBName,
		User:       mCfg.User,
		Pwd:        mCfg.Pwd,
		Params:     mCfg.Params,
		Collection: "vypar.agents",
	}

	req := &LocalCSVMongoBatchRequest{
		JobID:               "job-happy",
		BatchSize:           400,
		MaxInProcessBatches: 2,
		MaxBatches:          6,
		StartAt:             0,
		Source:              sourceCfg,
		Sink:                sinkCfg,
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
				"Test_ProcessBatchWorkflow_LocalCSV_Mongo_ContinueAsNewError - Activity started",
				"activity-type", activityType,
			)

		})

	defer func() {
		if err := recover(); err != nil {
			l.Error(
				"Test_ProcessBatchWorkflow_LocalCSV_Mongo_ContinueAsNewError - panicked",
				"workflow", bo.ProcessLocalCSVMongoWorkflowAlias,
				"error", err,
			)
		}

		err := env.GetWorkflowError()
		var ca *workflow.ContinueAsNewError
		require.True(t, errors.As(err, &ca), "expected ContinueAsNewError, got: %v", err)
		require.Equal(t, bo.ProcessLocalCSVMongoWorkflowAlias, ca.WorkflowType.Name, "expected workflow type to match")

		var next LocalCSVMongoBatchRequest
		ok, decErr := extractContinueAsNewInput(err, converter.GetDefaultDataConverter(), &next)
		require.True(t, ok, "expected to extract continue-as-new input, got error: %v", decErr)
		require.NoError(t, decErr, "error extracting continue-as-new input")
		require.NotNil(t, &next, "expected non-nil continue-as-new input")
		require.True(t, next.StartAt > req.StartAt, "expected next.StartAt > req.StartAt")
	}()

	env.ExecuteWorkflow(bo.ProcessLocalCSVMongoWorkflowAlias, req)
	require.True(t, env.IsWorkflowCompleted())

}

// Integration test for ProcessBatchWorkflow on a dev Temporal server.
// Start temporal dev server, setup mongo cluster, setup local csv file,
// & setup mongo, temporal test environment, before running this test.
func Test_ProcessBatchWorkflow_LocalCSV_Mongo_HappyPath_Server(t *testing.T) {
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
	const tq = "process-batch-workflow-local-csv-mongo-tq"

	// Temporal worker for the task queue
	w := worker.New(c, tq, worker.Options{
		BackgroundActivityContext: ctx,            // Global worker context for activities
		DeadlockDetectionTimeout:  24 * time.Hour, // set a long timeout to avoid deadlock detection during tests
	})

	// Register the workflow function
	w.RegisterWorkflowWithOptions(
		bo.ProcessBatchWorkflow[domain.CSVRow, sources.LocalCSVConfig, sinks.MongoSinkConfig[domain.CSVRow]],
		workflow.RegisterOptions{
			Name: bo.ProcessLocalCSVMongoWorkflowAlias,
		},
	)

	// Register activities.
	w.RegisterActivityWithOptions(
		bo.FetchNextActivity[domain.CSVRow, sources.LocalCSVConfig],
		activity.RegisterOptions{
			Name: bo.FetchNextLocalCSVSourceBatchAlias,
		},
	)
	w.RegisterActivityWithOptions(
		bo.WriteActivity[domain.CSVRow, sinks.MongoSinkConfig[domain.CSVRow]],
		activity.RegisterOptions{
			Name: bo.WriteNextMongoSinkBatchAlias,
		},
	)

	// Start the worker in a goroutine
	go func() {
		// Stop worker when test ends.
		if err := w.Run(worker.InterruptCh()); err != nil {
			l.Error("Test_ProcessBatchWorkflow_LocalCSV_Mongo_HappyPath_Server - worker stopped:", "error", err.Error())
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

	// Sink - MongoDB
	mCfg := utils.BuildMongoStoreConfig()
	require.NotEmpty(t, mCfg.DBName, "MongoDB name should not be empty")
	require.NotEmpty(t, mCfg.Host, "MongoDB host should not be empty")
	sinkCfg := sinks.MongoSinkConfig[domain.CSVRow]{
		Protocol:   mCfg.Protocol,
		Host:       mCfg.Host,
		DBName:     mCfg.DBName,
		User:       mCfg.User,
		Pwd:        mCfg.Pwd,
		Params:     mCfg.Params,
		Collection: "vypar.agents",
	}

	req := &LocalCSVMongoBatchRequest{
		JobID:               "process-batch-workflow-local-csv-mongo-server-happy",
		BatchSize:           200,
		MaxInProcessBatches: 2,
		MaxBatches:          6,
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
					sinks.ErrMsgMongoSinkNil,
					sinks.ErrMsgMongoSinkNilClient,
					sinks.ErrMsgMongoSinkEmptyCollection,
					sinks.ErrMsgMongoSinkDBProtocolRequired,
					sinks.ErrMsgMongoSinkDBHostRequired,
					sinks.ErrMsgMongoSinkDBNameRequired,
					sinks.ErrMsgMongoSinkDBUserRequired,
					sinks.ErrMsgMongoSinkDBPwdRequired,
					mongodb.ErrMsgMongoMissingDBName,
					mongodb.ErrMsgMongoCollectionNameOrDocEmpty,
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
		bo.ProcessLocalCSVMongoWorkflowAlias, // workflow function
		req,                                  // workflow input arg1
	)
	require.NoError(t, err)

	// With a real server, Continue-As-New chains transparently; Get waits for the final run.
	var out LocalCSVMongoBatchRequest
	require.NoError(t, run.Get(ctx, &out))
	require.NotNil(t, out)
	require.True(t, out.Done, "workflow should be marked as done")

	l.Debug("Test_ProcessBatchWorkflow_LocalCSV_Mongo_HappyPath_Server - workflow completed successfully",
		"workflow-id", run.GetID(),
		"workflow-run-id", run.GetRunID(),
		"offsets", out.Offsets,
		"num-batches-processed", len(out.Offsets),
	)
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
		bo.ProcessBatchWorkflow[domain.CSVRow, sources.LocalCSVConfig, sinks.SQLLiteSinkConfig[domain.CSVRow]],
		workflow.RegisterOptions{
			Name: bo.ProcessLocalCSVSQLLiteWorkflowAlias,
		},
	)

	// Register activities.
	env.RegisterActivityWithOptions(
		bo.FetchNextActivity[domain.CSVRow, sources.LocalCSVConfig],
		activity.RegisterOptions{
			Name: bo.FetchNextLocalCSVSourceBatchAlias,
		},
	)
	env.RegisterActivityWithOptions(
		bo.WriteActivity[domain.CSVRow, sinks.SQLLiteSinkConfig[domain.CSVRow]],
		activity.RegisterOptions{
			Name: bo.WriteNextSQLLiteSinkBatchAlias,
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

	req := &LocalCSVSQLLiteBatchRequest{
		JobID:               "job-happy",
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
				"workflow", bo.ProcessLocalCSVSQLLiteWorkflowAlias,
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
				"result-offsets", result.Offsets,
				"num-batches-processed", len(result.Batches),
			)
			require.EqualValues(t, 3, len(result.Offsets))
			require.EqualValues(t, 2, len(result.Batches))
			recordCount := 0
			errorCount := 0
			for _, batch := range result.Batches {
				recordCount += len(batch.Records)
				for _, rec := range batch.Records {
					if rec.BatchResult.Error != "" {
						errorCount++
					}
				}
			}
			require.EqualValues(t, 13, recordCount)
			require.EqualValues(t, 0, errorCount)
		}

	}()

	env.ExecuteWorkflow(bo.ProcessLocalCSVSQLLiteWorkflowAlias, req)

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
		bo.ProcessBatchWorkflow[domain.CSVRow, *fakeSrcConfig, *fakeSinkConfig[domain.CSVRow]],
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
		bo.ProcessBatchWorkflow[domain.CSVRow, *fakeSrcConfig, *fakeSinkConfig[domain.CSVRow]],
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

type FakeSourceSinkBatchRequest domain.BatchProcessingRequest[domain.CSVRow, *fakeSrcConfig, *fakeSinkConfig[domain.CSVRow]]

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
func (s *fakeSource) Next(ctx context.Context, offset uint64, size uint) (*domain.BatchProcess[domain.CSVRow], error) {
	if ctx != nil {
		if fail, ok := ctx.Value("fail-next").(bool); ok && fail {
			return nil, errors.New(ErrMsgSourceNext)
		}
	}

	bp := &domain.BatchProcess[domain.CSVRow]{
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
func (s *fakeSink[T]) Write(ctx context.Context, b *domain.BatchProcess[T]) (*domain.BatchProcess[T], error) {
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
