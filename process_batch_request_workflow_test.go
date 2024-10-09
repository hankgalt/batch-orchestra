package batch_orchestra_test

import (
	"context"
	"fmt"
	"log/slog"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/suite"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/converter"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/worker"

	bo "github.com/hankgalt/batch-orchestra"
	"github.com/hankgalt/batch-orchestra/clients"
)

type ProcessBatchRequestWorkflowTestSuite struct {
	suite.Suite
	testsuite.WorkflowTestSuite

	env *testsuite.TestWorkflowEnvironment
}

func TestProcessCSVWorkflowTestSuite(t *testing.T) {
	suite.Run(t, new(ProcessBatchRequestWorkflowTestSuite))
}

func (s *ProcessBatchRequestWorkflowTestSuite) SetupTest() {
	// get test logger
	l := getTestLogger()

	// set environment logger
	s.SetLogger(l)
}

func (s *ProcessBatchRequestWorkflowTestSuite) TearDownTest() {
	s.env.AssertExpectations(s.T())

	// err := os.RemoveAll(TEST_DIR)
	// s.NoError(err)
}

func (s *ProcessBatchRequestWorkflowTestSuite) Test_Local_File_ProcessBatchRequestWorkflow() {
	l := s.GetLogger()

	s.env = s.NewTestWorkflowEnvironment()

	// register workflow
	s.env.RegisterWorkflow(bo.ProcessBatchRequestWorkflow)

	// register activities
	s.env.RegisterActivityWithOptions(bo.GetCSVHeadersActivity, activity.RegisterOptions{
		Name: bo.GetCSVHeadersActivityName,
	})
	s.env.RegisterActivityWithOptions(bo.GetNextOffsetActivity, activity.RegisterOptions{
		Name: bo.GetNextOffsetActivityName,
	})
	s.env.RegisterActivityWithOptions(bo.ProcessBatchActivity, activity.RegisterOptions{
		Name: bo.ProcessBatchActivityName,
	})

	// create file client
	fileClient := &clients.LocalCSVFileClient{}

	ctx := context.WithValue(context.Background(), bo.ReaderClientContextKey, fileClient)
	s.env.SetWorkerOptions(worker.Options{
		BackgroundActivityContext: ctx,
	})

	s.env.SetTestTimeout(24 * time.Hour)

	testCfg := getTestConfig()

	s.Run("valid file", func() {
		start := time.Now()

		filePath := fmt.Sprintf("%s/%s", testCfg.dir, testCfg.filePath)
		req := &bo.BatchRequest{
			MaxBatches: 3,
			BatchSize:  400,
			Source: &bo.FileSource{
				FileName: LIVE_FILE_NAME_1,
				FilePath: filePath,
			},
		}

		expectedCall := []string{
			bo.GetCSVHeadersActivityName,
			bo.GetNextOffsetActivityName,
			bo.ProcessBatchActivityName,
		}

		var activityCalled []string
		s.env.SetOnActivityStartedListener(func(activityInfo *activity.Info, ctx context.Context, args converter.EncodedValues) {
			activityType := activityInfo.ActivityType.Name
			if strings.HasPrefix(activityType, "internalSession") {
				return
			}
			activityCalled = append(activityCalled, activityType)
			// var lastOffset int64
			switch activityType {
			case expectedCall[0]:
				// get headers
				var input bo.FileInfo
				s.NoError(args.Get(&input))
				s.Equal(req.Source.FileName, input.FileName)
				s.Equal(req.Source.FilePath, input.FilePath)
			case expectedCall[1]:
				// next offset
				var input bo.FileInfo
				s.NoError(args.Get(&input))
				s.Equal(req.Source.FileName, input.FileName)
				s.Equal(req.Source.FilePath, input.FilePath)
				l.Debug("Test_Local_File_ProcessBatchRequestWorkflow - next offset called", slog.Any("headers", input.Headers), slog.Any("start", input.Start), slog.Any("offsets", input.OffSets))
			case expectedCall[2]:
				// process batch
				var input bo.Batch
				s.NoError(args.Get(&input))
				s.Equal(req.Source.FileName, input.FileInfo.FileName)
				s.Equal(req.Source.FilePath, input.FilePath)
				l.Debug("Test_Local_File_ProcessBatchRequestWorkflow - process batch called", slog.Any("headers", input.Headers), slog.Any("start", input.Start), slog.Any("offsets", input.OffSets))
			default:
				panic("Test_Local_File_ProcessBatchRequestWorkflow - unexpected activity call")
			}
		})

		defer func() {
			if err := recover(); err != nil {
				l.Error(
					"Test_Local_File_ProcessBatchRequestWorkflow - panicked",
					slog.Any("error", err),
					slog.String("wkfl", bo.ProcessBatchRequestWorkflowName))
			}

			err := s.env.GetWorkflowError()
			if err != nil {
				l.Error("Test_Local_File_ProcessBatchRequestWorkflow - error", slog.Any("error", err))
			} else {
				var result bo.BatchRequest
				s.env.GetWorkflowResult(&result)

				timeTaken := time.Since(start)
				batches := [][]int64{}
				recordCount := 0
				for _, v := range result.Batches {
					batches = append(batches, []int64{v.Start, v.End, int64(len(v.Records))})
					recordCount += len(v.Records)
				}
				sort.SliceStable(batches, func(i, j int) bool {
					return batches[i][0] < batches[j][0]
				})

				fileInfo := result.Batches[fmt.Sprintf("%s-%d", LIVE_FILE_NAME_1, batches[len(batches)-1][0])].FileInfo
				l.Info(
					"Test_Local_File_ProcessBatchRequestWorkflow - time taken",
					slog.Any("time-taken", fmt.Sprintf("%dms", timeTaken.Milliseconds())),
					slog.Any("offsets", fileInfo.OffSets),
					slog.Any("batches", batches),
					slog.Any("record-count", recordCount))
				s.True(recordCount == 25, "record count should be 25")
				s.True(len(batches) == 9, "batch count should be 9")
			}
		}()

		s.env.ExecuteWorkflow(bo.ProcessBatchRequestWorkflow, req)

		s.True(s.env.IsWorkflowCompleted())
		s.NoError(s.env.GetWorkflowError())
	})
}

func (s *ProcessBatchRequestWorkflowTestSuite) Test_Cloud_File_ProcessBatchRequestWorkflow() {
	l := s.GetLogger()

	s.env = s.NewTestWorkflowEnvironment()

	// register workflow
	s.env.RegisterWorkflow(bo.ProcessBatchRequestWorkflow)

	// register activities
	s.env.RegisterActivityWithOptions(bo.GetCSVHeadersActivity, activity.RegisterOptions{
		Name: bo.GetCSVHeadersActivityName,
	})
	s.env.RegisterActivityWithOptions(bo.GetNextOffsetActivity, activity.RegisterOptions{
		Name: bo.GetNextOffsetActivityName,
	})
	s.env.RegisterActivityWithOptions(bo.ProcessBatchActivity, activity.RegisterOptions{
		Name: bo.ProcessBatchActivityName,
	})

	testCfg := getTestConfig()

	// create file client
	cscCfg := clients.CloudStorageClientConfig{
		CredsPath: testCfg.credsPath,
	}
	fileClient, err := clients.NewCloudCSVFileClient(cscCfg)
	s.NoError(err)
	defer func() {
		err := fileClient.Close()
		s.NoError(err)
	}()

	ctx := context.WithValue(context.Background(), bo.ReaderClientContextKey, fileClient)
	s.env.SetWorkerOptions(worker.Options{
		BackgroundActivityContext: ctx,
	})

	s.env.SetTestTimeout(24 * time.Hour)

	start := time.Now()

	req := &bo.BatchRequest{
		MaxBatches: 2,
		BatchSize:  400,
		Source: &bo.FileSource{
			FileName: LIVE_FILE_NAME_1,
			FilePath: testCfg.filePath,
			Bucket:   testCfg.bucket,
		},
	}

	expectedCall := []string{
		bo.GetCSVHeadersActivityName,
		bo.GetNextOffsetActivityName,
		bo.ProcessBatchActivityName,
	}

	var activityCalled []string
	s.env.SetOnActivityStartedListener(func(activityInfo *activity.Info, ctx context.Context, args converter.EncodedValues) {
		activityType := activityInfo.ActivityType.Name
		if strings.HasPrefix(activityType, "internalSession") {
			return
		}
		activityCalled = append(activityCalled, activityType)
		// var lastOffset int64
		switch activityType {
		case expectedCall[0]:
			// get headers
			var input bo.FileInfo
			s.NoError(args.Get(&input))
			s.Equal(req.Source.FileName, input.FileName)
		case expectedCall[1]:
			// next offset
			var input bo.FileInfo
			s.NoError(args.Get(&input))
			s.Equal(req.Source.FileName, input.FileName)
		case expectedCall[2]:
			// process batch
			var input bo.Batch
			s.NoError(args.Get(&input))
			s.Equal(req.Source.FileName, input.FileInfo.FileName)
			// s.Equal(input.End, lastOffset)
		default:
			panic("Test_Cloud_File_ProcessBatchRequestWorkflow - unexpected activity call")
		}
	})

	defer func() {
		if err := recover(); err != nil {
			l.Error(
				"Test_Cloud_File_ProcessBatchRequestWorkflow - panicked",
				slog.Any("error", err),
				slog.String("wkfl", bo.ProcessBatchRequestWorkflowName))
		}

		err := s.env.GetWorkflowError()
		if err != nil {
			l.Error("Test_Cloud_File_ProcessBatchRequestWorkflow - error", slog.Any("error", err))
		} else {
			var result bo.BatchRequest
			s.env.GetWorkflowResult(&result)

			timeTaken := time.Since(start)
			batches := [][]int64{}
			recordCount := 0
			for _, v := range result.Batches {
				batches = append(batches, []int64{v.Start, v.End, int64(len(v.Records))})
				recordCount += len(v.Records)
			}
			sort.SliceStable(batches, func(i, j int) bool {
				return batches[i][0] < batches[j][0]
			})

			fileInfo := result.Batches[fmt.Sprintf("%s-%d", LIVE_FILE_NAME_1, batches[len(batches)-1][0])].FileInfo
			l.Info(
				"Test_Cloud_File_ProcessBatchRequestWorkflow - time taken",
				slog.Any("time-taken", fmt.Sprintf("%dms", timeTaken.Milliseconds())),
				slog.Any("offsets", fileInfo.OffSets),
				slog.Any("batches", batches),
				slog.Any("record-count", recordCount))
			s.True(recordCount == 25, "record count should be 25")
			s.True(len(batches) == 9, "batch count should be 9")
		}
	}()

	s.env.ExecuteWorkflow(bo.ProcessBatchRequestWorkflow, req)

	s.True(s.env.IsWorkflowCompleted())
	s.NoError(s.env.GetWorkflowError())
}

func (s *ProcessBatchRequestWorkflowTestSuite) Test_DB_File_ProcessBatchRequestWorkflow() {
	l := s.GetLogger()

	s.env = s.NewTestWorkflowEnvironment()

	// register workflow
	s.env.RegisterWorkflow(bo.ProcessBatchRequestWorkflow)

	// register activities
	s.env.RegisterActivityWithOptions(bo.GetNextOffsetActivity, activity.RegisterOptions{
		Name: bo.GetNextOffsetActivityName,
	})
	s.env.RegisterActivityWithOptions(bo.ProcessBatchActivity, activity.RegisterOptions{
		Name: bo.ProcessBatchActivityName,
	})

	// setup db client
	dbClient, err := getTestDBClient()
	s.NoError(err)

	// insert 5 dummy records into db
	err = insertAgentRecords(dbClient)
	s.NoError(err)

	ctx := context.WithValue(context.Background(), bo.ReaderClientContextKey, dbClient)
	s.env.SetWorkerOptions(worker.Options{
		BackgroundActivityContext: ctx,
	})

	s.env.SetTestTimeout(24 * time.Hour)

	start := time.Now()

	req := &bo.BatchRequest{
		MaxBatches: 2,
		BatchSize:  2,
		Source: &bo.FileSource{
			FileName: TABLE_NAME_1,
		},
	}

	expectedCall := []string{
		bo.GetNextOffsetActivityName,
		bo.ProcessBatchActivityName,
	}

	var activityCalled []string
	s.env.SetOnActivityStartedListener(func(activityInfo *activity.Info, ctx context.Context, args converter.EncodedValues) {
		activityType := activityInfo.ActivityType.Name
		if strings.HasPrefix(activityType, "internalSession") {
			return
		}
		activityCalled = append(activityCalled, activityType)
		// var lastOffset int64
		switch activityType {
		case expectedCall[0]:
			// next offset
			var input bo.FileInfo
			s.NoError(args.Get(&input))
			s.Equal(req.Source.FileName, input.FileName)
		case expectedCall[1]:
			// process batch
			var input bo.Batch
			s.NoError(args.Get(&input))
			s.Equal(req.Source.FileName, input.FileInfo.FileName)
			// s.Equal(input.End, lastOffset)
		default:
			panic("Test_DB_File_ProcessBatchRequestWorkflow - unexpected activity call")
		}
	})

	defer func() {
		if err := recover(); err != nil {
			l.Error(
				"Test_DB_File_ProcessBatchRequestWorkflow - panicked",
				slog.Any("error", err),
				slog.String("wkfl", bo.ProcessBatchRequestWorkflowName))
		}

		err := s.env.GetWorkflowError()
		if err != nil {
			l.Error("Test_DB_File_ProcessBatchRequestWorkflow - error", slog.Any("error", err))
		} else {
			var result bo.BatchRequest
			s.env.GetWorkflowResult(&result)

			timeTaken := time.Since(start)
			batches := [][]int64{}
			recordCount := 0
			for _, v := range result.Batches {
				batches = append(batches, []int64{v.Start, v.End, int64(len(v.Records))})
				recordCount += len(v.Records)
			}
			sort.SliceStable(batches, func(i, j int) bool {
				return batches[i][0] < batches[j][0]
			})

			fileInfo := result.Batches[fmt.Sprintf("%s-%d", TABLE_NAME_1, batches[len(batches)-1][0])].FileInfo
			l.Info(
				"Test_DB_File_ProcessBatchRequestWorkflow - time taken",
				slog.Any("time-taken", fmt.Sprintf("%dms", timeTaken.Milliseconds())),
				slog.Any("offsets", fileInfo.OffSets),
				slog.Any("batches", batches),
				slog.Any("record-count", recordCount),
			)
			s.True(recordCount == 5, "record count should be 5")
			s.True(len(batches) == 3, "batch count should be 3")
		}
	}()

	s.env.ExecuteWorkflow(bo.ProcessBatchRequestWorkflow, req)

	s.True(s.env.IsWorkflowCompleted())
	s.NoError(s.env.GetWorkflowError())
}
