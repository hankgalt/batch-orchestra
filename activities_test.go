package batch_orchestra_test

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"testing"

	bo "github.com/hankgalt/batch-orchestra"
	"github.com/hankgalt/batch-orchestra/clients"
	"github.com/stretchr/testify/suite"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/worker"
)

const (
	TABLE_NAME_1     string = "agent"
	LIVE_FILE_NAME_1 string = "Agents-sm.csv"
)

const (
	ActivityAlias              string = "some-random-activity-alias"
	GetCSVHeadersActivityAlias string = "get-csv-headers-activity-alias"
	DeleteFileActivityAlias    string = "delete-file-activity-alias"
	GetNextOffsetActivityAlias string = "getNext-offset-activity-alias"
	ProcessBatchActivityAlias  string = "process-batch-activity-alias"
)

type BatchActivitiesTestSuite struct {
	suite.Suite
	testsuite.WorkflowTestSuite
}

func TestCSVBatchActivitiesTestSuite(t *testing.T) {
	suite.Run(t, new(BatchActivitiesTestSuite))
}

func (s *BatchActivitiesTestSuite) SetupTest() {
	// get test logger
	l := getTestLogger()

	// set environment logger
	s.SetLogger(l)
}

func (s *BatchActivitiesTestSuite) Test_LocalActivity() {
	localActivityFn := func(ctx context.Context, name string) (string, error) {
		return "hello " + name, nil
	}

	env := s.NewTestActivityEnvironment()
	result, err := env.ExecuteLocalActivity(localActivityFn, "local_activity")
	s.NoError(err)
	var laResult string
	err = result.Get(&laResult)
	s.NoError(err)
	fmt.Println("Test_LocalActivity - local activity result: ", laResult)
	s.Equal("hello local_activity", laResult)
}

func (s *BatchActivitiesTestSuite) Test_ActivityRegistration() {
	activityFn := func(msg string) (string, error) {
		return msg, nil
	}

	env := s.NewTestActivityEnvironment()
	env.RegisterActivityWithOptions(activityFn, activity.RegisterOptions{Name: ActivityAlias})
	input := "some random input"

	encodedValue, err := env.ExecuteActivity(activityFn, input)
	s.NoError(err)
	output := ""
	encodedValue.Get(&output)
	s.Equal(input, output)

	encodedValue, err = env.ExecuteActivity(ActivityAlias, input)
	s.NoError(err)
	output = ""
	encodedValue.Get(&output)
	fmt.Println("Test_ActivityRegistration - output: ", output)
	s.Equal(input, output)
}

func (s *BatchActivitiesTestSuite) Test_Local_File_GetCSVHeadersActivity() {
	l := s.GetLogger()

	// get test environment
	env := s.NewTestActivityEnvironment()

	// register GetCSVHeadersActivity
	env.RegisterActivityWithOptions(bo.GetCSVHeadersActivity, activity.RegisterOptions{Name: GetCSVHeadersActivityAlias})

	// create file client
	fileClient := &clients.LocalCSVFileClient{}

	// set reader client in request context
	ctx := context.WithValue(context.Background(), bo.ReaderClientContextKey, fileClient)
	env.SetWorkerOptions(worker.Options{
		BackgroundActivityContext: ctx,
	})

	testCfg := getTestConfig()

	batchSize := int64(400)

	s.Run("valid file", func() {
		filePath := fmt.Sprintf("%s/%s", testCfg.dir, testCfg.filePath)
		req := &bo.FileInfo{
			FileSource: bo.FileSource{
				FileName: LIVE_FILE_NAME_1,
				FilePath: filePath,
			},
			FileType: bo.CSV,
		}
		s.Equal(req.Start, int64(0))

		// test get csv file headers
		result, err := s.testGetCSVHeadersActivity(env, req, batchSize)
		s.NoError(err)
		l.Debug("Test_Local_File_GetCSVHeadersActivity - get csv headers result", slog.Any("result", result))
		s.Equal(result.Headers, []string{"ENTITY_NAME", "ENTITY_NUM", "ORG_NAME", "FIRST_NAME", "MIDDLE_NAME", "LAST_NAME", "PHYSICAL_ADDRESS", "AGENT_TYPE"})
		s.Equal(result.Start, int64(93))
	})

	s.Run("missing file name", func() {
		filePath := fmt.Sprintf("%s/%s", testCfg.dir, testCfg.filePath)
		req := &bo.FileInfo{
			FileSource: bo.FileSource{
				FileName: "",
				FilePath: filePath,
			},
			FileType: bo.CSV,
		}
		s.Equal(req.Start, int64(0))

		// test get csv file headers
		_, err := s.testGetCSVHeadersActivity(env, req, batchSize)
		s.Error(err)
		l.Debug("Test_Local_File_GetCSVHeadersActivity - get csv headers error", slog.Any("error", err))

		var applicationErr *temporal.ApplicationError
		isAppErr := errors.As(err, &applicationErr)
		s.Equal(isAppErr, true)
		s.Equal(applicationErr.Type(), bo.ERR_MISSING_FILE_NAME)
	})
}

func (s *BatchActivitiesTestSuite) Test_Cloud_File_GetCSVHeadersActivity() {
	l := s.GetLogger()

	// get test environment
	env := s.NewTestActivityEnvironment()

	// register GetCSVHeadersActivity
	env.RegisterActivityWithOptions(bo.GetCSVHeadersActivity, activity.RegisterOptions{Name: GetCSVHeadersActivityAlias})

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

	// set reader client in request context
	ctx := context.WithValue(context.Background(), bo.ReaderClientContextKey, fileClient)
	env.SetWorkerOptions(worker.Options{
		BackgroundActivityContext: ctx,
	})

	batchSize := int64(400)

	s.Run("valid file", func() {
		req := &bo.FileInfo{
			FileSource: bo.FileSource{
				FileName: LIVE_FILE_NAME_1,
				FilePath: testCfg.filePath,
				Bucket:   testCfg.bucket,
			},
			FileType: bo.CLOUD_CSV,
		}
		s.Equal(req.Start, int64(0))

		// test get csv file headers
		result, err := s.testGetCSVHeadersActivity(env, req, batchSize)
		s.NoError(err)
		l.Debug("Test_Cloud_File_GetCSVHeadersActivity - get csv headers result", slog.Any("result", result))
		s.Equal(result.Headers, []string{"ENTITY_NAME", "ENTITY_NUM", "ORG_NAME", "FIRST_NAME", "MIDDLE_NAME", "LAST_NAME", "PHYSICAL_ADDRESS", "AGENT_TYPE"})
		s.Equal(result.Start, int64(93))
	})

	s.Run("missing file name", func() {
		req := &bo.FileInfo{
			FileSource: bo.FileSource{
				FileName: "",
				FilePath: testCfg.filePath,
				Bucket:   testCfg.bucket,
			},
			FileType: bo.CLOUD_CSV,
		}
		s.Equal(req.Start, int64(0))

		// test get csv file headers
		_, err := s.testGetCSVHeadersActivity(env, req, batchSize)
		s.Error(err)
		l.Debug("Test_Cloud_File_GetCSVHeadersActivity - get csv headers error", slog.Any("error", err))

		var applicationErr *temporal.ApplicationError
		isAppErr := errors.As(err, &applicationErr)
		s.Equal(isAppErr, true)
		s.Equal(applicationErr.Type(), bo.ERR_MISSING_FILE_NAME)
	})

	s.Run("missing cloud bucket", func() {
		req := &bo.FileInfo{
			FileSource: bo.FileSource{
				FileName: LIVE_FILE_NAME_1,
				FilePath: testCfg.filePath,
				Bucket:   "",
			},
			FileType: bo.CLOUD_CSV,
		}
		s.Equal(req.Start, int64(0))

		// test get csv file headers
		_, err := s.testGetCSVHeadersActivity(env, req, batchSize)
		s.Error(err)
		l.Debug("Test_Cloud_File_GetCSVHeadersActivity - get csv headers error", slog.Any("error", err))

		var applicationErr *temporal.ApplicationError
		isAppErr := errors.As(err, &applicationErr)
		s.Equal(isAppErr, true)
		s.Equal(applicationErr.Type(), bo.ERR_MISSING_CLOUD_BUCKET)
	})
}

func (s *BatchActivitiesTestSuite) Test_Mock_Client_GetCSVHeadersActivity() {
	l := s.GetLogger()

	// get test environment
	env := s.NewTestActivityEnvironment()

	// register GetCSVHeadersActivity
	env.RegisterActivityWithOptions(bo.GetCSVHeadersActivity, activity.RegisterOptions{Name: GetCSVHeadersActivityAlias})

	testCfg := getTestConfig()

	// create file client
	fileClient := getFileClientMock()

	// set reader client in request context
	ctx := context.WithValue(context.Background(), bo.ReaderClientContextKey, fileClient)
	env.SetWorkerOptions(worker.Options{
		BackgroundActivityContext: ctx,
	})

	filePath := fmt.Sprintf("%s/%s", testCfg.dir, testCfg.filePath)
	req := &bo.FileInfo{
		FileSource: bo.FileSource{
			FileName: LIVE_FILE_NAME_1,
			FilePath: filePath,
		},
		FileType: bo.CSV,
	}
	s.Equal(req.Start, int64(0))

	batchSize := int64(400)

	// test get csv file headers
	result, err := s.testGetCSVHeadersActivity(env, req, batchSize)
	s.NoError(err)
	l.Debug("Test_Mock_Client_GetCSVHeadersActivity - get csv headers result", slog.Any("result", result))
	s.Equal(result.Headers, []string{"ENTITY_NAME", "ENTITY_NUM", "ORG_NAME", "FIRST_NAME", "MIDDLE_NAME", "LAST_NAME", "PHYSICAL_ADDRESS", "AGENT_TYPE"})
	s.Equal(result.Start, int64(93))
}

func (s *BatchActivitiesTestSuite) testGetCSVHeadersActivity(env *testsuite.TestActivityEnvironment, req *bo.FileInfo, batchSize int64) (*bo.FileInfo, error) {
	resp, err := env.ExecuteActivity(bo.GetCSVHeadersActivity, req, batchSize)
	if err != nil {
		return nil, err
	}

	var result bo.FileInfo
	err = resp.Get(&result)
	if err != nil {
		return nil, err
	}
	return &result, nil
}

func (s *BatchActivitiesTestSuite) Test_Local_File_GetNextOffsetActivity() {
	l := s.GetLogger()

	// get test environment
	env := s.NewTestActivityEnvironment()

	// register GetCSVHeadersActivity & GetNextOffsetActivity
	env.RegisterActivityWithOptions(bo.GetCSVHeadersActivity, activity.RegisterOptions{Name: GetCSVHeadersActivityAlias})
	env.RegisterActivityWithOptions(bo.GetNextOffsetActivity, activity.RegisterOptions{Name: GetNextOffsetActivityAlias})

	// create file client
	fileClient := &clients.LocalCSVFileClient{}

	// set reader client in request context
	ctx := context.WithValue(context.Background(), bo.ReaderClientContextKey, fileClient)
	env.SetWorkerOptions(worker.Options{
		BackgroundActivityContext: ctx,
	})

	testCfg := getTestConfig()
	batchSize := int64(400)

	s.Run("valid file", func() {
		filePath := fmt.Sprintf("%s/%s", testCfg.dir, testCfg.filePath)
		req := &bo.FileInfo{
			FileSource: bo.FileSource{
				FileName: LIVE_FILE_NAME_1,
				FilePath: filePath,
			},
			FileType: bo.CSV,
		}
		s.Equal(req.Start, int64(0))

		// test get csv file headers
		result, err := s.testGetCSVHeadersActivity(env, req, batchSize)
		s.NoError(err)
		l.Debug("Test_Local_File_GetNextOffsetActivity - get csv headers result", slog.Any("result", result))
		s.Equal(result.Headers, []string{"ENTITY_NAME", "ENTITY_NUM", "ORG_NAME", "FIRST_NAME", "MIDDLE_NAME", "LAST_NAME", "PHYSICAL_ADDRESS", "AGENT_TYPE"})
		s.Equal(result.Start, int64(93))

		i := 0
		for i <= 12 {
			i++
			// test get next offset
			result, err = s.testGetNextOffsetActivity(env, result, batchSize)
			s.NoError(err)
			l.Debug("Test_Local_File_GetNextOffsetActivity - get next offset result", slog.Any("result", result), slog.Any("count", i))
			if result.End > 0 {
				break
			}
		}

		s.Equal(i, len(result.OffSets)-1)
		s.Equal(result.End, result.OffSets[len(result.OffSets)-1])
	})
}

func (s *BatchActivitiesTestSuite) Test_Cloud_File_GetNextOffsetActivity() {
	l := s.GetLogger()

	// get test environment
	env := s.NewTestActivityEnvironment()

	// register GetCSVHeadersActivity & GetNextOffsetActivity
	env.RegisterActivityWithOptions(bo.GetCSVHeadersActivity, activity.RegisterOptions{Name: GetCSVHeadersActivityAlias})
	env.RegisterActivityWithOptions(bo.GetNextOffsetActivity, activity.RegisterOptions{Name: GetNextOffsetActivityAlias})

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

	// set reader client in request context
	ctx := context.WithValue(context.Background(), bo.ReaderClientContextKey, fileClient)
	env.SetWorkerOptions(worker.Options{
		BackgroundActivityContext: ctx,
	})

	batchSize := int64(400)

	s.Run("valid file", func() {
		req := &bo.FileInfo{
			FileSource: bo.FileSource{
				FileName: LIVE_FILE_NAME_1,
				FilePath: testCfg.filePath,
				Bucket:   testCfg.bucket,
			},
			FileType: bo.CLOUD_CSV,
		}
		s.Equal(req.Start, int64(0))

		// get csv file headers
		result, err := s.testGetCSVHeadersActivity(env, req, batchSize)
		s.NoError(err)
		l.Debug("Test_Cloud_File_GetNextOffsetActivity - get csv headers result", slog.Any("result", result))
		s.Equal(result.Headers, []string{"ENTITY_NAME", "ENTITY_NUM", "ORG_NAME", "FIRST_NAME", "MIDDLE_NAME", "LAST_NAME", "PHYSICAL_ADDRESS", "AGENT_TYPE"})
		s.Equal(result.Start, int64(93))

		i := 0
		for i <= 12 {
			i++
			// get next offset
			result, err = s.testGetNextOffsetActivity(env, result, batchSize)
			s.NoError(err)
			l.Debug("Test_Cloud_File_GetNextOffsetActivity - get next offset result", slog.Any("result", result), slog.Any("count", i))
			if result.End > 0 {
				break
			}
		}

		s.Equal(i, len(result.OffSets)-1)
		s.Equal(result.End, result.OffSets[len(result.OffSets)-1])
	})
}

func (s *BatchActivitiesTestSuite) Test_DB_File_GetNextOffsetActivity() {
	l := s.GetLogger()

	// get test environment
	env := s.NewTestActivityEnvironment()

	// register GetNextOffsetActivity
	env.RegisterActivityWithOptions(bo.GetNextOffsetActivity, activity.RegisterOptions{Name: GetNextOffsetActivityAlias})

	// setup db client
	dbClient, err := getTestDBClient()
	s.NoError(err)

	// insert 5 dummy records into db
	err = insertAgentRecords(dbClient)
	s.NoError(err)

	// set reader client in request context
	ctx := context.WithValue(context.Background(), bo.ReaderClientContextKey, dbClient)
	env.SetWorkerOptions(worker.Options{
		BackgroundActivityContext: ctx,
	})

	batchSize := int64(2)

	s.Run("valid file", func() {
		req := &bo.FileInfo{
			FileSource: bo.FileSource{
				FileName: TABLE_NAME_1,
			},
			FileType: bo.DB_CURSOR,
		}
		s.Equal(req.Start, int64(0))

		i := 0
		result, err := s.testGetNextOffsetActivity(env, req, batchSize)
		s.NoError(err)
		l.Debug("Test_DB_File_GetNextOffsetActivity - get next offset result", slog.Any("result", result))
		i++
		for i <= 5 {
			i++
			// test get next offset
			result, err = s.testGetNextOffsetActivity(env, result, batchSize)
			s.NoError(err)
			l.Debug("Test_DB_File_GetNextOffsetActivity - get next offset result", slog.Any("result", result))
			if result.End > 0 {
				break
			}
		}

		s.Equal(i, len(result.OffSets)-1)
		s.Equal(result.End, result.OffSets[len(result.OffSets)-1])
	})
}

func (s *BatchActivitiesTestSuite) Test_Mock_Client_GetNextOffsetActivity() {
	l := s.GetLogger()

	// get test environment
	env := s.NewTestActivityEnvironment()

	// register GetCSVHeadersActivity & GetNextOffsetActivity
	env.RegisterActivityWithOptions(bo.GetCSVHeadersActivity, activity.RegisterOptions{Name: GetCSVHeadersActivityAlias})
	env.RegisterActivityWithOptions(bo.GetNextOffsetActivity, activity.RegisterOptions{Name: GetNextOffsetActivityAlias})

	// create file client
	fileClient := getFileClientMock()

	testCfg := getTestConfig()

	// set reader client in request context
	ctx := context.WithValue(context.Background(), bo.ReaderClientContextKey, fileClient)
	env.SetWorkerOptions(worker.Options{
		BackgroundActivityContext: ctx,
	})

	filePath := fmt.Sprintf("%s/%s", testCfg.dir, testCfg.filePath)
	req := &bo.FileInfo{
		FileSource: bo.FileSource{
			FileName: LIVE_FILE_NAME_1,
			FilePath: filePath,
		},
		FileType: bo.CSV,
	}
	s.Equal(req.Start, int64(0))

	batchSize := int64(400)

	// test get csv file headers
	result, err := s.testGetCSVHeadersActivity(env, req, batchSize)
	s.NoError(err)
	l.Debug("Test_Mock_Client_GetNextOffsetActivity - get csv headers result", slog.Any("result", result))
	s.Equal(result.Headers, []string{"ENTITY_NAME", "ENTITY_NUM", "ORG_NAME", "FIRST_NAME", "MIDDLE_NAME", "LAST_NAME", "PHYSICAL_ADDRESS", "AGENT_TYPE"})
	s.Equal(result.Start, int64(93))

	i := 0
	for i <= 5 {
		i++
		// test get next offset
		result, err = s.testGetNextOffsetActivity(env, result, batchSize)
		s.NoError(err)
		l.Debug("Test_Mock_Client_GetNextOffsetActivity - get next offset result", slog.Any("result", result), slog.Any("count", i))
		if result.End > 0 {
			break
		}
	}

	s.Equal(i, len(result.OffSets)-1)
	s.Equal(result.End, result.OffSets[len(result.OffSets)-1])
}

func (s *BatchActivitiesTestSuite) testGetNextOffsetActivity(env *testsuite.TestActivityEnvironment, req *bo.FileInfo, batchSize int64) (*bo.FileInfo, error) {
	resp, err := env.ExecuteActivity(bo.GetNextOffsetActivity, req, batchSize)
	if err != nil {
		return nil, err
	}

	var result bo.FileInfo
	err = resp.Get(&result)
	if err != nil {
		return nil, err
	}
	return &result, nil
}

func (s *BatchActivitiesTestSuite) Test_Local_File_ProcessBatchActivity() {
	l := s.GetLogger()

	// get test environment
	env := s.NewTestActivityEnvironment()

	// register GetCSVHeadersActivity, GetNextOffsetActivity & ProcessBatchActivity
	env.RegisterActivityWithOptions(bo.GetCSVHeadersActivity, activity.RegisterOptions{Name: GetCSVHeadersActivityAlias})
	env.RegisterActivityWithOptions(bo.GetNextOffsetActivity, activity.RegisterOptions{Name: GetNextOffsetActivityAlias})
	env.RegisterActivityWithOptions(bo.ProcessBatchActivity, activity.RegisterOptions{Name: ProcessBatchActivityAlias})

	// create file client
	fileClient := &clients.LocalCSVFileClient{}

	// set reader client in request context
	ctx := context.WithValue(context.Background(), bo.ReaderClientContextKey, fileClient)
	env.SetWorkerOptions(worker.Options{
		BackgroundActivityContext: ctx,
	})

	batchSize := int64(400)

	testCfg := getTestConfig()

	s.Run("valid file", func() {
		filePath := fmt.Sprintf("%s/%s", testCfg.dir, testCfg.filePath)
		req := &bo.FileInfo{
			FileSource: bo.FileSource{
				FileName: LIVE_FILE_NAME_1,
				FilePath: filePath,
			},
			FileType: bo.CSV,
		}
		s.Equal(req.Start, int64(0))

		// test get csv file headers
		result, err := s.testGetCSVHeadersActivity(env, req, batchSize)
		s.NoError(err)
		l.Debug("Test_Local_File_ProcessBatchActivity - get csv headers result", slog.Any("result", result))
		s.Equal(result.Headers, []string{"ENTITY_NAME", "ENTITY_NUM", "ORG_NAME", "FIRST_NAME", "MIDDLE_NAME", "LAST_NAME", "PHYSICAL_ADDRESS", "AGENT_TYPE"})
		s.Equal(result.Start, int64(93))

		// get next offset
		result, err = s.testGetNextOffsetActivity(env, result, batchSize)
		s.NoError(err)
		l.Debug("Test_Local_File_ProcessBatchActivity - get next offset result", slog.Any("result", result))

		// process batch
		batchReq := &bo.Batch{
			FileInfo: result,
			Start:    result.OffSets[len(result.OffSets)-2],
			End:      result.OffSets[len(result.OffSets)-1],
		}

		batchRes, err := s.testProcessBatchActivity(env, batchReq)
		s.NoError(err)
		l.Debug("Test_Local_File_ProcessBatchActivity - process batch result", slog.Any("result", batchRes))
		s.Equal(3, len(batchRes.Records))
	})
}

func (s *BatchActivitiesTestSuite) Test_Cloud_File_ProcessBatchActivity() {
	l := s.GetLogger()

	// get test environment
	env := s.NewTestActivityEnvironment()

	// register GetCSVHeadersActivity, GetNextOffsetActivity & ProcessBatchActivity
	env.RegisterActivityWithOptions(bo.GetCSVHeadersActivity, activity.RegisterOptions{Name: GetCSVHeadersActivityAlias})
	env.RegisterActivityWithOptions(bo.GetNextOffsetActivity, activity.RegisterOptions{Name: GetNextOffsetActivityAlias})
	env.RegisterActivityWithOptions(bo.ProcessBatchActivity, activity.RegisterOptions{Name: ProcessBatchActivityAlias})

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

	// set reader client in request context
	ctx := context.WithValue(context.Background(), bo.ReaderClientContextKey, fileClient)
	env.SetWorkerOptions(worker.Options{
		BackgroundActivityContext: ctx,
	})

	batchSize := int64(400)

	s.Run("valid file", func() {
		req := &bo.FileInfo{
			FileSource: bo.FileSource{
				FileName: LIVE_FILE_NAME_1,
				FilePath: testCfg.filePath,
				Bucket:   testCfg.bucket,
			},
			FileType: bo.CLOUD_CSV,
		}
		s.Equal(req.Start, int64(0))

		// test get csv file headers
		result, err := s.testGetCSVHeadersActivity(env, req, batchSize)
		s.NoError(err)
		l.Debug("Test_Cloud_File_ProcessBatchActivity - get csv headers result", slog.Any("result", result))
		s.Equal(result.Headers, []string{"ENTITY_NAME", "ENTITY_NUM", "ORG_NAME", "FIRST_NAME", "MIDDLE_NAME", "LAST_NAME", "PHYSICAL_ADDRESS", "AGENT_TYPE"})
		s.Equal(result.Start, int64(93))

		// get next offset
		result, err = s.testGetNextOffsetActivity(env, result, batchSize)
		s.NoError(err)
		l.Debug("Test_Cloud_File_ProcessBatchActivity - get next offset result", slog.Any("result", result))

		// process batch
		batchReq := &bo.Batch{
			FileInfo: result,
			Start:    result.OffSets[len(result.OffSets)-2],
			End:      result.OffSets[len(result.OffSets)-1],
		}

		batchRes, err := s.testProcessBatchActivity(env, batchReq)
		s.NoError(err)
		l.Debug("Test_Cloud_File_ProcessBatchActivity - process batch result", slog.Any("result", batchRes))
		s.Equal(3, len(batchRes.Records))
	})
}

func (s *BatchActivitiesTestSuite) Test_DB_File_ProcessBatchActivity() {
	l := s.GetLogger()

	// get test environment
	env := s.NewTestActivityEnvironment()

	// register GetCSVHeadersActivity, GetNextOffsetActivity & ProcessBatchActivity
	env.RegisterActivityWithOptions(bo.GetCSVHeadersActivity, activity.RegisterOptions{Name: GetCSVHeadersActivityAlias})
	env.RegisterActivityWithOptions(bo.GetNextOffsetActivity, activity.RegisterOptions{Name: GetNextOffsetActivityAlias})
	env.RegisterActivityWithOptions(bo.ProcessBatchActivity, activity.RegisterOptions{Name: ProcessBatchActivityAlias})

	// setup db client
	dbClient, err := getTestDBClient()
	s.NoError(err)

	// insert 5 dummy records into db
	err = insertAgentRecords(dbClient)
	s.NoError(err)

	// set reader client in request context
	ctx := context.WithValue(context.Background(), bo.ReaderClientContextKey, dbClient)
	env.SetWorkerOptions(worker.Options{
		BackgroundActivityContext: ctx,
	})

	batchSize := int64(2)

	s.Run("valid file", func() {
		req := &bo.FileInfo{
			FileSource: bo.FileSource{
				FileName: TABLE_NAME_1,
			},
			FileType: bo.DB_CURSOR,
		}
		s.Equal(req.Start, int64(0))

		// get next offset
		result, err := s.testGetNextOffsetActivity(env, req, batchSize)
		s.NoError(err)
		l.Debug("Test_DB_File_ProcessBatchActivity - get next offset result", slog.Any("result", result))

		// process batch
		batchReq := &bo.Batch{
			FileInfo: result,
			Start:    result.OffSets[len(result.OffSets)-2],
			End:      result.OffSets[len(result.OffSets)-1],
		}

		batchRes, err := s.testProcessBatchActivity(env, batchReq)
		s.NoError(err)
		l.Debug("Test_DB_File_ProcessBatchActivity - process batch result", slog.Any("result", batchRes))
		s.Equal(2, len(batchRes.Records))
	})
}

func (s *BatchActivitiesTestSuite) Test_Mock_Client_ProcessBatchActivity() {
	l := s.GetLogger()

	// get test environment
	env := s.NewTestActivityEnvironment()

	// register GetCSVHeadersActivity, GetNextOffsetActivity & ProcessBatchActivity
	env.RegisterActivityWithOptions(bo.GetCSVHeadersActivity, activity.RegisterOptions{Name: GetCSVHeadersActivityAlias})
	env.RegisterActivityWithOptions(bo.GetNextOffsetActivity, activity.RegisterOptions{Name: GetNextOffsetActivityAlias})
	env.RegisterActivityWithOptions(bo.ProcessBatchActivity, activity.RegisterOptions{Name: ProcessBatchActivityAlias})

	// create file client
	fileClient := getFileClientMock()

	testCfg := getTestConfig()

	// set reader client in request context
	ctx := context.WithValue(context.Background(), bo.ReaderClientContextKey, fileClient)
	env.SetWorkerOptions(worker.Options{
		BackgroundActivityContext: ctx,
	})

	filePath := fmt.Sprintf("%s/%s", testCfg.dir, testCfg.filePath)
	req := &bo.FileInfo{
		FileSource: bo.FileSource{
			FileName: LIVE_FILE_NAME_1,
			FilePath: filePath,
		},
		FileType: bo.CSV,
	}
	s.Equal(req.Start, int64(0))

	batchSize := int64(400)

	// test get csv file headers
	result, err := s.testGetCSVHeadersActivity(env, req, batchSize)
	s.NoError(err)
	l.Debug("Test_Mock_Client_ProcessBatchActivity - get csv headers result", slog.Any("result", result))
	s.Equal(result.Headers, []string{"ENTITY_NAME", "ENTITY_NUM", "ORG_NAME", "FIRST_NAME", "MIDDLE_NAME", "LAST_NAME", "PHYSICAL_ADDRESS", "AGENT_TYPE"})
	s.Equal(result.Start, int64(93))

	// get next offset
	result, err = s.testGetNextOffsetActivity(env, result, batchSize)
	s.NoError(err)
	l.Debug("Test_Mock_Client_ProcessBatchActivity - get next offset result", slog.Any("result", result))

	// process batch
	batchReq := &bo.Batch{
		FileInfo: result,
		Start:    result.OffSets[len(result.OffSets)-2],
		End:      result.OffSets[len(result.OffSets)-1],
	}

	batchRes, err := s.testProcessBatchActivity(env, batchReq)
	s.NoError(err)
	l.Debug("Test_Mock_Client_ProcessBatchActivity - process batch result", slog.Any("result", batchRes))
	// s.Equal(3, len(batchRes.Records))
}

func (s *BatchActivitiesTestSuite) testProcessBatchActivity(env *testsuite.TestActivityEnvironment, req *bo.Batch) (*bo.Batch, error) {
	resp, err := env.ExecuteActivity(bo.ProcessBatchActivity, req)
	if err != nil {
		return nil, err
	}

	var result bo.Batch
	err = resp.Get(&result)
	if err != nil {
		return nil, err
	}
	return &result, nil
}

func getTestLogger() *slog.Logger {
	// create log level var
	logLevel := &slog.LevelVar{}
	// set log level
	logLevel.Set(slog.LevelDebug)
	// create log handler options
	opts := &slog.HandlerOptions{
		Level: logLevel,
	}
	// create log handler
	handler := slog.NewJSONHandler(os.Stdout, opts)
	// create logger
	l := slog.New(handler)
	// set default logger
	slog.SetDefault(l)

	return l
}

type testConfig struct {
	dir       string
	bucket    string
	credsPath string
	filePath  string
}

func getTestConfig() testConfig {
	dataDir := os.Getenv("DATA_DIR")
	credsPath := os.Getenv("CREDS_PATH")
	bktName := os.Getenv("BUCKET_NAME")
	filePath := os.Getenv("FILE_PATH")

	return testConfig{
		dir:       dataDir,
		bucket:    bktName,
		credsPath: credsPath,
		filePath:  filePath,
	}
}

func getTestDBClient() (*clients.SQLLiteDBClient, error) {
	dbFile := "data/__deleteme.db"
	dbClient, err := clients.NewSQLLiteDBClient(dbFile)
	if err != nil {
		return nil, err
	}

	dbClient.ExecuteSchema(clients.AgentSchema)

	return dbClient, nil
}

func insertAgentRecords(dbClient *clients.SQLLiteDBClient) error {
	records := []map[string]interface{}{}
	for i := 1; i <= 5; i++ {
		records = append(records, map[string]interface{}{
			"entity_id":   i,
			"entity_name": fmt.Sprintf("entity_%d", i),
			"first_name":  fmt.Sprintf("first_%d", i),
			"last_name":   fmt.Sprintf("last_%d", i),
			"agent_type":  "individual agent",
		})
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	for _, rec := range records {
		res, err := dbClient.InsertAgentRecord(ctx, rec)
		if err != nil {
			fmt.Printf("error inserting record: %v, error: %v\n", rec, err)
			return err
		}

		_, err = res.LastInsertId()
		if err != nil {
			fmt.Printf("error record insert ID: %v, error: %v\n", rec, err)
			return err
		}
		_, err = res.RowsAffected()
		if err != nil {
			fmt.Printf("error record rows affected: %v, error: %v\n", rec, err)
			return err
		}
	}

	return nil
}

type FileClientMock struct {
	bo.ChunkReader
	bo.ChunkHandler
}

func getFileClientMock() *FileClientMock {
	return &FileClientMock{}
}

func (lcl *FileClientMock) ReadData(ctx context.Context, fileSrc bo.FileSource, offset, limit int64) (interface{}, int64, error) {
	ext := filepath.Ext(fileSrc.FileName)

	if ext == ".csv" {
		data := `ENTITY_NAME|ENTITY_NUM|ORG_NAME|FIRST_NAME|MIDDLE_NAME|LAST_NAME|PHYSICAL_ADDRESS|AGENT_TYPE
AUTHENTIC MEN|5231360|DARYL REESE LAW PC||||3843 BRICKWAY BLVD STE 204 SANTA ROSA CA  95403|Registered 1505 Agent
Clayton Care Homes LLC|202252411186||Janice||Clemons|8993 AUTUMNWOOD DR SACRAMENTO CA  95826|Individual Agent`

		if offset > 0 {
			data = `114 ADELINE STREET, LLC|202253512495||David|G.|Finkelstein|1528 S EL CAMINO REAL SUITE 306 SAN MATEO CA  94402|Individual Agent
115 27th Street LLC|202253419779||CJ||Stos|669 2ND STREET ENCINITAS CA  92024|Individual Agent
120 E. Santa Anita Ave., LLC|202253514705||Andrew|B|Crow|1880 CENTURY PARK EAST SUITE 1600 LOS ANGELES CA  90067|Individual Agent
`
		}

		n := int64(len(data))
		return []byte(data), n, nil
	}

	return nil, 0, errors.New("error undefined file type")
}

func (lcl *FileClientMock) HandleData(ctx context.Context, fileSrc bo.FileSource, start int64, data interface{}) (<-chan bo.Result, <-chan error, error) {
	// Create a channel to stream the records and errors
	recStream, errStream := make(chan bo.Result), make(chan error)

	go func() {
		defer func() {
			close(recStream)
			close(errStream)
		}()

		var size int64 = 80
		record := "Lorem ipsum dolor sit amet, consectetur adipiscing elit, sed do eiusmod tempor incididunt ut labore et dolore magna aliqua"

		ext := filepath.Ext(fileSrc.FileName)
		if ext == ".csv" {
			i := 1
			var currOffset int64 = 0
			for i <= 5 {
				recStream <- bo.Result{
					Start:  start + currOffset,
					End:    start + size,
					Result: record,
				}
				i++
			}
		}
	}()

	return recStream, errStream, nil
}
