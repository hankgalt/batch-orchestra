package batch_orchestra_test

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"testing"

	bo "github.com/hankgalt/batch-orchestra"
	"github.com/hankgalt/batch-orchestra/clients"
	"github.com/stretchr/testify/suite"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/worker"
)

const (
	TABLE_NAME_1     string = "agent"
	LIVE_FILE_NAME_1 string = "Agents-sm.csv"
)

const (
	ActivityAlias              string = "some-random-activity-alias"
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
	l := s.GetLogger()

	localActivityFn := func(ctx context.Context, name string) (string, error) {
		return "hello " + name, nil
	}

	env := s.NewTestActivityEnvironment()
	result, err := env.ExecuteLocalActivity(localActivityFn, "local_activity")
	s.NoError(err)
	var laResult string
	err = result.Get(&laResult)
	s.NoError(err)
	l.Debug("Test_LocalActivity - local activity result", slog.Any("result", laResult))
	s.Equal("hello local_activity", laResult)
}

func (s *BatchActivitiesTestSuite) Test_ActivityRegistration() {
	l := s.GetLogger()

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
	l.Debug("Test_ActivityRegistration - output: ", slog.Any("result", output))
	s.Equal(input, output)
}

func (s *BatchActivitiesTestSuite) Test_Local_File_GetNextOffsetActivity() {
	l := s.GetLogger()

	// get test environment
	env := s.NewTestActivityEnvironment()

	// register GetNextOffsetActivity
	env.RegisterActivityWithOptions(bo.GetNextOffsetActivity, activity.RegisterOptions{Name: GetNextOffsetActivityAlias})

	testCfg := getTestConfig()
	filePath := fmt.Sprintf("%s/%s", testCfg.dir, testCfg.filePath)

	// create file client
	fileClient, err := clients.NewLocalCSVFileClient(LIVE_FILE_NAME_1, filePath)
	s.NoError(err)

	// set reader client in request context
	ctx := context.WithValue(context.Background(), bo.ReaderClientContextKey, fileClient)
	env.SetWorkerOptions(worker.Options{
		BackgroundActivityContext: ctx,
	})

	batchSize := int64(400)

	s.Run("valid file", func() {
		req := &bo.FileInfo{
			FileName: LIVE_FILE_NAME_1,
		}
		s.Equal(req.Start, int64(0))

		// test get next offset
		result, err := s.testGetNextOffsetActivity(env, req, batchSize)
		s.NoError(err)

		i := 1
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

		s.Equal(i, 10)
		s.Equal(i, len(result.OffSets)-1)
		s.Equal(result.End, result.OffSets[len(result.OffSets)-1])
	})
}

func (s *BatchActivitiesTestSuite) Test_Cloud_File_GetNextOffsetActivity() {
	l := s.GetLogger()

	// get test environment
	env := s.NewTestActivityEnvironment()

	// register GetNextOffsetActivity
	env.RegisterActivityWithOptions(bo.GetNextOffsetActivity, activity.RegisterOptions{Name: GetNextOffsetActivityAlias})

	testCfg := getTestConfig()
	// create file client
	cscCfg := clients.CloudStorageClientConfig{
		CredsPath: testCfg.credsPath,
		FileName:  LIVE_FILE_NAME_1,
		FilePath:  testCfg.filePath,
		Bucket:    testCfg.bucket,
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
			FileName: LIVE_FILE_NAME_1,
		}
		s.Equal(req.Start, int64(0))

		result, err := s.testGetNextOffsetActivity(env, req, batchSize)
		s.NoError(err)

		i := 1
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

		s.Equal(i, 10)
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
			FileName: TABLE_NAME_1,
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

		s.Equal(i, 3)
		s.Equal(i, len(result.OffSets)-1)
		s.Equal(result.End, result.OffSets[len(result.OffSets)-1])
	})
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

	// register GetNextOffsetActivity & ProcessBatchActivity
	env.RegisterActivityWithOptions(bo.GetNextOffsetActivity, activity.RegisterOptions{Name: GetNextOffsetActivityAlias})
	env.RegisterActivityWithOptions(bo.ProcessBatchActivity, activity.RegisterOptions{Name: ProcessBatchActivityAlias})

	testCfg := getTestConfig()
	filePath := fmt.Sprintf("%s/%s", testCfg.dir, testCfg.filePath)

	// create file client
	fileClient, err := clients.NewLocalCSVFileClient(LIVE_FILE_NAME_1, filePath)
	s.NoError(err)

	// set reader client in request context
	ctx := context.WithValue(context.Background(), bo.ReaderClientContextKey, fileClient)
	env.SetWorkerOptions(worker.Options{
		BackgroundActivityContext: ctx,
	})

	batchSize := int64(400)

	s.Run("valid file", func() {
		req := &bo.FileInfo{
			FileName: LIVE_FILE_NAME_1,
		}
		s.Equal(req.Start, int64(0))

		// get next offset
		result, err := s.testGetNextOffsetActivity(env, req, batchSize)
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

	// register GetNextOffsetActivity & ProcessBatchActivity
	env.RegisterActivityWithOptions(bo.GetNextOffsetActivity, activity.RegisterOptions{Name: GetNextOffsetActivityAlias})
	env.RegisterActivityWithOptions(bo.ProcessBatchActivity, activity.RegisterOptions{Name: ProcessBatchActivityAlias})

	testCfg := getTestConfig()
	// create file client
	cscCfg := clients.CloudStorageClientConfig{
		CredsPath: testCfg.credsPath,
		FileName:  LIVE_FILE_NAME_1,
		FilePath:  testCfg.filePath,
		Bucket:    testCfg.bucket,
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
			FileName: LIVE_FILE_NAME_1,
		}
		s.Equal(req.Start, int64(0))

		// get next offset
		result, err := s.testGetNextOffsetActivity(env, req, batchSize)
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

	// register GetNextOffsetActivity & ProcessBatchActivity
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
			FileName: TABLE_NAME_1,
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
	logLevel.Set(slog.LevelInfo)
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
	dbClient, err := clients.NewSQLLiteDBClient(dbFile, TABLE_NAME_1)
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
