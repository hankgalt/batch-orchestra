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
	TEST_DIR         string = "data"
	DATA_PATH        string = "scheduler"
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

type CSVBatchActivitiesTestSuite struct {
	suite.Suite
	testsuite.WorkflowTestSuite
}

func TestCSVBatchActivitiesTestSuite(t *testing.T) {
	suite.Run(t, new(CSVBatchActivitiesTestSuite))
}

func (s *CSVBatchActivitiesTestSuite) Test_LocalActivity() {
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

func (s *CSVBatchActivitiesTestSuite) Test_ActivityRegistration() {
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

func (s *CSVBatchActivitiesTestSuite) Test_Local_File_GetCSVHeadersActivity() {
	// get test logger
	l := getTestLogger()

	// set environment logger
	s.SetLogger(l)

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

	filePath := fmt.Sprintf("%s/%s", TEST_DIR, DATA_PATH)
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
	l.Debug("Test_Local_File_GetCSVHeadersActivity - get csv headers result", slog.Any("result", result))
	s.Equal(result.Headers, []string{"ENTITY_NAME", "ENTITY_NUM", "ORG_NAME", "FIRST_NAME", "MIDDLE_NAME", "LAST_NAME", "PHYSICAL_ADDRESS", "AGENT_TYPE"})
	s.Equal(result.Start, int64(93))
}

func (s *CSVBatchActivitiesTestSuite) Test_Cloud_File_GetCSVHeadersActivity() {
	// get test logger
	l := getTestLogger()

	// set environment logger
	s.SetLogger(l)

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

	req := &bo.FileInfo{
		FileSource: bo.FileSource{
			FileName: LIVE_FILE_NAME_1,
			FilePath: DATA_PATH,
			Bucket:   testCfg.bucket,
		},
		FileType: bo.CLOUD_CSV,
	}
	s.Equal(req.Start, int64(0))

	batchSize := int64(400)

	// test get csv file headers
	result, err := s.testGetCSVHeadersActivity(env, req, batchSize)
	s.NoError(err)
	l.Debug("Test_Cloud_File_GetCSVHeadersActivity - get csv headers result", slog.Any("result", result))
	s.Equal(result.Headers, []string{"ENTITY_NAME", "ENTITY_NUM", "ORG_NAME", "FIRST_NAME", "MIDDLE_NAME", "LAST_NAME", "PHYSICAL_ADDRESS", "AGENT_TYPE"})
	s.Equal(result.Start, int64(93))
}

func (s *CSVBatchActivitiesTestSuite) testGetCSVHeadersActivity(env *testsuite.TestActivityEnvironment, req *bo.FileInfo, batchSize int64) (*bo.FileInfo, error) {
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

func (s *CSVBatchActivitiesTestSuite) Test_Local_File_GetNextOffsetActivity() {
	// get test logger
	l := getTestLogger()

	// set environment logger
	s.SetLogger(l)

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

	filePath := fmt.Sprintf("%s/%s", TEST_DIR, DATA_PATH)
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
}

func (s *CSVBatchActivitiesTestSuite) Test_Cloud_File_GetNextOffsetActivity() {
	// get test logger
	l := getTestLogger()

	// set environment logger
	s.SetLogger(l)

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

	req := &bo.FileInfo{
		FileSource: bo.FileSource{
			FileName: LIVE_FILE_NAME_1,
			FilePath: DATA_PATH,
			Bucket:   testCfg.bucket,
		},
		FileType: bo.CLOUD_CSV,
	}
	s.Equal(req.Start, int64(0))

	batchSize := int64(400)

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
}

func (s *CSVBatchActivitiesTestSuite) Test_DB_File_GetNextOffsetActivity() {
	// get test logger
	l := getTestLogger()

	// set environment logger
	s.SetLogger(l)

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

	req := &bo.FileInfo{
		FileSource: bo.FileSource{
			FileName: TABLE_NAME_1,
		},
		FileType: bo.DB_CURSOR,
	}
	s.Equal(req.Start, int64(0))

	batchSize := int64(2)

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
}

func (s *CSVBatchActivitiesTestSuite) testGetNextOffsetActivity(env *testsuite.TestActivityEnvironment, req *bo.FileInfo, batchSize int64) (*bo.FileInfo, error) {
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

func (s *CSVBatchActivitiesTestSuite) Test_Local_File_ProcessBatchActivity() {
	// get test logger
	l := getTestLogger()

	// set environment logger
	s.SetLogger(l)

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

	filePath := fmt.Sprintf("%s/%s", TEST_DIR, DATA_PATH)
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
}

func (s *CSVBatchActivitiesTestSuite) Test_Cloud_File_ProcessBatchActivity() {
	// get test logger
	l := getTestLogger()

	// set environment logger
	s.SetLogger(l)

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

	req := &bo.FileInfo{
		FileSource: bo.FileSource{
			FileName: LIVE_FILE_NAME_1,
			FilePath: DATA_PATH,
			Bucket:   testCfg.bucket,
		},
		FileType: bo.CLOUD_CSV,
	}
	s.Equal(req.Start, int64(0))

	batchSize := int64(400)

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
}

func (s *CSVBatchActivitiesTestSuite) Test_DB_File_ProcessBatchActivity() {
	// get test logger
	l := getTestLogger()

	// set environment logger
	s.SetLogger(l)

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

	req := &bo.FileInfo{
		FileSource: bo.FileSource{
			FileName: TABLE_NAME_1,
		},
		FileType: bo.DB_CURSOR,
	}
	s.Equal(req.Start, int64(0))

	batchSize := int64(2)

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
}

func (s *CSVBatchActivitiesTestSuite) testProcessBatchActivity(env *testsuite.TestActivityEnvironment, req *bo.Batch) (*bo.Batch, error) {
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
}

func getTestConfig() testConfig {
	dataDir := os.Getenv("DATA_DIR")
	credsPath := os.Getenv("CREDS_PATH")
	bktName := os.Getenv("BUCKET_NAME")

	return testConfig{
		dir:       dataDir,
		bucket:    bktName,
		credsPath: credsPath,
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
