package clients_test

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"testing"

	bo "github.com/hankgalt/batch-orchestra"
	"github.com/hankgalt/batch-orchestra/clients"

	"github.com/stretchr/testify/require"
)

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

func TestLocalCSVFileClient(t *testing.T) {
	l := getTestLogger()
	fileName := "Agents-sm.csv"
	filePath := "scheduler"
	batchSize := int64(400)

	testCfg := getTestConfig()
	localFilePath := filepath.Join(testCfg.dir, filePath)

	fileClient, err := clients.NewLocalCSVFileClient(fileName, localFilePath)
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	data, n, last, err := fileClient.ReadData(ctx, int64(0), batchSize)
	require.NoError(t, err)

	buf, ok := data.([]byte)
	require.Equal(t, true, ok)

	l.Debug("TestLocalCSVFileClient", slog.Int64("n", n), slog.Int("len", len(buf)), slog.Any("last", last))

	require.Equal(t, len(buf), int(n))
	require.Equal(t, true, n < batchSize)

	recStream, errStream, err := fileClient.HandleData(ctx, int64(0), data)
	require.NoError(t, err)

	recordCount, errorCount := processCSVStream(t, ctx, recStream, errStream)

	offset := n
	i := 1
	for !last {
		// for !last {
		data, n, last, err = fileClient.ReadData(ctx, offset, batchSize)
		require.NoError(t, err)

		buf, ok = data.([]byte)
		require.Equal(t, true, ok)
		l.Debug("TestLocalCSVFileClient", slog.Int64("n", n), slog.Int("len", len(buf)), slog.Any("last", last))

		require.Equal(t, len(buf), int(n))
		require.Equal(t, true, n < batchSize)

		recStream, errStream, err := fileClient.HandleData(ctx, int64(0), data)
		require.NoError(t, err)

		recCount, errCount := processCSVStream(t, ctx, recStream, errStream)

		recordCount += recCount
		errorCount += errCount

		offset += n

		i++
	}

	l.Debug("TestLocalCSVFileClient", slog.Int("i", i), slog.Int("recordCount", recordCount), slog.Int("errorCount", errorCount))

	require.Equal(t, recordCount, 26)
	require.Equal(t, i, 10)
}

func processCSVStream(t *testing.T, ctx context.Context, recStream <-chan bo.Result, errStream <-chan error) (int, int) {
	// require.NoError(nil)
	recCnt := 0
	errCnt := 0
	for {
		select {
		case rec, ok := <-recStream:
			if ok {
				recCnt++

				vals, ok := rec.Result.([]string)
				require.Equal(t, ok, true)

				fmt.Printf("record# %d, size: %d, record: %v\n", recCnt, len(vals), vals)
			} else {
				fmt.Println("record stream closed")
				return recCnt, errCnt
			}
		case err, ok := <-errStream:
			if ok {
				errCnt++
				fmt.Printf("error processing record: %v\n", err)
			} else {
				fmt.Println("error stream closed")
				return recCnt, errCnt
			}
		case <-ctx.Done():
			fmt.Println("timeout")
			return recCnt, errCnt
		}
	}
}

func TestFilePath(t *testing.T) {
	filePath := "data/scheduler/agents.csv"
	base := filepath.Base(filePath)
	require.Equal(t, base, "agents.csv")
	dir := filepath.Dir(filePath)
	require.Equal(t, dir, "data/scheduler")
	ext := filepath.Ext(filePath)
	require.Equal(t, ext, ".csv")

	filePath = "data/_deleteme.db"
	base = filepath.Base(filePath)
	require.Equal(t, base, "_deleteme.db")
	dir = filepath.Dir(filePath)
	require.Equal(t, dir, "data")
	ext = filepath.Ext(filePath)
	require.Equal(t, ext, ".db")
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
