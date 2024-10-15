package clients_test

import (
	"context"
	"log/slog"
	"testing"

	"github.com/hankgalt/batch-orchestra/clients"
	"github.com/stretchr/testify/require"
)

func TestCloudCSVFileClient(t *testing.T) {
	l := getTestLogger()
	fileName := "Agents-sm.csv"
	filePath := "scheduler"
	batchSize := int64(400)

	testCfg := getTestConfig()

	cscCfg := clients.CloudStorageClientConfig{
		CredsPath: testCfg.credsPath,
		FileName:  fileName,
		FilePath:  filePath,
		Bucket:    testCfg.bucket,
	}

	fileClient, err := clients.NewCloudCSVFileClient(cscCfg)
	require.NoError(t, err)
	defer func() {
		err := fileClient.Close()
		require.NoError(t, err)
	}()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	data, n, last, err := fileClient.ReadData(ctx, int64(0), batchSize)
	require.NoError(t, err)

	buf, ok := data.([]byte)
	require.Equal(t, true, ok)

	l.Debug("TestCloudCSVFileClient", slog.Int64("n", n), slog.Int("len", len(buf)), slog.Any("last", last))

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
		l.Debug("TestCloudCSVFileClient", slog.Int64("n", n), slog.Int("len", len(buf)), slog.Any("last", last))

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

	l.Debug("TestCloudCSVFileClient", slog.Int("i", i), slog.Int("recordCount", recordCount), slog.Int("errorCount", errorCount))

	require.Equal(t, recordCount, 26)
	require.Equal(t, i, 10)
}
