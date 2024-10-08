package clients_test

import (
	"bytes"
	"context"
	"testing"

	bo "github.com/hankgalt/batch-orchestra"
	"github.com/hankgalt/batch-orchestra/clients"
	"github.com/stretchr/testify/require"
)

func TestCloudCSVFileClient(t *testing.T) {
	fileName := "Agents-sm.csv"
	filePath := "scheduler"
	batchSize := int64(600)

	testCfg := getTestConfig()

	cscCfg := clients.CloudStorageClientConfig{
		CredsPath: testCfg.credsPath,
	}

	fileClient, err := clients.NewCloudCSVFileClient(cscCfg)
	require.NoError(t, err)
	defer func() {
		err := fileClient.Close()
		require.NoError(t, err)
	}()

	reqFile := bo.FileSource{
		FileName: fileName,
		FilePath: filePath,
		Bucket:   testCfg.bucket,
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	data, n, err := fileClient.ReadData(ctx, reqFile, int64(0), batchSize)
	require.NoError(t, err)

	buf, ok := data.([]byte)
	require.Equal(t, true, ok)

	require.Equal(t, len(buf), int(batchSize))
	require.Equal(t, n, batchSize)

	var nextOffset int64
	i := 0
	if data != nil {
		i = bytes.LastIndex(data.([]byte), []byte{'\n'})
	}
	if i > 0 && n == batchSize {
		nextOffset = int64(i) + 1
	} else {
		nextOffset = int64(n)
	}
	require.Equal(t, true, int64(nextOffset) < batchSize)

	recStream, errStream, err := fileClient.HandleData(ctx, reqFile, data)
	require.NoError(t, err)

	processCSVStream(t, ctx, recStream, errStream)
}
