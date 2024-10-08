package clients_test

import (
	"bytes"
	"context"
	"fmt"
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
	fileName := "Agents-sm.csv"
	filePath := "scheduler"
	batchSize := int64(600)

	testCfg := getTestConfig()
	localFilePath := filepath.Join(testCfg.dir, filePath)

	reqFile := bo.FileSource{
		FileName: fileName,
		FilePath: localFilePath,
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	fileClient := &clients.LocalCSVFileClient{}

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

func processCSVStream(t *testing.T, ctx context.Context, recStream <-chan interface{}, errStream <-chan error) {
	// require.NoError(nil)
	recCnt := 0
	errCnt := 0
	for {
		select {
		case rec, ok := <-recStream:
			if ok {
				recCnt++

				vals, ok := rec.([]string)
				require.Equal(t, ok, true)

				fmt.Printf("record# %d, size: %d, record: %v\n", recCnt, len(vals), vals)
			} else {
				fmt.Println("record stream closed")
				return
			}
		case err, ok := <-errStream:
			if ok {
				errCnt++
				fmt.Printf("error processing record: %v\n", err)
			} else {
				fmt.Println("error stream closed")
				return
			}
		case <-ctx.Done():
			fmt.Println("timeout")
			return
		}
	}
}
