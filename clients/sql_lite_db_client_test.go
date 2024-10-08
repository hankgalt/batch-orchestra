package clients_test

import (
	"context"
	"fmt"
	"testing"

	bo "github.com/hankgalt/batch-orchestra"
	"github.com/hankgalt/batch-orchestra/clients"
	"github.com/stretchr/testify/require"
)

func TestSQLLiteDBClient(t *testing.T) {
	dbFile := "data/__deleteme.db"
	dbClient, err := clients.NewSQLLiteDBClient(dbFile)
	require.NoError(t, err)

	defer func() {
		// err = os.Remove(dbFile)
		err := dbClient.Close()
		require.NoError(t, err)
	}()

	res := dbClient.ExecuteSchema(clients.AgentSchema)
	n, err := res.LastInsertId()
	require.NoError(t, err)
	require.Equal(t, int64(0), n)

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
		res, err = dbClient.InsertEntityRecord(ctx, "agent", rec)
		require.NoError(t, err)

		n, err := res.LastInsertId()
		require.NoError(t, err)
		require.Equal(t, int(n), rec["entity_id"].(int))
		rows, err := res.RowsAffected()
		require.NoError(t, err)
		require.Equal(t, int(rows), 1)
	}

	batchSize := int64(2)
	tableName := "agent"
	reqFile := bo.FileSource{
		FileName: tableName,
	}
	data, n, err := dbClient.ReadData(ctx, reqFile, int64(0), batchSize)
	require.NoError(t, err)
	require.Equal(t, int64(2), n)

	recs, ok := data.([]clients.Agent)
	require.Equal(t, true, ok)
	require.Equal(t, int64(len(recs)), n)

	recStream, errStream, err := dbClient.HandleData(ctx, reqFile, int64(0), data)
	require.NoError(t, err)

	processDBRecordStream(t, ctx, recStream, errStream)
}

func processDBRecordStream(t *testing.T, ctx context.Context, recStream <-chan bo.Result, errStream <-chan error) {
	recCnt := 0
	errCnt := 0

	defer func() {
		require.Equal(t, recCnt, 2)
	}()

	for {
		select {
		case rec, ok := <-recStream:
			if ok {
				recCnt++

				val, ok := rec.Result.(clients.Agent)
				require.Equal(t, ok, true)

				fmt.Printf("record# %d, record: %v\n", recCnt, val)
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
