package sqllite_test

import (
	"context"
	"fmt"
	"os"
	"testing"

	sqllite "github.com/hankgalt/batch-orchestra/internal/clients/sql_lite"
	"github.com/stretchr/testify/require"
)

func TestSQLLiteDBClient(t *testing.T) {
	dbFile := "data/__deleteme.db"
	dbClient, err := sqllite.NewSQLLiteDBClient(dbFile)
	require.NoError(t, err)

	defer func() {
		err := dbClient.Close(context.Background())
		require.NoError(t, err)

		err = os.Remove(dbFile)
		require.NoError(t, err)
	}()

	res := dbClient.ExecuteSchema(sqllite.AgentSchema)
	n, err := res.LastInsertId()
	require.NoError(t, err)
	require.Equal(t, int64(0), n)

	start := 13745
	for i := range 5 {
		record := map[string]any{
			"entity_id":   fmt.Sprintf("%d", start+i),
			"entity_name": "Test Entity",
			"name":        "Test Agent",
			"address":     "7648 Gotham St New York NY",
			"agent_type":  "Individual agent",
		}
		res, err = dbClient.InsertRecord(context.Background(), "agent", record)
		require.NoError(t, err)

		n, err = res.LastInsertId()
		require.NoError(t, err)
		require.True(t, n > 0)
	}

	ags, err := dbClient.FetchRecords(context.Background(), "agent", 0, 10)
	require.NoError(t, err)
	require.Equal(t, len(ags), 5)
}
