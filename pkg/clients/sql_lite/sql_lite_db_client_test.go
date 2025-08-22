package sqllite_test

import (
	"testing"

	sqllite "github.com/hankgalt/batch-orchestra/pkg/clients/sql_lite"
	"github.com/stretchr/testify/require"
)

func TestSQLLiteDBClient(t *testing.T) {
	dbFile := "data/__deleteme.db"
	tableName := "agent"
	dbClient, err := sqllite.NewSQLLiteDBClient(dbFile, tableName)
	require.NoError(t, err)

	defer func() {
		// err = os.Remove(dbFile)
		err := dbClient.Close()
		require.NoError(t, err)
	}()

	res := dbClient.ExecuteSchema(sqllite.AgentSchema)
	n, err := res.LastInsertId()
	require.NoError(t, err)
	require.Equal(t, int64(0), n)
}
