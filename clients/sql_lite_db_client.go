package clients

import (
	"context"
	"database/sql"
	"fmt"

	bo "github.com/hankgalt/batch-orchestra"
	"github.com/jmoiron/sqlx"
	_ "github.com/mattn/go-sqlite3"
)

type SQLLiteDBClient struct {
	*sqlx.DB
	source *FileSource
	bo.BatchRequestProcessor
}

func NewSQLLiteDBClient(dbFile, tableName string) (*SQLLiteDBClient, error) {
	// Check if the table name is provided
	if tableName == "" {
		return nil, ErrMissingFileName
	}

	db, err := sqlx.Connect("sqlite3", dbFile)
	if err != nil {
		return nil, err
	}

	return &SQLLiteDBClient{
		DB: db,
		source: &FileSource{
			FileName: tableName,
		},
	}, nil
}

func (db *SQLLiteDBClient) ReadData(ctx context.Context, offset, limit int64) (interface{}, int64, bool, error) {
	if db.source.FileName == "agent" {
		agents, err := db.FetchEntityAgentRecords(ctx, int(offset), int(limit))
		if err != nil {
			return nil, 0, false, err
		}
		return agents, int64(len(agents)), int64(len(agents)) < limit, nil
	}

	return nil, 0, false, fmt.Errorf("%s table not found", db.source.FileName)
}

func (db *SQLLiteDBClient) HandleData(ctx context.Context, start int64, data interface{}) (<-chan bo.Result, <-chan error, error) {
	if db.source.FileName == "agent" {
		recs, ok := data.([]Agent)
		if !ok {
			return nil, nil, ErrInvalidData
		}

		// Create a channel to stream the records and errors
		recStream, errStream := make(chan bo.Result), make(chan error)

		go func() {
			defer func() {
				close(recStream)
				close(errStream)
			}()

			for i, rec := range recs {
				// Process DB record
				recStream <- bo.Result{
					Start:  start + int64(i),
					End:    start + int64(i+1),
					Result: rec,
				}
			}
		}()

		return recStream, errStream, nil
	}

	return nil, nil, fmt.Errorf("%s table not found", db.source.FileName)
}

func (db *SQLLiteDBClient) Close() error {
	err := db.DB.Close()
	return err
}

func (db *SQLLiteDBClient) ExecuteSchema(schema string) sql.Result {
	// exec the schema or fail; multi-statement Exec behavior varies between
	return db.MustExec(schema)
}

func (db *SQLLiteDBClient) FetchEntityAgentRecords(ctx context.Context, offset, limit int) ([]Agent, error) {
	agents := []Agent{}
	qryStr := fmt.Sprintf("SELECT * FROM agent LIMIT %d OFFSET %d", limit, offset)
	err := db.DB.Select(&agents, qryStr)
	if err != nil {
		return nil, err
	}
	return agents, nil
}

func (db *SQLLiteDBClient) InsertAgentRecord(ctx context.Context, record map[string]interface{}) (sql.Result, error) {
	return db.InsertEntityRecord(ctx, "agent", record)
}

func (db *SQLLiteDBClient) InsertEntityRecord(ctx context.Context, table string, record map[string]interface{}) (sql.Result, error) {
	var colIdx string
	var cols string
	values := []string{}

	for i, col := range db.MapRecord(table, record) {
		if i == 0 {
			colIdx = fmt.Sprintf("$%d", i+1)
			cols = col
		} else {
			colIdx = fmt.Sprintf("%s, $%d", colIdx, i+1)
			cols = fmt.Sprintf("%s, %s", cols, col)
		}
		if col != "entity_id" {
			values = append(values, record[col].(string))
		}
	}

	qryStr := "INSERT INTO " + table + " (" + cols + ") VALUES (" + colIdx + ")"
	tx := db.DB.MustBegin()
	res := tx.MustExec(qryStr, record["entity_id"].(int), values[0], values[1], values[2], values[3])
	err := tx.Commit()
	if err != nil {
		return nil, err
	}

	return res, nil
}

func (db *SQLLiteDBClient) MapRecord(table string, record map[string]interface{}) []string {
	cols := []string{}
	if table == "agent" {
		if entity_id := record["entity_id"]; entity_id.(int) > 0 {
			cols = append(cols, "entity_id")
		}
		if entity_name := record["entity_name"]; entity_name.(string) != "" {
			cols = append(cols, "entity_name")
		}
		if first_name := record["first_name"]; first_name.(string) != "" {
			cols = append(cols, "first_name")
		}
		if last_name := record["last_name"]; last_name.(string) != "" {
			cols = append(cols, "last_name")
		}
		if agent_type := record["agent_type"]; agent_type.(string) != "" {
			cols = append(cols, "agent_type")
		}
	}
	return cols
}

type Agent struct {
	EntityId   int    `db:"entity_id"`
	EntityName string `db:"entity_name"`
	FirstName  string `db:"first_name"`
	LastName   string `db:"last_name"`
	AgentType  string `db:"agent_type"`
}

var AgentSchema = `
	DROP TABLE IF EXISTS agent;
	CREATE TABLE agent (
		entity_id   INTEGER PRIMARY KEY,
		entity_name VARCHAR(250) DEFAULT '',
		first_name  VARCHAR(80)  DEFAULT '',
		last_name   VARCHAR(80)  DEFAULT '',
		agent_type  VARCHAR(250) DEFAULT ''
	);
	`
