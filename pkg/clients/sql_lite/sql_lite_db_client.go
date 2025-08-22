package sqllite

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/jmoiron/sqlx"
	_ "github.com/mattn/go-sqlite3"
)

type SQLLiteDBClient struct {
	*sqlx.DB
}

func NewSQLLiteDBClient(dbFile, tableName string) (*SQLLiteDBClient, error) {
	// Check if the table name is provided
	if tableName == "" {
		return nil, fmt.Errorf("table name cannot be empty")
	}

	db, err := sqlx.Connect("sqlite3", dbFile)
	if err != nil {
		return nil, err
	}

	return &SQLLiteDBClient{
		DB: db,
	}, nil
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

func (db *SQLLiteDBClient) InsertAgentRecord(ctx context.Context, record map[string]any) (sql.Result, error) {
	return db.InsertEntityRecord(ctx, "agent", record)
}

func (db *SQLLiteDBClient) InsertEntityRecord(ctx context.Context, table string, record map[string]any) (sql.Result, error) {
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

func (db *SQLLiteDBClient) MapRecord(table string, record map[string]any) []string {
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
