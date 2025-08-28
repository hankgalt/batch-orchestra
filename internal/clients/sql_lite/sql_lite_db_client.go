package sqllite

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/jmoiron/sqlx"
	_ "github.com/mattn/go-sqlite3"
)

type SQLLiteDBClient struct {
	store *sqlx.DB
}

func NewSQLLiteDBClient(dbFile string) (*SQLLiteDBClient, error) {
	db, err := sqlx.Connect("sqlite3", dbFile)
	if err != nil {
		return nil, err
	}

	return &SQLLiteDBClient{
		store: db,
	}, nil
}

func (db *SQLLiteDBClient) ExecuteSchema(schema string) sql.Result {
	// exec the schema or fail; multi-statement Exec behavior varies between
	return db.store.MustExec(schema)
}

func (db *SQLLiteDBClient) Close(ctx context.Context) error {
	return db.store.Close()
}

func (db *SQLLiteDBClient) InsertRecord(ctx context.Context, table string, record map[string]any) (sql.Result, error) {
	if table != "agent" {
		return nil, fmt.Errorf("unsupported table: %s", table)
	}

	row := MapAgentRecord(record)
	if row == nil {
		return nil, fmt.Errorf("invalid record: %v", record)
	}

	return db.insertRecord("agent", row)
}

func (db *SQLLiteDBClient) FetchRecords(ctx context.Context, table string, offset, limit int) ([]Agent, error) {
	if table != "agent" {
		return nil, fmt.Errorf("unsupported table: %s", table)
	}

	agents := []Agent{}
	qryStr := fmt.Sprintf("SELECT * FROM %s LIMIT %d OFFSET %d", table, limit, offset)
	err := db.store.Select(&agents, qryStr)
	if err != nil {
		return nil, err
	}
	return agents, nil
}

func (db *SQLLiteDBClient) insertRecord(table string, row *Row) (sql.Result, error) {
	var colIdx string
	var cols string
	values := []any{}

	for i, col := range row.Columns {
		if i == 0 {
			colIdx = fmt.Sprintf("$%d", i+1)
			cols = col.Key
		} else {
			colIdx = fmt.Sprintf("%s, $%d", colIdx, i+1)
			cols = fmt.Sprintf("%s, %s", cols, col.Key)
		}
		values = append(values, col.Value.(string))
	}

	qryStr := "INSERT INTO " + table + " (" + cols + ") VALUES (" + colIdx + ")"
	tx := db.store.MustBegin()
	res := tx.MustExec(qryStr, values...)
	err := tx.Commit()
	if err != nil {
		return nil, err
	}

	return res, nil
}
