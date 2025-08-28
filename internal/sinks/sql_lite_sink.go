package sinks

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/hankgalt/batch-orchestra/pkg/domain"

	sqllite "github.com/hankgalt/batch-orchestra/internal/clients/sql_lite"
)

const SQLLiteSink = "sql-lite-sink"

// SQLLiteRecordWriter is the tiny capability we need.
type SQLLiteRecordWriter interface {
	InsertRecord(ctx context.Context, table string, record map[string]any) (sql.Result, error)
	Close(ctx context.Context) error
}

// SQLLite sink.
type sqlLiteSink[T any] struct {
	client SQLLiteRecordWriter // SQLLite client
	table  string              // table name
}

// Name returns the name of the SQLLite sink.
func (s *sqlLiteSink[T]) Name() string { return SQLLiteSink }

// Close closes the SQLLite sink.
func (s *sqlLiteSink[T]) Close(ctx context.Context) error {
	return s.client.Close(ctx)
}

// Write writes the batch of records to SQLLite.
func (s *sqlLiteSink[T]) Write(ctx context.Context, b *domain.BatchProcess[T]) (*domain.BatchProcess[T], error) {
	if s == nil {
		return b, errors.New("sql-lite sink is nil")
	}
	if s.client == nil {
		return b, errors.New("sql-lite sink: nil client")
	}
	if s.table == "" {
		return b, errors.New("sql-lite sink: empty table")
	}

	if len(b.Records) == 0 {
		return b, nil // nothing to write
	}

	for i, rec := range b.Records {
		// allow cancellation
		select {
		case <-ctx.Done():
			return b, ctx.Err()
		default:
		}

		if rec.BatchResult.Error != "" {
			continue // skip already errored records
		}

		doc, err := toMapAny(rec.Data)
		if err != nil {
			b.Records[i].BatchResult.Error = fmt.Sprintf("record %d convert: %s", i, err.Error())
			continue
		}

		res, err := s.client.InsertRecord(ctx, s.table, doc)
		if err != nil {
			b.Records[i].BatchResult.Error = fmt.Sprintf("record %d insert: %s", i, err.Error())
			continue
		}

		n, err := res.LastInsertId()
		if err != nil {
			b.Records[i].BatchResult.Error = fmt.Sprintf("record %d last insert id: %s", i, err.Error())
			continue
		}
		b.Records[i].BatchResult.Result = n // store the inserted ID or result
	}
	return b, nil
}

// SQLLiteDB sink config.
type SQLLiteSinkConfig[T any] struct {
	DBFile string // e.g., "test.db"
	Table  string
}

// Name of the sink.
func (c SQLLiteSinkConfig[T]) Name() string { return SQLLiteSink }

// BuildSink builds a SQLLite sink from the config.
func (c SQLLiteSinkConfig[T]) BuildSink(ctx context.Context) (domain.Sink[T], error) {
	if c.DBFile == "" {
		return nil, errors.New("sql-lite sink: DB file is required")
	}
	if c.Table == "" {
		return nil, errors.New("sql-lite sink: table name is required")
	}

	dbClient, err := sqllite.NewSQLLiteDBClient(c.DBFile)
	if err != nil {
		return nil, err
	}

	return &sqlLiteSink[T]{
		client: dbClient,
		table:  c.Table,
	}, nil
}
