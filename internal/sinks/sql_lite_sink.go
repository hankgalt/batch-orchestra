package sinks

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"sync"

	sqllite "github.com/hankgalt/batch-orchestra/internal/clients/sql_lite"
	"github.com/hankgalt/batch-orchestra/pkg/domain"
)

// Error constants and variables
const (
	ERR_SQLLITE_SINK_NIL               = "sql-lite sink is nil"
	ERR_SQLLITE_SINK_NIL_CLIENT        = "sql-lite sink: nil client"
	ERR_SQLLITE_SINK_EMPTY_TABLE       = "sql-lite sink: empty table"
	ERR_SQLLITE_SINK_DB_FILE_REQUIRED  = "sql-lite sink: DB file is required"
	ERR_SQLLITE_SINK_ALL_BATCH_RECORDS = "sql-lite sink: all batch records failed"
)

var (
	ErrSQLLiteSinkNil             = errors.New(ERR_SQLLITE_SINK_NIL)
	ErrSQLLiteSinkNilClient       = errors.New(ERR_SQLLITE_SINK_NIL_CLIENT)
	ErrSQLLiteSinkEmptyTable      = errors.New(ERR_SQLLITE_SINK_EMPTY_TABLE)
	ErrSQLLiteSinkDBFileRequired  = errors.New(ERR_SQLLITE_SINK_DB_FILE_REQUIRED)
	ErrSQLLiteSinkAllBatchRecords = errors.New(ERR_SQLLITE_SINK_ALL_BATCH_RECORDS)
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
func (s *sqlLiteSink[T]) Write(ctx context.Context, b *domain.BatchProcess) (*domain.BatchProcess, error) {
	if s == nil {
		return b, ErrSQLLiteSinkNil
	}
	if s.client == nil {
		return b, ErrSQLLiteSinkNilClient
	}
	if s.table == "" {
		return b, ErrSQLLiteSinkEmptyTable
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

// WriteStream writes the batch of records to MongoDB.
func (s *sqlLiteSink[T]) WriteStream(ctx context.Context, b *domain.BatchProcess) (*domain.BatchProcess, error) {
	if s == nil {
		return b, ErrSQLLiteSinkNil
	}
	if s.client == nil {
		return b, ErrSQLLiteSinkNilClient
	}
	if s.table == "" {
		return b, ErrSQLLiteSinkEmptyTable
	}

	if len(b.Records) == 0 {
		return b, nil // nothing to write
	}

	type out struct {
		idx int
		res int64
		err error
	}

	const maxConcurrent = 8
	sem := make(chan struct{}, maxConcurrent) // semaphore to cap concurrency
	ch := make(chan out)
	var wg sync.WaitGroup
	launched := 0

	var errCount int
	errs := map[string]int{}
	for i, rec := range b.Records {
		// allow cancellation before launching work
		select {
		case <-ctx.Done():
			// Stop launching new work; we'll still drain anything already launched.
			goto waitAndCollect
		default:
		}

		if rec.BatchResult.Error != "" {
			continue // skip already errored records
		}

		launched++
		wg.Add(1)
		sem <- struct{}{} // acquire slot

		go func(i int) {
			defer wg.Done()
			defer func() { <-sem }() // release slot

			doc, err := toMapAny(rec.Data)
			if err != nil {
				select {
				case ch <- out{idx: i, err: err}:
				case <-ctx.Done():
				}
				return
			}

			res, err := s.client.InsertRecord(ctx, s.table, doc)
			if err != nil {
				select {
				case ch <- out{idx: i, err: err}:
				case <-ctx.Done():
				}
				return
			}

			n, err := res.LastInsertId()
			if err != nil {
				select {
				case ch <- out{idx: i, err: err}:
				case <-ctx.Done():
				}
				return
			}

			select {
			case ch <- out{idx: i, res: n}:
			case <-ctx.Done():
			}
		}(i)
	}

waitAndCollect:
	// Close ch after all launched goroutines finish.
	go func() {
		wg.Wait()
		close(ch)
	}()

	// Collect results and update b in a single goroutine (this one).
	for r := range ch {
		if r.err != nil {
			b.Records[r.idx].BatchResult.Error = r.err.Error()
			errs[r.err.Error()]++
			errCount++
			continue
		}
		b.Records[r.idx].BatchResult.Result = r.res
	}

	// Set the error map on the batch
	b.Error = errs

	// If we aborted launches due to cancellation, surface that.
	if err := ctx.Err(); err != nil {
		return b, err
	}

	if errCount >= len(b.Records) {
		return b, ErrSQLLiteSinkAllBatchRecords
	}
	return b, nil
}

// SQLLiteDB sink config.
type SQLLiteSinkConfig[T any] struct {
	DBFile string // e.g., "test.db"
	Table  string
}

// Name of the sink.
func (c *SQLLiteSinkConfig[T]) Name() string { return SQLLiteSink }

// BuildSink builds a SQLLite sink from the config.
func (c *SQLLiteSinkConfig[T]) BuildSink(ctx context.Context) (domain.Sink[T], error) {
	if c.DBFile == "" {
		return nil, ErrSQLLiteSinkDBFileRequired
	}
	if c.Table == "" {
		return nil, ErrSQLLiteSinkEmptyTable
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

// toMapAny converts common row shapes to map[string]any.
//   - map[string]any: pass-through
//   - map[string]string: widen to any
//   - everything else: JSON round-trip into map[string]any
func toMapAny[T any](rec T) (map[string]any, error) {
	// Fast paths
	if m, ok := any(rec).(map[string]any); ok {
		return m, nil
	}
	if ms, ok := any(rec).(map[string]string); ok {
		out := make(map[string]any, len(ms))
		for k, v := range ms {
			out[k] = v
		}
		return out, nil
	}

	// Fallback: JSON round-trip (covers structs, slices, etc.)
	b, err := json.Marshal(rec)
	if err != nil {
		return nil, fmt.Errorf("marshal: %w", err)
	}
	var out map[string]any
	if err := json.Unmarshal(b, &out); err != nil {
		return nil, fmt.Errorf("unmarshal: %w", err)
	}
	return out, nil
}
