package sinks

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/hankgalt/batch-orchestra/internal/clients/mongodb"
	"github.com/hankgalt/batch-orchestra/pkg/domain"
)

// Error constants and variables
const (
	ErrMsgMongoSinkNil                = "mongo sink is nil"
	ErrMsgMongoSinkNilClient          = "mongo sink: nil client"
	ErrMsgMongoSinkEmptyCollection    = "mongo sink: empty collection"
	ErrMsgMongoSinkDBProtocolRequired = "mongo sink: DB protocol is required"
	ErrMsgMongoSinkDBHostRequired     = "mongo sink: DB host is required"
	ErrMsgMongoSinkDBNameRequired     = "mongo sink: DB name is required"
	ErrMsgMongoSinkDBUserRequired     = "mongo sink: DB user is required"
	ErrMsgMongoSinkDBPwdRequired      = "mongo sink: DB password is required"
)

var (
	ErrMongoSinkNil                = errors.New(ErrMsgMongoSinkNil)
	ErrMongoSinkNilClient          = errors.New(ErrMsgMongoSinkNilClient)
	ErrMongoSinkEmptyCollection    = errors.New(ErrMsgMongoSinkEmptyCollection)
	ErrMongoSinkDBProtocolRequired = errors.New(ErrMsgMongoSinkDBProtocolRequired)
	ErrMongoSinkDBHostRequired     = errors.New(ErrMsgMongoSinkDBHostRequired)
	ErrMongoSinkDBNameRequired     = errors.New(ErrMsgMongoSinkDBNameRequired)
	ErrMongoSinkDBUserRequired     = errors.New(ErrMsgMongoSinkDBUserRequired)
	ErrMongoSinkDBPwdRequired      = errors.New(ErrMsgMongoSinkDBPwdRequired)
)

const MongoSink = "mongo-sink"

// MongoDocWriter is the tiny capability we need.
type MongoDocWriter interface {
	AddCollectionDoc(ctx context.Context, collectionName string, doc map[string]any) (string, error)
	Close(ctx context.Context) error
}

// MongoDB sink.
type mongoSink[T any] struct {
	client     MongoDocWriter // MongoDB client
	collection string         // collection name
}

// Name returns the name of the mongo sink.
func (s *mongoSink[T]) Name() string { return MongoSink }

func (s *mongoSink[T]) WriteStream(ctx context.Context, start uint64, data []T) (<-chan domain.BatchResult, error) {
	resStream := make(chan domain.BatchResult)

	go func() {
		defer close(resStream)

		for i, rec := range data {
			// allow cancellation
			select {
			case <-ctx.Done():
				return
			default:
			}

			doc, err := toMapAny(rec)
			if err != nil {
				resStream <- domain.BatchResult{
					Error: fmt.Sprintf("record %d convert: %s", i, err.Error()),
				}
				continue
			}

			res, err := s.client.AddCollectionDoc(ctx, s.collection, doc)
			if err != nil {
				resStream <- domain.BatchResult{
					Error: fmt.Sprintf("record %d insert: %s", i, err.Error()),
				}
				continue
			}
			resStream <- domain.BatchResult{
				Result: res,
			}
		}
	}()

	return resStream, nil
}

// Write writes the batch of records to MongoDB.
func (s *mongoSink[T]) Write(ctx context.Context, b *domain.BatchProcess[T]) (*domain.BatchProcess[T], error) {
	if s == nil {
		return b, ErrMongoSinkNil
	}
	if s.client == nil {
		return b, ErrMongoSinkNilClient
	}
	if s.collection == "" {
		return b, ErrMongoSinkEmptyCollection
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

		res, err := s.client.AddCollectionDoc(ctx, s.collection, doc)
		if err != nil {
			b.Records[i].BatchResult.Error = fmt.Sprintf("record %d insert: %s", i, err.Error())
			continue
		}
		b.Records[i].BatchResult.Result = res // store the inserted ID or result
	}
	return b, nil
}

// Close closes the mongo sink.
func (s *mongoSink[T]) Close(ctx context.Context) error {
	return s.client.Close(ctx)
}

// MongoDB sink config.
type MongoSinkConfig[T any] struct {
	Protocol   string // e.g., "mongodb", "mongodb+srv"
	Host       string // e.g., "localhost:27017"
	DBName     string // e.g., "testdb"
	User       string // MongoDB user
	Pwd        string // MongoDB password
	Params     string // e.g., "retryWrites=true&w=majority"
	Collection string
}

// Name of the sink.
func (c MongoSinkConfig[T]) Name() string { return MongoSink }

// BuildSink builds a MongoDB sink from the config.
func (c MongoSinkConfig[T]) BuildSink(ctx context.Context) (domain.Sink[T], error) {
	if c.Protocol == "" {
		return nil, ErrMongoSinkDBProtocolRequired
	}
	if c.Host == "" {
		return nil, ErrMongoSinkDBHostRequired
	}
	if c.DBName == "" {
		return nil, ErrMongoSinkDBNameRequired
	}
	if c.User == "" {
		return nil, ErrMongoSinkDBUserRequired
	}
	if c.Pwd == "" {
		return nil, ErrMongoSinkDBPwdRequired
	}
	if c.Collection == "" {
		return nil, ErrMongoSinkEmptyCollection
	}

	mCfg := mongodb.MongoConfig{
		Protocol: c.Protocol,
		Host:     c.Host,
		User:     c.User,
		Pwd:      c.Pwd,
		Params:   c.Params,
		DBName:   c.DBName,
	}

	mCl, err := mongodb.NewMongoStore(ctx, mCfg)
	if err != nil {
		return nil, fmt.Errorf("mongo sink: create store: %w", err)
	}

	// init client/collection; consider pooling in activities
	return &mongoSink[T]{
		client:     mCl,
		collection: c.Collection,
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
