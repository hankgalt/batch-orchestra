package sinks

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/hankgalt/batch-orchestra/pkg/clients/mongodb"
	"github.com/hankgalt/batch-orchestra/pkg/domain"
)

const (
	MongoSink = "mongo-sink"
)

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

// Write writes the batch of records to MongoDB.
func (s *mongoSink[T]) Write(ctx context.Context, b *domain.BatchProcess[T]) (*domain.BatchProcess[T], error) {
	if s == nil {
		return b, errors.New("mongo sink is nil")
	}
	if s.client == nil {
		return b, errors.New("mongo sink: nil client")
	}
	if s.collection == "" {
		return b, errors.New("mongo sink: empty collection")
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
		return nil, errors.New("mongo sink: DB protocol is required")
	}
	if c.Host == "" {
		return nil, errors.New("mongo sink: DB host is required")
	}
	if c.DBName == "" {
		return nil, errors.New("mongo sink: DB name is required")
	}
	if c.User == "" {
		return nil, errors.New("mongo sink: DB user is required")
	}
	if c.Pwd == "" {
		return nil, errors.New("mongo sink: DB password is required")
	}
	if c.Collection == "" {
		return nil, errors.New("mongo sink: collection name is required")
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
