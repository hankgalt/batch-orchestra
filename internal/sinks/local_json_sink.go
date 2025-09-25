package sinks

import (
	"context"

	"github.com/hankgalt/batch-orchestra/pkg/domain"
)

const LocalJSONSink = "local-json-sink"

// Local json sink
type localJSONSink[T any] struct{}

// Name returns the name of the local json sink.
func (s *localJSONSink[T]) Name() string { return LocalJSONSink }

// Write returns the written batch process.
func (s *localJSONSink[T]) Write(ctx context.Context, b *domain.BatchProcess) (*domain.BatchProcess, error) {
	for _, rec := range b.Records {
		rec.BatchResult.Result = rec.Data // echo the record as result
	}
	return b, nil
}

// Close closes the local json sink.
func (s *localJSONSink[T]) Close(ctx context.Context) error {
	return nil
}

// Local json sink config
type LocalJSONSinkConfig[T any] struct{}

// Name of the local json sink.
func (c *LocalJSONSinkConfig[T]) Name() string { return LocalJSONSink }

// BuildSink returns a local json sink.
func (c *LocalJSONSinkConfig[T]) BuildSink(ctx context.Context) (domain.Sink[T], error) {
	return &localJSONSink[T]{}, nil
}
