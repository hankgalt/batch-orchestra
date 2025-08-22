package sinks

import (
	"context"

	"github.com/hankgalt/batch-orchestra/pkg/domain"
)

const (
	NoopSink = "noop-sink"
)

// No operation sink for testing or defaulting.
type noopSink[T any] struct{}

// Name returns the name of the noop sink.
func (s *noopSink[T]) Name() string { return NoopSink }

// Write does nothing and returns the count of records as written.
func (s *noopSink[T]) Write(ctx context.Context, b *domain.BatchProcess[T]) (*domain.BatchProcess[T], error) {
	for _, rec := range b.Records {
		rec.BatchResult.Result = rec.Data // echo the record as result
	}
	b.Done = true // mark as done
	return b, nil
}

// Close closes the noop sink.
func (s *noopSink[T]) Close(ctx context.Context) error {
	return nil
}

// No operation sink config for testing or defaulting.
type NoopSinkConfig[T any] struct{}

// Name of the sink.
func (c NoopSinkConfig[T]) Name() string { return NoopSink }

// BuildSink returns a noop sink.
func (c NoopSinkConfig[T]) BuildSink(ctx context.Context) (domain.Sink[T], error) {
	return &noopSink[T]{}, nil
}
