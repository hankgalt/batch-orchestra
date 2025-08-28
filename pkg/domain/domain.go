package domain

import (
	"context"
)

// Domain "record" type that moves through the pipeline.
type CSVRow map[string]string

type BatchResult struct {
	Result any
	Error  string
}

type BatchRecord[T any] struct {
	Data        T
	Start, End  uint64
	BatchResult BatchResult
	Done        bool
}

// BatchProcess[T any] is the neutral "batch process" unit.
type BatchProcess[T any] struct {
	BatchId     string
	Records     []*BatchRecord[T]
	StartOffset uint64 // start offset in the source
	NextOffset  uint64 // cursor / next-page token / byte offset
	Error       string
	Done        bool
}

// SourceConfig[T any] is a config that *knows how to build* a Source for a specific T.
type SourceConfig[T any] interface {
	BuildSource(ctx context.Context) (Source[T], error)
	Name() string
}

// Source[T any] is a source of batches of T, e.g., a CSV file, a database table, etc.
// Pulls the next batch given an offset; return next offset.
type Source[T any] interface {
	Next(ctx context.Context, offset uint64, n uint) (*BatchProcess[T], error)
	Name() string
	Close(context.Context) error
}

// NextStreamer[T any] is an interface for streaming the next batch of records.
type NextStreamer[T any] interface {
	NextStream(ctx context.Context, offset uint64, n uint) (<-chan *BatchRecord[T], error)
}

// SinkConfig[T any] is a config that *knows how to build* a Sink for a specific T.
type SinkConfig[T any] interface {
	BuildSink(ctx context.Context) (Sink[T], error)
	Name() string
}

// Sink[T any] is a sink that writes a batch of T to a destination, e.g., a database, a file, etc.
// Writes a batch and return side info (e.g., count written, last id).
type Sink[T any] interface {
	Write(ctx context.Context, b *BatchProcess[T]) (*BatchProcess[T], error)
	Name() string
	Close(context.Context) error
}

// WriteStreamer[T any] is an interface for streaming writes of batches of T.
type WriteStreamer[T any] interface {
	WriteStream(ctx context.Context, start uint64, data []T) (<-chan BatchResult, error)
}

// WriteInput[T any, D SinkConfig[T]] is the input for the WriteActivity.
type WriteInput[T any, D SinkConfig[T]] struct {
	Sink  D
	Batch *BatchProcess[T]
}

// WriteOutput is the output for the WriteActivity.
type WriteOutput[T any] struct {
	Batch *BatchProcess[T]
}

// FetchInput[T any, S SourceConfig[T]] is the input for the FetchNextActivity.
type FetchInput[T any, S SourceConfig[T]] struct {
	Source    S
	Offset    uint64
	BatchSize uint
}

// FetchOutput[T any] is the output for the FetchNextActivity.
type FetchOutput[T any] struct {
	Batch *BatchProcess[T]
}

// BatchProcessingRequest[T any, S SourceConfig[T], D SinkConfig[T]] is a request to process a batch of T from a source S and write to a sink D.
type BatchProcessingRequest[T any, S SourceConfig[T], D SinkConfig[T]] struct {
	MaxInProcessBatches uint                        // maximum number of batches to process
	BatchSize           uint                        // maximum size of each batch
	MaxBatches          uint                        // max number of batches to processed be waorkflow (continue as new limit)
	JobID               string                      // unique identifier for the job
	StartAt             uint64                      // initial offset
	Source              S                           // source configuration
	Sink                D                           // sink configuration
	Done                bool                        // whether the job is done
	Offsets             []uint64                    // list of offsets for each batch
	Batches             map[string]*BatchProcess[T] // map of batch by ID
}

type Rule struct {
	Target   string // this value replaces the header in the CSV file
	Group    bool   // if true, include this Target column's value in a grouped field
	NewField string // if has value, include Target as new field with this value
	Order    int    // order of the rule in the mapping
}

// TransformerFunc transforms a slice of values into a key-value map based on, in closure, headers and rules.
type TransformerFunc func(values []string) map[string]any

type CloudFileConfig struct {
	Name   string
	Path   string
	Bucket string
}
