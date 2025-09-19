package domain

import (
	"context"
	"time"
)

// Domain "record" type that moves through the pipeline.
type CSVRow map[string]string

type BatchResult struct {
	Result any
	Error  string
}

type BatchRecord struct {
	Data        any
	Start, End  string
	BatchResult BatchResult
	Done        bool
}

// BatchProcess is the neutral "batch process" unit.
type BatchProcess struct {
	BatchId     string
	Records     []*BatchRecord
	StartOffset string         // start offset in the source
	NextOffset  string         // cursor / next-page token / byte offset
	Error       map[string]int // map of error message to count
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
	Next(ctx context.Context, offset string, n uint) (*BatchProcess, error)
	Name() string
	Close(context.Context) error
}

// NextStreamer[T any] is an interface for streaming the next batch of records.
type NextStreamer[T any] interface {
	NextStream(ctx context.Context, offset string, n uint) (<-chan *BatchRecord, error)
}

type HasSize interface {
	Size() int64
}

// SinkConfig[T any] is a config that *knows how to build* a Sink for a specific T.
type SinkConfig[T any] interface {
	BuildSink(ctx context.Context) (Sink[T], error)
	Name() string
}

// Sink[T any] is a sink that writes a batch of T to a destination, e.g., a database, a file, etc.
// Writes a batch and return side info (e.g., count written, last id).
type Sink[T any] interface {
	Write(ctx context.Context, b *BatchProcess) (*BatchProcess, error)
	Name() string
	Close(context.Context) error
}

type SnapshotConfig interface {
	BuildSnapshotter(ctx context.Context) (Snapshotter, error)
	Name() string
}

type Snapshotter interface {
	Snapshot(ctx context.Context, key string, snapshot any) error
	Close(context.Context) error
	Name() string
}

// WriteStreamer[T any] is an interface for streaming writes of batches of T.
type WriteStreamer[T any] interface {
	WriteStream(ctx context.Context, start string, data []T) (<-chan BatchResult, error)
}

// WriteInput[T any, D SinkConfig[T]] is the input for the WriteActivity.
type WriteInput[T any, D SinkConfig[T]] struct {
	Sink  D
	Batch *BatchProcess
}

// WriteOutput is the output for the WriteActivity.
type WriteOutput[T any] struct {
	Batch *BatchProcess
}

// FetchInput[T any, S SourceConfig[T]] is the input for the FetchNextActivity.
type FetchInput[T any, S SourceConfig[T]] struct {
	Source    S
	Offset    string
	BatchSize uint
}

// FetchOutput[T any] is the output for the FetchNextActivity.
type FetchOutput[T any] struct {
	Batch *BatchProcess
}

// BatchProcessingRequest[T any, S SourceConfig[T], D SinkConfig[T]]
// is a request to process a batch of T from a source S and write to a sink D.
type BatchProcessingRequest[T any, S SourceConfig[T], D SinkConfig[T], SS SnapshotConfig] struct {
	JobID               string                     // unique identifier for the job
	BatchSize           uint                       // maximum size of each batch
	MaxInProcessBatches uint                       // maximum number of batches to process
	MaxBatches          uint                       // max number of batches to processed be waorkflow (continue as new limit)
	PauseDuration       time.Duration              // duration to pause between batches
	PauseRecordCount    int64                      // number of times to pause between batches
	Policies            map[string]RetryPolicySpec // map of retry policies for batch activities by activity alias

	Source      S  // source configuration
	Sink        D  // sink configuration
	Snapshotter SS // snapshotter configuration

	StartAt  string                   // initial offset
	Done     bool                     // whether the job is done
	Offsets  []string                 // list of offsets for each batch
	Batches  map[string]*BatchProcess // map of batch by ID
	Snapshot *BatchSnapshot           // Processed snapshot
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

type RetryPolicySpec struct {
	InitialInterval        time.Duration
	BackoffCoefficient     float64
	MaximumInterval        time.Duration
	MaximumAttempts        int32
	NonRetryableErrorTypes []string
}

// BatchProcessingResult
// is a result snapshot a batch of T
type BatchProcessingResult struct {
	JobID          string                   // unique identifier for the job
	StartAt        string                   // initial offset
	Done           bool                     // whether the job is done
	Offsets        []string                 // list of offsets for each batch
	Batches        map[string]*BatchProcess // map of batch by ID
	Error          string                   // error message if any
	DonePercentage float32                  // percentage of batches done
	NumRecords     int64                    // number of records processed
	NumBatches     int64                    // number of batches processed
}

type ErrorRecord struct {
	Start, End string
	Error      string
}

type BatchSnapshot struct {
	Done           bool
	NumBatches     int64                    // number of batches processed
	NumRecords     int64                    // number of records processed
	PauseCount     int64                    // number of times the job was paused
	SnapshotIdx    []string                 // snapshot indexes (offsets)
	Errors         map[string][]ErrorRecord // map of batch ID to list of error records
	DonePercentage float32                  // percentage of batches done
}
