package batch_orchestra

import "context"

type ContextKey string

func (c ContextKey) String() string {
	return string(c)
}

const ReaderClientContextKey = ContextKey("reader-client")
const ReaderBucketContextKey = ContextKey("reader-bucket")

type ChunkReader interface {
	ReadData(ctx context.Context, offset, limit int64) (interface{}, int64, bool, error)
}

type ChunkHandler interface {
	HandleData(ctx context.Context, start int64, data interface{}) (<-chan Result, <-chan error, error)
}

type BatchRequestProcessor interface {
	ChunkReader
	ChunkHandler
}

// in process state
type FileInfo struct {
	FileName string
	Start    int64
	End      int64
	OffSets  []int64
}

// batch request
type BatchRequest struct {
	MaxBatches int
	BatchSize  int32
	FileName   string
	Batches    map[string]*Batch
}

// batch in process state
type Batch struct {
	*FileInfo
	BatchID    string
	Start, End int64
	HostID     string
	Records    map[string]*Record
	Error      error
}

// record in process state
type Record struct {
	RecordID   string
	Start, End int64
	Status     bool
	Error      error
}

type Result struct {
	Start, End int64
	Result     interface{}
}
