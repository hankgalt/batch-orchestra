package batch_orchestra

import "context"

type ContextKey string

func (c ContextKey) String() string {
	return string(c)
}

const ReaderClientContextKey = ContextKey("reader-client")
const ReaderBucketContextKey = ContextKey("reader-bucket")

type FileSource struct {
	FileName string
	FilePath string
	Bucket   string
}

type ChunkReader interface {
	ReadData(ctx context.Context, fileSrc FileSource, offset, limit int64) (interface{}, int64, error)
}

type ChunkHandler interface {
	HandleData(ctx context.Context, fileSrc FileSource, start int64, data interface{}) (<-chan Result, <-chan error, error)
}

type BatchRequestProcessor interface {
	ChunkReader
	ChunkHandler
}

type FileType string

const (
	CSV       FileType = "CSV"
	CLOUD_CSV FileType = "CLOUD_CSV"
	DB_CURSOR FileType = "DB_CURSOR"
)

// in process state
type FileInfo struct {
	FileType FileType
	FileSource
	Headers []string
	Start   int64
	End     int64
	OffSets []int64
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
