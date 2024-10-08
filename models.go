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
	ReadData(ctx context.Context, fileSource FileSource, offset, limit int64) (interface{}, int64, error)
}

type ChunkHandler interface {
	HandleData(ctx context.Context, fileSrc FileSource, data interface{}) (<-chan interface{}, <-chan error, error)
}

type BatchRequestProcessor interface {
	ChunkReader
	ChunkHandler
}

type FileType string

const (
	CSV       FileType = "CSV"
	JSON      FileType = "JSON"
	DB_CURSOR FileType = "DB_CURSOR"
)

// in process state
type FileInfo struct {
	FileType FileType
	FileName string
	FilePath string
	Headers  []string
	Start    int64
	End      int64
	OffSets  []int64
}
