# Batch Orchestra
## Overview
temporal.io go-sdk Batch Orchestra is go implementation of [Drew Hoskins's](https://github.com/drewhoskins/batch-orchestra) Batch Orchestra [python](https://github.com/drewhoskins/batch-orchestra) implementation.

The batch request workflow relies on provided reader client to read and process the batched chunks.

The reader client must implement `BatchRequestProcessor` interface and valid reader client must be provided for the selected file.

<sub>models.go</sub>
```
type BatchRequestProcessor interface {
	ChunkReader
	ChunkHandler
}

type ChunkReader interface {
	ReadData(ctx context.Context, fileSrc FileSource, offset, limit int64) (interface{}, int64, error)
}

type ChunkHandler interface {
	HandleData(ctx context.Context, fileSrc FileSource, start int64, data interface{}) (<-chan Result, <-chan error, error)
}
```

See `/clients` for sample reader implementations for a sqllite db client & reader client for a local & GCP bucket csv file.

Batch request workflow, based on file source, assigns file type to the request state. `MaxBatches` & `BatchSize` influences the concurrent processing and volume of data being processed
```
type FileSource struct {
	FileName string
	FilePath string
	Bucket   string
}

type BatchRequest struct {
	MaxBatches int
	BatchSize  int32
	Source     *FileSource
	Batches    map[string]*Batch
}
```

![Process Flow](/process-flow.png)
## Setup
### Local
- `go 1.21.6`
- `go get & go mod tidy`
- Create `/env/test.env` and add following env vars. `CREDS_PATH` & `BUCKET` are only needed for GCP cloud bucket reader
```
DATA_DIR=<data-dir>
FILE_PATH=<file-path>
CREDS_PATH=creds/<cred_json>.json
BUCKET_NAME=<gcp-bucket>
```
- Add GCP credential to `/creds` folder
- Run sample test workflows and test activities
### Worker
## In-development
- Sample server-worker setup
- Progress tracking
- Activity heartbeat feedback
- Algorithms space usage optimizations