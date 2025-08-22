# Batch Orchestra
## Overview
 - temporal.io go-sdk Batch Orchestra is go implementation of [Drew Hoskins's](https://github.com/drewhoskins/batch-orchestra) Batch Orchestra [python](https://github.com/drewhoskins/batch-orchestra) implementation.


ProcessBatchWorkflow processes batches of data from source & insert/upsert data to sink

<sub>models.go</sub>

BatchProcessingRequest[T any, S SourceConfig[T], D SinkConfig[T]] is a request to process a batch of T from a source S and write to a sink D.
```
type BatchProcessingRequest[T any, S SourceConfig[T], D SinkConfig[T]] struct {
	MaxBatches uint                        // maximum number of batches to process
	BatchSize  uint                        // maximum size of each batch
	JobID      string                      // unique identifier for the job
	StartAt    uint64                      // initial offset
	Source     S                           // source configuration
	Sink       D                           // sink configuration
	Done       bool                        // whether the job is done
	Offsets    []uint64                    // list of offsets for each batch
	Batches    map[string]*BatchProcess[T] // map of batch by ID
}
```

SourceConfig[T any] is a config that *knows how to build* a Source for a specific T.
```
type SourceConfig[T any] interface {
	BuildSource(ctx context.Context) (Source[T], error)
	Name() string
}
```

Source[T any] is a source of batches of T, e.g., a CSV file, a database table, etc. 
Pulls the next batch given an offset; return next offset. A source satisfies following interface:
```
type Source[T any] interface {
	Next(ctx context.Context, offset uint64, n uint) (*BatchProcess[T], error)
	Name() string
	Close(context.Context) error
}
```

SinkConfig[T any] is a config that *knows how to build* a Sink for a specific T.
```
type SinkConfig[T any] interface {
	BuildSink(ctx context.Context) (Sink[T], error)
	Name() string
}
```

Sink[T any] is a sink that writes a batch of T to a destination, e.g., a database, a file, etc.
Writes a batch and return side info (e.g., count written, last id).
```
type Sink[T any] interface {
	Write(ctx context.Context, b *BatchProcess[T]) (*BatchProcess[T], error)
	Name() string
	Close(context.Context) error
}
```

![Process Flow](/process-flow-1.png)
## Setup
### Local
- `go 1.23.6`
- `go get & go mod tidy`
- Test with sample sources & sinks
- Create `env/test.env` & add following env vars. 
	- for cloud CSV (GCS only) source
		- Add `GOOGLE_APPLICATION_CREDENTIALS, BUCKET, FILE_NAME`
		- Add GCP credential to `/creds` folder
	- for local CSV source
		- `DATA_DIR, FILE_NAME`
	- for mongo sink
		- Add `MONGO_PROTOCOL, MONGO_DBNAME, MONGO_HOSTNAME, MONGO_CONN_PARAMS, MONGO_USERNAME, MONGO_PASSWORD, MONGO_COLLECTION`
- Mongo setup
	- Create `clients/mongodb/mongo.env` & add following vars
		- `MONGO_HOST, MONGO_REPLICA_SET_NAME, MONGO_INITDB_ROOT_USERNAME, MONGO_INITDB_ROOT_PASSWORD, MONGO_ADMIN_USER, MONGO_ADMIN_PASS, MONGO_APP_DB, MONGO_APP_USER, MONGO_APP_PASS`
	- add `127.0.0.1	mongo mongo-rep1 mongo-rep2` to `/etc/hosts`
	- start docker
	- `make start-mongo`
- Local dev server
	- `start-dev-server`

- Run sample test workflows and test activities
### Worker
## In-development
- Sample server-worker setup
- Progress tracking
- Activity heartbeat feedback
- Algorithms space usage optimizations