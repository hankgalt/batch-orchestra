# Batch Orchestra
Batch Orchestra is a reusable Temporal (Go SDK) module for building batch data pipelines—from any Source to any Sink—with type-safe generics, clear extension points, and proven workflow patterns.

It provides the orchestration to read in chunks, process reliably, and write with retries using Temporal’s guarantees. You focus on the “how to read” and “how to write”; Batch Orchestra conducts the rest.

## Overview:

### Why Batch Orchestra:
Real-world “read → transform → write” jobs must solve the same headaches:
- Chunking & offsets for resumability and idempotency
- Throughput vs. history size, requiring Continue-as-New
- Retries & backoff around flaky IO
- Deterministic orchestration, moving all side effects to activities
- Extensibility to add new sources/sinks without touching workflow logic

Batch Orchestra gives you these building blocks so teams implement only domain-specific reading and writing while inheriting robust orchestration.

It’s the Go version inspired by [Drew Hoskins's](https://github.com/drewhoskins/batch-orchestra) Batch Orchestra [python](https://github.com/drewhoskins/batch-orchestra) implementation

### Concepts & Interfaces:
At the core is a generic batch request that flows through a single reusable workflow, plus three small extension points:
- BatchProcessingRequest: run configuration (batch size, starting offset, identifiers, and concrete source/sink configs).
```
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
```
- SourceConfig / SinkConfig: lightweight “builders” that construct concrete Sources/Sinks.
```
type SourceConfig[T any] interface {
	BuildSource(ctx context.Context) (Source[T], error)
	Name() string
}
type SinkConfig[T any] interface {
	BuildSink(ctx context.Context) (Sink[T], error)
	Name() string
}
```
- Source / Sink: concrete implementations that read the next batch and write a batch respectively.
```
type Source[T any] interface {
	Next(ctx context.Context, offset uint64, n uint) (*BatchProcess[T], error)
	Name() string
	Close(context.Context) error
}
type Sink[T any] interface {
	Write(ctx context.Context, b *BatchProcess[T]) (*BatchProcess[T], error)
	Name() string
	Close(context.Context) error
}
```
- Optional progress bookkeeping (offsets seen per batch, completion flag).
 - Named source/sink configs so logs, metrics, and registration aliases are consistent.

 ### Quick start:
 #### Install:
 - Go 1.23+
 - `go get github.com/hankgalt/batch-orchestra`
 - `go mod tidy`
#### Define a Source:
- Create a SourceConfig that can build a Source instance (open files, connect to cloud/object stores, etc.).
- Implement a Source that yields batches of records and advances an offset (the offset should not count header rows for CSV-style inputs).
- Keep reading logic tolerant of ragged data when appropriate and idempotent across retries.
- See `pkg/sources/` for examples
#### Define a Sink:
- Create a SinkConfig that can build a Sink instance (e.g., a DB writer, an API producer, a file appender).
- Implement a Sink that writes a batch and returns counts/metadata; stop on the first hard error to propagate failure back to the workflow.
- Keep writing logic idempotent when possible; rely on Temporal retries for transient issues.
- See `pkg/sinks/` for examples
#### Register activities/workflow (with aliases):
- Always register activities and workflows with explicit names (aliases).
With generics, reflection-derived names can be unstable (e.g., “func1”), which breaks resolution.
- Use the same names inside the workflow when scheduling activities.
#### Start a workflow run:
- Construct a BatchProcessingRequest with your concrete SourceConfig and SinkConfig.
- Choose a reasonable BatchSize and initial StartAt offset
- Execute the registered workflow name on your Temporal task queue.

### Continue-as-New (CAN) & preserving state:
Large jobs should periodically Continue-as-New to keep workflow history small and stable:
- Use the workflow’s own registered name when continuing (available via workflow info).
- Carry forward all state needed by the next run in the request you pass on (e.g., StartAt, any alias names if you store them in configs or request, custom metadata).

### Testing:
Use Temporal’s Go SDK test suite to run workflows and activities deterministically:
- Register your concrete generic instantiations with explicit aliases.
- Execute the workflow by its alias and assert on observable effects (e.g., rows written, offsets advanced, errors propagated).
- For external systems, provide fake clients that capture inputs and inject failures.
- If you need to examine a Continue-as-New handoff during tests, disable run-following and decode the next-run input from the error payloads with the test environment’s data converter.

Guidelines:
- Keep IO in activities and use the test harness to supply activity context (or inject a background context via worker options).
- Prefer small, table-driven tests for sources and sinks; and one or two end-to-end tests per recipe.

Run sample test workflows and test activities:
- Mongo setup
	- Create `pkg/clients/mongodb/mongo.env` & add following vars
		- `MONGO_HOST, MONGO_REPLICA_SET_NAME, MONGO_INITDB_ROOT_USERNAME, MONGO_INITDB_ROOT_PASSWORD, MONGO_ADMIN_USER, MONGO_ADMIN_PASS, MONGO_APP_DB, MONGO_APP_USER, MONGO_APP_PASS`
	- add `127.0.0.1	mongo mongo-rep1 mongo-rep2` to `/etc/hosts`
	- start docker
	- `make start-mongo`
- Local dev server
	- `start-dev-server`
- Sources/Sinks: Create `env/test.env` & add following env vars. 
	- for cloud CSV (GCS only) source
		- Add `GOOGLE_APPLICATION_CREDENTIALS, BUCKET, FILE_NAME`
		- Add GCP credential to creds path folder
	- for local CSV source
		- `DATA_DIR, FILE_NAME`
	- for mongo sink
		- Add `MONGO_PROTOCOL, MONGO_DBNAME, MONGO_HOSTNAME, MONGO_CONN_PARAMS, MONGO_USERNAME, MONGO_PASSWORD, MONGO_COLLECTION`

### Configuration & Samples:
The repository includes environment hints and scripts to run local samples, such as:
- Local CSV reading (directory and filename settings)
- Cloud object store CSV reading (bucket and object keys)
- MongoDB sink (connection and collection settings)
- Targets to launch local dependencies (e.g., standalone Mongo) for tinkering

Check the sample environment files and make targets for a quick local demo.

### Recipes:
#### CSV → Mongo:
- Source: a CSV reader that maps rows into a simple key/value record.
- Sink: a Mongo writer that widens records to document maps and inserts them.
- Tuning: choose BatchSize for throughput and offset cadence; configure Continue-as-New every N batches to cap history size.

#### Streaming source/sink (contrib idea):
Extending from “pull next batch” to fully streaming:
- Define streaming read contracts that expose bounded channels and error channels.
- Define streaming write contracts that consume from channels with back-pressure.
- Add a small “queue executor” activity to bridge streams in a deterministic way (bounded buffers, checkpoints, and heartbeats).
- Keep the workflow orchestration unchanged; only the executor and concrete sources/sinks differ.

Open a proposal if you’d like to collaborate on a general streaming extension.

### Contributing:
Contributions are welcome. High-impact areas:
- New Sources and Sinks (cloud stores, databases, message brokers)
- Streaming variants and a pluggable executor
- Better examples and quick-start demos
- Observability (metrics/tracing), progress reporting, and heartbeats
- Documentation improvements and architecture diagrams
Principles:
- Maintain workflow determinism; keep side effects in activities.
- Register activities/workflows with aliases and call by those names.
- Add tests using the Temporal test suite; mock external systems with small fakes.
- Document configuration knobs and environment variables for new integrations.

### Batch process workflow flow:

![Process Flow](/process-flow-1.png)
