# Batch Orchestra
**Batch Orchestra** is a reusable Temporal (Go SDK) module for building batch data pipelines, from any Source to any Sink, with type-safe generics, clear extension points, and proven workflow patterns.

It orchestrates the boring but hard parts: chunked reads, resilient writes, retries and backoff, Continue-as-New to keep history small, and first-class progress snapshots and per-activity retry policies so you can operate pipelines with confidence.

## Overview:

### Why Batch Orchestra:
Real pipelines repeatedly solve the same problems:
- **Resumability:** offsets/cursors and idempotency across retries
- **Stability:** bounded workflow history via Continue-as-New
- **Resilience:** transient failures vs. non-retryable ones
- **Observability:** knowing “how far we got” without bloating history
- **Extensibility:** add new sources/sinks without touching the workflow
- **Concurrent batch execution - pipelined** fetch→write with an internal bounded queue so multiple batches can be **in-flight concurrently**; `MaxInProcessBatches` controls the buffer size and provides back-pressure when sinks are slower than sources

Batch Orchestra gives you a ready-to-use orchestration layer so you implement only **how to read** the next batch, **how to write** a batch & where to **save snapshots**. Everything else is handled for you, deterministically.

It’s the Go version inspired by [Drew Hoskins's](https://github.com/drewhoskins/batch-orchestra) Batch Orchestra [python](https://github.com/drewhoskins/batch-orchestra) implementation

### Architecture overview:
At a high level, a single **generic workflow** drives a simple, **pipelined** loop:
1. **Fetch:** calls the Source’s fetch activity to read up to N items starting at the current offset.
2. **Queue & back-pressure** Fetched batches are placed on an internal in-memory queue inside the workflow. The queue buffers up to MaxInProcessBatches batches. If the queue is full, fetching pauses until the writer drains it. This decouples read throughput from write throughput and keeps memory bounded deterministically.
3. **Write:** calls the Sink’s write activity to persist the batch.
4. **Advance & Observe:** updates offset and counters. (optional but recommended): persists a compact batch result record.
6. **Continue-as-New** before system thresholds are met to keep event history small.

All side effects live in activities; the workflow logic remains deterministic.

Concurrency is achieved by pipelining (fetch ahead + queued batches) and Temporal’s ability to have multiple activities in flight; the queue bounds and activity options control back-pressure and retry semantics.


### Core Concepts:
#### Request (runtime config & state)
The request carries:
- **Run configuration:** batch size, max batches per run, max in process batches, job ID, start offset.
- **Bindings:** concrete **Source** config and **Sink** config.
- **Policies:** a declarative **map of retry policies by activity alias** (see below).
- **Snapshotter config:** where to persist batch result snapshots before workflow completion or continuing as new.
- **Minimal state:** current offset and aggregate counters.

Only the state required to resume is carried across runs; richer state is snapshotted externally.

#### Source & Sink (pluggable I/O)
- **Source** config builds a `Source` instance; `Source` returns batch populated with `records`, `next offset`, and `done`.
- **Sink** config builds a `Sink` instance; `Sink` writes a batch and returns write metadata.
- **Snapshot** config builds a `Snapshotter` instance; `Snapshotter` persists workflow result snapshot.

These are the only places you write domain-specific code. Everything else is generic.

#### Activity aliases (deterministic wiring)
Generics can produce unstable function names (e.g., “func1”). To avoid that, activities and workflows are to be **registered with explicit aliases** generated based on **source/sink/snapshotter** dependencies. The batch process workflow **calls activities by alias**, preventing “activity not registered” errors and keeps wiring explicit.


 ### Quick start:
 #### Install:
 - Go 1.23+
 - `go get github.com/hankgalt/batch-orchestra`
 - `go mod tidy`
#### Define a Source:
- Create a `SourceConfig` that can build a `Source` instance (open files, connect to cloud/object stores, etc.).
- Implement a `Source` that yields batches of records and advances an offset (the offset should not count header rows for CSV-style inputs).
- Keep reading logic tolerant of ragged data when appropriate and idempotent across retries.
- See `internal/sources/` for examples
#### Define a Sink:
- Create a `SinkConfig` that can build a `Sink` instance (e.g., a DB writer, an API producer, a file appender).
- Implement a `Sink` that writes a batch and returns counts/metadata; stop on the first hard error to propagate failure back to the workflow.
- Keep writing logic idempotent when possible; rely on Temporal retries for transient issues.
- See `internal/sinks/` for examples
#### Define Snapshotter
- See `internal/snapshotters` for examples
#### Define Concrete types
Define Request types & create activity & workflow aliases for configured **source/sink/snapshotter**. See examples in `workflow_test.go` & `activities_test.go`.
#### Register activities/workflow (with aliases):
- Always register activities and workflows with explicit names (aliases) derived from configured **source/sink/snapshotter**. With generics, reflection-derived names can be unstable (e.g., “func1”), which breaks resolution.
- The workflow uses the same generated aliases, when scheduling activities.
#### Start a workflow run:
- Construct a `BatchProcessingRequest` with concrete `SourceConfig`, `SinkConfig` && `SnapshotterConfig`.
- Choose a reasonable `BatchSize`, `MaxInProcessBatchSize` and initial `StartAt` offset
- Execute the registered workflow name on your Temporal task queue as main workflow or as childworkflow for relevant usecase

### Testing:
Use Temporal’s Go SDK test suite to run workflows and activities deterministically:
- Register your concrete generic instantiations with explicit aliases.
- Execute the workflow by its alias and assert on observable effects (e.g., rows written, offsets advanced, errors propagated).
- For external systems, provide fake clients that capture inputs and inject failures.
- If you need to examine a **Continue-as-New** handoff during tests, disable run-following and decode the next-run input from the error payloads with the test environment’s data converter.
- See examples `workflow_test.go` & `activities_test.go`.

Guidelines:
- Keep IO in activities and use the test harness to supply activity context (or inject a background context via worker options).
- Prefer small, table-driven tests for sources and sinks; and one or two end-to-end tests per recipe.

Run sample test workflows and test activities:
- Local dev server
	- `start-dev-server`
- Sources/Sinks: Create `env/test.env` & add following env vars. 
	- for cloud CSV (GCS only) source
		- (`GOOGLE_APPLICATION_CREDENTIALS, BUCKET, FILE_NAME`)
	- for local CSV source
		- `DATA_DIR, FILE_NAME`
		- Add a sample csv file with headers.

### Configuration & Samples:
The repository includes environment hints and scripts to run local samples, such as:
- **Local CSV** reading (directory and filename settings)
- **Cloud object store CSV** reading (bucket and object keys)
- **SQLLiteDB** sink (connection and collection settings)
- Targets to launch local dependencies (e.g., standalone temporal server) for tinkering

Check the sample environment files and make targets for a quick local demo.

### Recipes:
#### CSV → SQLLite:
- Source: a CSV reader (local/cloud) that maps rows into a simple key/value record.
- Sink: a SQLLite writer that widens records to document maps and inserts them.
- Tuning: choose `BatchSize`, `MaxInProcessBatches` for throughput and offset cadence; configure Continue-as-New every `MaxBatches` to cap history size.

#### Roadmaps & contribution ideas:
- **Streaming read/write** executors with bounded buffers and back-pressure
- **Metrics/Tracing** helpers and dashboards
- Additional **snapshotters** (object storage, SQL, observability backends), **sources** (cloud object stores, message logs) & **sinks** (data warehouses, search indices)
- Example environments and documentation
- Spec hardening & test coverage.

Open a proposal if you’d like to collaborate on a general streaming extension.

### Operational tips:
- Tune `MaxInProcessBatches` based on source/sink speeds.
- Use **source** config to build relevant **snapshotter** config that saves snapshots at same location as source.
- Keep snapshots **compact** and focused on what you’ll actually use to resume or audit.
- Surface minimal state in **Search Attributes** (e.g., total processed, last offset, job ID) for quick visibility in Temporal Web.
- Keep I/O in **activities**; workflows must stay deterministic.
- Register and call by **aliases**.
- Document configuration knobs for any new source/sink/snapshotter.

### Batch process workflow flow:

![Process Flow](/process-flow-1.png)
