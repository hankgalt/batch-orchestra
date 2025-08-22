package domain

import (
	"context"
	"sort"
	"strings"
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

type Rule struct {
	Target   string // this value replaces the header in the CSV file
	Group    bool   // if true, include this Target column's value in a grouped field
	NewField string // if has value, include Target as new field with this value
	Order    int    // order of the rule in the mapping
}

// TransformerFunc transforms a slice of values into a key-value map based on, in closure, headers and rules.
type TransformerFunc func(values []string) map[string]any

// A request to process a batch of CSVRow from a local CSV file.
// type LocalCSVRequest = BatchProcessingRequest[CSVRow, LocalCSVConfig, NoopSinkConfig[CSVRow]]

// // A request to process a batch of CSVRow from a cloud CSV source (S3/GCS/Azure).
// type CloudCSVRequest = BatchProcessingRequest[CSVRow, CloudCSVConfig, NoopSinkConfig[CSVRow]]

// A request to process a batch of CSVRow and write to a MongoDB sink.
// type LocalCSVMongoRequest = BatchProcessingRequest[CSVRow, sources.LocalCSVConfig, sinks.MongoSinkConfig[CSVRow]]

// // A request to process a batch of CSVRow from a cloud CSV source and write to a MongoDB sink.
// type CloudCSVMongoRequest = BatchProcessingRequest[CSVRow, CloudCSVConfig, MongoSinkConfig[CSVRow]]

type CloudFileConfig struct {
	Name   string
	Path   string
	Bucket string
}

func BuildBusinessModelTransformRules() map[string]Rule {
	return map[string]Rule{
		"ENTITY_NUM":                  {Target: "ENTITY_ID"},                                // rename to ENTITY_ID
		"FIRST_NAME":                  {Target: "NAME", Group: true, Order: 1},              // group into NAME
		"MIDDLE_NAME":                 {Target: "NAME", Group: true, Order: 2},              // group into NAME
		"LAST_NAME":                   {Target: "NAME", Group: true, Order: 3},              // group into NAME
		"PHYSICAL_ADDRESS":            {Target: "ADDRESS"},                                  // rename to ADDRESS
		"PHYSICAL_ADDRESS1":           {Target: "ADDRESS", Group: true, Order: 1},           // group into ADDRESS
		"PHYSICAL_ADDRESS2":           {Target: "ADDRESS", Group: true, Order: 2},           // group into ADDRESS
		"PHYSICAL_ADDRESS3":           {Target: "ADDRESS", Group: true, Order: 3},           // group into ADDRESS
		"PHYSICAL_CITY":               {Target: "ADDRESS", Group: true, Order: 4},           // group into ADDRESS
		"PHYSICAL_STATE":              {Target: "ADDRESS", Group: true, Order: 5},           // group into ADDRESS
		"PHYSICAL_POSTAL_CODE":        {Target: "ADDRESS", Group: true, Order: 6},           // group into ADDRESS
		"PHYSICAL_COUNTRY":            {Target: "ADDRESS", Group: true, Order: 7},           // group into ADDRESS
		"ADDRESS1":                    {Target: "ADDRESS", Group: true, Order: 1},           // group into ADDRESS
		"ADDRESS2":                    {Target: "ADDRESS", Group: true, Order: 2},           // group into ADDRESS
		"ADDRESS3":                    {Target: "ADDRESS", Group: true, Order: 3},           // group into ADDRESS
		"CITY":                        {Target: "ADDRESS", Group: true, Order: 4},           // group into ADDRESS
		"STATE":                       {Target: "ADDRESS", Group: true, Order: 5},           // group into ADDRESS
		"POSTAL_CODE":                 {Target: "ADDRESS", Group: true, Order: 6},           // group into ADDRESS
		"COUNTRY":                     {Target: "ADDRESS", Group: true, Order: 7},           // group into ADDRESS
		"PRINCIPAL_ADDRESS":           {Target: "PRINCIPAL_ADDRESS", Group: true, Order: 1}, // group into PRINCIPAL_ADDRESS
		"PRINCIPAL_ADDRESS1":          {Target: "PRINCIPAL_ADDRESS", Group: true, Order: 2}, // group into PRINCIPAL_ADDRESS
		"PRINCIPAL_ADDRESS2":          {Target: "PRINCIPAL_ADDRESS", Group: true, Order: 3}, // group into PRINCIPAL_ADDRESS
		"PRINCIPAL_CITY":              {Target: "PRINCIPAL_ADDRESS", Group: true, Order: 4}, // group into PRINCIPAL_ADDRESS
		"PRINCIPAL_STATE":             {Target: "PRINCIPAL_ADDRESS", Group: true, Order: 5}, // group into PRINCIPAL_ADDRESS
		"PRINCIPAL_POSTAL_CODE":       {Target: "PRINCIPAL_ADDRESS", Group: true, Order: 6}, // group into PRINCIPAL_ADDRESS
		"PRINCIPAL_COUNTRY":           {Target: "PRINCIPAL_ADDRESS", Group: true, Order: 7}, // group into PRINCIPAL_ADDRESS
		"MAILING_ADDRESS":             {Target: "MAILING_ADDRESS", Group: true, Order: 1},   // group into MAILING_ADDRESS
		"MAILING_ADDRESS1":            {Target: "MAILING_ADDRESS", Group: true, Order: 2},   // group into MAILING_ADDRESS
		"MAILING_ADDRESS2":            {Target: "MAILING_ADDRESS", Group: true, Order: 3},   // group into MAILING_ADDRESS
		"MAILING_ADDRESS3":            {Target: "MAILING_ADDRESS", Group: true, Order: 4},   // group into MAILING_ADDRESS
		"MAILING_CITY":                {Target: "MAILING_ADDRESS", Group: true, Order: 5},   // group into MAILING_ADDRESS
		"MAILING_STATE":               {Target: "MAILING_ADDRESS", Group: true, Order: 6},   // group into MAILING_ADDRESS
		"MAILING_POSTAL_CODE":         {Target: "MAILING_ADDRESS", Group: true, Order: 7},   // group into MAILING_ADDRESS
		"MAILING_COUNTRY":             {Target: "MAILING_ADDRESS", Group: true, Order: 8},   // group into MAILING_ADDRESS
		"PRINCIPAL_ADDRESS_IN_CA":     {Target: "ADDRESS_IN_CA", Group: true, Order: 1},     // group into ADDRESS_IN_CA
		"PRINCIPAL_ADDRESS1_IN_CA":    {Target: "ADDRESS_IN_CA", Group: true, Order: 2},     // group into ADDRESS_IN_CA
		"PRINCIPAL_ADDRESS2_IN_CA":    {Target: "ADDRESS_IN_CA", Group: true, Order: 3},     // group into ADDRESS_IN_CA
		"PRINCIPAL_CITY_IN_CA":        {Target: "ADDRESS_IN_CA", Group: true, Order: 4},     // group into ADDRESS_IN_CA
		"PRINCIPAL_STATE_IN_CA":       {Target: "ADDRESS_IN_CA", Group: true, Order: 5},     // group into ADDRESS_IN_CA
		"PRINCIPAL_POSTAL_CODE_IN_CA": {Target: "ADDRESS_IN_CA", Group: true, Order: 6},     // group into ADDRESS_IN_CA
		"PRINCIPAL_COUNTRY_IN_CA":     {Target: "ADDRESS_IN_CA", Group: true, Order: 7},     // group into ADDRESS_IN_CA
		"POSITION_TYPE":               {Target: "AGENT_TYPE", NewField: "Principal"},        // new Target field AGENT_TYPE with value Principal
	}
}

// BuildTransformerWithRules creates a transformer function based on the provided headers and rules.
// The rules map defines how to transform each header:
//   - If a header maps to an empty string, it is used as it is.
//   - If a header maps to rename rule, it is renamed to target.
//   - If a header maps to a group rule,
//     the values of grouped headers are grouped as string and set to new target field.
//     Order determines position in grouped string. Original headers are ignored.
//   - If a header maps to a new field rule,
//     a new target field is added with the newField value.
//     Original header is used as it is.
//
// The transformer function takes a slice of string values (CSV record) and returns a map[string]any
// where keys are the transformed header names and values are the corresponding record values.
//
// Example usage:
//
//	 For headers:
//		headers := []string{"ENTITY_NUM", "PHYSICAL_CITY", "PHYSICAL_ADDRESS", "WORK_ADDRESS", "POSITION_TYPE", "STATUS"}
//
//	 And rules:
//		rules := map[string]Rule{
//		  "ENTITY_NUM": {Target: "ENTITY_ID"},        // rename
//		  "PHYSICAL_CITY": {Target: "ADDRESS", Group: true, Order: 2},      // group into ADDRESS as second token
//		  "PHYSICAL_ADDRESS": {Target: "ADDRESS", Group: true, Order: 1},    // group into ADDRESS as first token
//		  "WORK_ADDRESS": {Target: "WORK_ADDR"},      // rename into WORK_ADDR
//		  "POSITION_TYPE": {Target: "AGENT_TYPE", NewField: "Principal"}, // new Target field AGENT_TYPE with value Principal
//		}
//
//	The transformer function will then transform following CSV record:
//		["A5639", "New York", "123 Main St", "7648 Gotham St New York NY", "Manager", ""]
//
//	Into:
//		map[string]any{
//	  		"entity_id": "A5639",
//	  		"address": "123 Main St New York",
//	  		"work_addr": "7648 Gotham St New York NY",
//	  		"agent_type": "Principal",
//	  		"position_type": "Manager",
//	  		"status": "",
//		}
//
// allowing for flexible transformations of CSV records based on the provided headers and rules.
func BuildTransformerWithRules(headers []string, rules map[string]Rule) TransformerFunc {
	type colPlan struct {
		target   string
		group    bool
		newField string
		source   string
		order    int
		val      string
	}
	plan := make([]colPlan, len(headers))
	for i, h := range headers {
		if r, ok := rules[h]; ok && r.Target != "" {
			plan[i] = colPlan{source: h, target: r.Target, group: r.Group, newField: r.NewField, order: r.Order}
		} else {
			plan[i] = colPlan{source: h, target: h}
		}
	}

	return func(values []string) map[string]any {
		out := make(map[string]any, len(plan))
		// groupParts := map[string][]string{}
		groupParts := map[string][]colPlan{}
		n := len(values)

		for i := 0; i < len(plan) && i < n; i++ {
			var val string
			if i < n {
				val = strings.TrimSpace(values[i])
			} else {
				val = "" // ensure presence for non-grouped keys even if value slice is short
			}

			p := plan[i]
			if p.group {
				// Skip empty parts for grouped targets
				if val != "" {
					p.val = val // Store the value in the plan for grouping
					groupParts[p.target] = append(groupParts[p.target], p)
				}
			} else {
				// Always set the key, even if val == ""
				if p.newField != "" {
					out[strings.ToLower(p.target)] = p.newField
					out[strings.ToLower(p.source)] = val
				} else {
					out[strings.ToLower(p.target)] = val
				}
			}
		}

		// sort grouped parts by order
		for target, parts := range groupParts {
			// Sort parts by order
			if len(parts) > 1 {
				sort.Slice(parts, func(i, j int) bool {
					return parts[i].order < parts[j].order
				})
				// Materialize grouped fields (join non-empty parts with single spaces)
				var joinedParts []string
				for _, p := range parts {
					if p.val != "" {
						joinedParts = append(joinedParts, p.val)

					}
				}
				out[strings.ToLower(target)] = strings.TrimSpace(strings.Join(joinedParts, " "))
			}
		}

		return out
	}
}
