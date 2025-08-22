package sources

import (
	"bytes"
	"context"
	"encoding/csv"
	"errors"
	"fmt"
	"io"
	"log"
	"os"

	"cloud.google.com/go/storage"

	"github.com/hankgalt/batch-orchestra/pkg/domain"
	"github.com/hankgalt/batch-orchestra/pkg/utils"
)

const (
	CloudCSVSource = "cloud-csv-source"
)

type CloudSource string

const (
	CloudSourceGCS   CloudSource = "gcs"
	CloudSourceS3    CloudSource = "s3"
	CloudSourceAzure CloudSource = "azure"
)

// GCPStorageReadAtAdapter is an adapter for GCP Storage reader to implement ReadAt interface.
type GCPStorageReadAtAdapter struct {
	Reader *storage.Reader // storage.Reader is a GCP Storage reader
}

// ReadAt reads data from the GCP Storage reader at the specified offset.
func (g *GCPStorageReadAtAdapter) ReadAt(p []byte, off int64) (n int, err error) {
	if g.Reader == nil {
		return 0, errors.New("cloud csv: reader is nil")
	}

	// seek to the specified offset
	_, err = io.CopyN(io.Discard, g.Reader, off)
	if err != nil {
		return 0, err
	}

	return g.Reader.Read(p)
}

// Cloud CSV (S3/GCS/Azure) source.
type cloudCSVSource struct {
	provider  string // e.g., "s3", "gcs"
	path      string
	bucket    string
	delimiter rune
	hasHeader bool
	transFunc domain.TransformerFunc // transformer function to apply to each row
	client    *storage.Client        // GCP Storage client, if needed // GCP Storage client, if using GCS
}

func (s *cloudCSVSource) Close(ctx context.Context) error {
	return s.client.Close()
}

// Name of the source.
func (s *cloudCSVSource) Name() string { return CloudCSVSource }

func (s *cloudCSVSource) NextStream(
	ctx context.Context,
	offset uint64,
	size uint,
) (<-chan *domain.BatchRecord[domain.CSVRow], error) {
	// If size is 0 or negative, return an empty batch.
	if size <= 0 {
		return nil, fmt.Errorf("cloud csv: size must be greater than 0")
	}

	// Ensure client is initialized
	if s.client == nil {
		return nil, errors.New("cloud csv: client is not initialized")
	}

	// If headers are enabled but transformer function is not set.
	if s.hasHeader && s.transFunc == nil {
		return nil, fmt.Errorf("cloud csv: transformer function is not set for cloud CSV source with headers")
	}

	// Ensure object exists in the bucket
	obj := s.client.Bucket(s.bucket).Object(s.path)
	if _, err := obj.Attrs(ctx); err != nil {
		return nil, fmt.Errorf("cloud csv: object does not exist or error getting attributes: %w", err)
	}

	// Create a reader for the object
	rc, err := obj.NewReader(ctx)
	if err != nil {
		return nil, fmt.Errorf("cloud csv: error creating reader for object %s in bucket %s: %w", s.path, s.bucket, err)
	}
	defer func() {
		if err := rc.Close(); err != nil {
			log.Printf("cloud csv: error closing reader: %v", err)
		}
	}()

	// Set start index & done flag.
	startIndex := int64(offset)
	done := false

	// Create a read-at adapter for the GCP Storage reader.
	// This allows us to read data at specific offsets.
	readAtAdapter := &GCPStorageReadAtAdapter{
		Reader: rc,
	}

	// Read data bytes from the object at the specified offset
	data := make([]byte, size)
	numBytesRead, err := readAtAdapter.ReadAt(data, startIndex)
	if err != nil && err != io.EOF {
		return nil, fmt.Errorf("error reading object %s in bucket %s at offset %d: %w", s.path, s.bucket, startIndex, err)
	}

	// If read data is less than requested, cursor reached EOF, set Done
	if uint(numBytesRead) < size {
		done = true
	}

	// get last line break index to avoid partial record
	i := bytes.LastIndex(data, []byte{'\n'})
	if i < 0 {
		// If no newline found, read till the end, set nextOffset to numBytesRead
		i = numBytesRead - 1
	}

	resStream := make(chan *domain.BatchRecord[domain.CSVRow])

	if done {
		resStream <- &domain.BatchRecord[domain.CSVRow]{
			Start: offset,
			End:   offset,
			Done:  done,
		}
	}

	go func() {
		defer close(resStream)

		// create data buffer for bytes upto last line break
		buffer := bytes.NewBuffer(data[:i+1])

		// Create a CSV reader with the buffer
		csvReader := csv.NewReader(buffer)
		csvReader.Comma = s.delimiter
		csvReader.FieldsPerRecord = -1 // Read all fields

		// Initialize start & next offsets
		nextOffset := csvReader.InputOffset()

		// Initialize read count and records slice
		readCount := 0

		// Read records from the CSV reader
		for {
			// allow cancellation
			select {
			case <-ctx.Done():
				return
			default:
			}

			rec, err := csvReader.Read()
			if err != nil {
				if errors.Is(err, io.EOF) {
					return
				}
				// Attempt record cleanup if error occurs
				cleanedStr := utils.CleanRecord(string(data[nextOffset:csvReader.InputOffset()]))
				record, err := utils.ReadSingleRecord(cleanedStr)
				if err != nil {
					resStream <- &domain.BatchRecord[domain.CSVRow]{
						Start: uint64(nextOffset),
						End:   uint64(csvReader.InputOffset()),
						BatchResult: domain.BatchResult{
							Error: fmt.Sprintf("read data row: %v", err),
						},
					}

					// Update startIndex to the next record's offset
					startIndex = nextOffset

					// update nextOffset to the next record's offset
					nextOffset = csvReader.InputOffset()
					// update row read count
					readCount++

					continue
				}

				rec = record
			}

			// startIndex & nextOffset will be 0 for the first read, skip headers
			if startIndex <= 0 && nextOffset <= 0 && s.hasHeader {
				// Update startIndex to the next record's offset
				startIndex = nextOffset

				// update nextOffset to the next record's offset
				nextOffset = csvReader.InputOffset()

				// update row read count
				readCount++

				continue
			}

			// Update startIndex to the next record's offset
			startIndex = nextOffset

			// update nextOffset to the next record's offset
			nextOffset = csvReader.InputOffset()

			// update row read count
			readCount++

			// Create a CSVRow from the transformed record
			rec = utils.CleanAlphaNumericsArr(rec, []rune{'.', '-', '_', '#', '&', '@'})
			res := s.transFunc(rec)

			row := domain.CSVRow{}
			for k, v := range res {
				st, ok := v.(string)
				if !ok {
					row[k] = ""
				}
				row[k] = st
			}

			resStream <- &domain.BatchRecord[domain.CSVRow]{
				Start: uint64(startIndex),
				End:   uint64(csvReader.InputOffset()),
				Data:  row,
			}
		}
	}()

	return resStream, nil
}

// Next reads the next batch of CSV rows from the cloud storage (S3/GCS/Azure).
// It reads from the cloud storage at the specified offset and returns a batch of CSVRow.
// Currently only supports GCP Storage. Ensure the environment variable is set for GCP credentials
func (s *cloudCSVSource) Next(
	ctx context.Context,
	offset uint64,
	size uint,
) (*domain.BatchProcess[domain.CSVRow], error) {
	bp := &domain.BatchProcess[domain.CSVRow]{
		Records:     nil,
		NextOffset:  offset,
		StartOffset: offset,
		Done:        false,
	}

	// If size is 0 or negative, return an empty batch.
	if size <= 0 {
		return bp, nil
	}

	// Ensure client is initialized
	if s.client == nil {
		return bp, errors.New("cloud csv: client is not initialized")
	}

	// If headers are enabled but transformer function is not set.
	if s.hasHeader && s.transFunc == nil {
		return bp, fmt.Errorf("cloud csv: transformer function is not set for cloud CSV source with headers")
	}

	// Ensure object exists in the bucket
	obj := s.client.Bucket(s.bucket).Object(s.path)
	if _, err := obj.Attrs(ctx); err != nil {
		return bp, fmt.Errorf("cloud csv: object does not exist or error getting attributes: %w", err)
	}

	// Create a reader for the object
	rc, err := obj.NewReader(ctx)
	if err != nil {
		return bp, fmt.Errorf("cloud csv: error creating reader for object %s in bucket %s: %w", s.path, s.bucket, err)
	}
	defer func() {
		if err := rc.Close(); err != nil {
			log.Printf("cloud csv: error closing reader: %v", err)
		}
	}()

	// Set start index & done flag.
	startIndex := int64(offset)
	done := false

	// Create a read-at adapter for the GCP Storage reader.
	// This allows us to read data at specific offsets.
	readAtAdapter := &GCPStorageReadAtAdapter{
		Reader: rc,
	}

	// Read data bytes from the object at the specified offset
	data := make([]byte, size)
	numBytesRead, err := readAtAdapter.ReadAt(data, startIndex)
	if err != nil && err != io.EOF {
		return bp, fmt.Errorf("error reading object %s in bucket %s at offset %d: %w", s.path, s.bucket, startIndex, err)
	}

	// If read data is less than requested, cursor reached EOF, set Done
	if uint(numBytesRead) < size {
		done = true
	}

	// get last line break index to avoid partial record
	i := bytes.LastIndex(data, []byte{'\n'})
	if i < 0 {
		// If no newline found, read till the end, set nextOffset to numBytesRead
		i = numBytesRead - 1
	}

	// create data buffer for bytes upto last line break
	buffer := bytes.NewBuffer(data[:i+1])

	// Create a CSV reader with the buffer
	csvReader := csv.NewReader(buffer)
	csvReader.Comma = s.delimiter
	csvReader.FieldsPerRecord = -1 // Read all fields

	// Initialize start & next offsets
	nextOffset := csvReader.InputOffset()

	// Initialize read count and records slice
	readCount := 0

	// Read records from the CSV reader
	for {
		// allow cancellation
		select {
		case <-ctx.Done():
			return bp, ctx.Err()
		default:
		}

		rec, err := csvReader.Read()
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			// Attempt record cleanup if error occurs
			cleanedStr := utils.CleanRecord(string(data[nextOffset:csvReader.InputOffset()]))
			record, err := utils.ReadSingleRecord(cleanedStr)
			if err != nil {
				bp.Records = append(bp.Records, &domain.BatchRecord[domain.CSVRow]{
					Start: uint64(startIndex),
					End:   uint64(csvReader.InputOffset()),
					BatchResult: domain.BatchResult{
						Error: fmt.Sprintf("read data row: %v", err),
					},
				})

				// Update startIndex to the next record's offset
				startIndex = nextOffset

				// update nextOffset to the next record's offset
				nextOffset = csvReader.InputOffset()

				// update row read count
				readCount++

				continue
			}

			rec = record
		}

		// startIndex & nextOffset will be 0 for the first read, validate headers
		if startIndex <= 0 && nextOffset <= 0 && s.hasHeader {
			// Update startIndex to the next record's offset
			startIndex = nextOffset

			// update nextOffset to the next record's offset
			nextOffset = csvReader.InputOffset()

			// update row read count
			readCount++

			continue
		}

		// Create a BatchRecord for the current record
		br := domain.BatchRecord[domain.CSVRow]{
			Start: uint64(startIndex),
			End:   uint64(csvReader.InputOffset()),
		}

		// Update startIndex to the next record's offset
		startIndex = nextOffset

		// update nextOffset to the csvReader's offset
		nextOffset = csvReader.InputOffset()

		// Update read count
		readCount++

		// Create a CSVRow from the transformed record
		rec = utils.CleanAlphaNumericsArr(rec, []rune{'.', '-', '_', '#', '&', '@'})
		res := s.transFunc(rec)

		row := domain.CSVRow{}
		for k, v := range res {
			st, ok := v.(string)
			if !ok {
				row[k] = ""
			}
			row[k] = st
		}
		br.Data = row

		// Update records slice
		bp.Records = append(bp.Records, &br)
	}

	bp.NextOffset = offset + uint64(nextOffset)
	bp.Done = done

	return bp, nil
}

// Cloud CSV (S3/GCS/Azure) - source config.
type CloudCSVConfig struct {
	Provider     string // "s3"|"gcs"|...
	Bucket       string
	Path         string
	Delimiter    rune // e.g., ',', '|'
	HasHeader    bool
	MappingRules map[string]domain.Rule
}

// Name of the source.
func (c CloudCSVConfig) Name() string { return CloudCSVSource }

// BuildSource builds a cloud CSV source from the config.
func (c CloudCSVConfig) BuildSource(ctx context.Context) (domain.Source[domain.CSVRow], error) {
	// build s3/gcs/azure client from c.Provider, bucket, key

	if c.Path == "" {
		return nil, errors.New("cloud csv: object path is required")
	}

	if c.Bucket == "" {
		return nil, errors.New("cloud csv: bucket name is required")
	}

	if c.Delimiter == 0 {
		c.Delimiter = ',' // default
	}

	if c.Provider == "" {
		c.Provider = "gcs" // default to GCS
	}

	if c.Provider != "gcs" {
		return nil, errors.New("cloud csv: unsupported provider, only 'gcs' is supported")
	}

	// Ensure the environment variable is set for GCP credentials
	cPath := os.Getenv("GOOGLE_APPLICATION_CREDENTIALS")
	if cPath == "" {
		return nil, errors.New("cloud csv: missing credentials path")
	}

	client, err := storage.NewClient(ctx)
	if err != nil {
		return nil, errors.New("cloud csv: failed to create storage client: " + err.Error())
	}

	obj := client.Bucket(c.Bucket).Object(c.Path)
	if _, err = obj.Attrs(ctx); err != nil {
		//
		if err := client.Close(); err != nil {
			log.Printf("cloud csv: error closing client: %v", err)
		}
		return nil, errors.New("cloud csv: object does not exist or error getting attributes: " + err.Error())
	}

	rc, err := obj.NewReader(ctx)
	if err != nil {
		return nil, fmt.Errorf("cloud csv: error creating reader for object %s in bucket %s: %w", c.Path, c.Bucket, err)
	}
	defer func() {
		if err := rc.Close(); err != nil {
			log.Printf("cloud csv: error closing reader: %v", err)
		}
	}()

	src := &cloudCSVSource{
		provider:  c.Provider,
		bucket:    c.Bucket,
		path:      c.Path,
		delimiter: c.Delimiter,
		hasHeader: c.HasHeader,
	}

	if c.HasHeader {
		r := csv.NewReader(rc)
		r.Comma = c.Delimiter
		r.FieldsPerRecord = -1

		h, err := r.Read()
		if err != nil {
			if errors.Is(err, io.EOF) {
				// empty file: treat as no headers
				h = nil
			} else {
				return nil, fmt.Errorf("cloud csv: read header: %w", err)
			}
		}
		headers := utils.CleanHeaders(h)

		// build transformer function
		var rules map[string]domain.Rule
		if len(c.MappingRules) > 0 {
			rules = c.MappingRules
		} else {
			// If no mapping rules are provided, use default rules
			rules = domain.BuildBusinessModelTransformRules()
		}
		transFunc := domain.BuildTransformerWithRules(headers, rules)
		src.transFunc = transFunc
	}

	src.client = client

	return src, nil
}

// ReadCSVRows reads CSV rows from the given data buffer using the specified delimiter.
// It returns a slice of records and an error if any.
func ReadCSVRows(data []byte, delimiter rune) ([][]string, error) {
	// create data buffer for bytes upto last line break
	buffer := bytes.NewBuffer(data)

	// Create a CSV reader with the buffer
	csvReader := csv.NewReader(buffer)
	csvReader.Comma = delimiter
	csvReader.FieldsPerRecord = -1 // Read all fields

	// Initialize next offset
	nextOffset := csvReader.InputOffset()

	// Initialize read count and records slice
	readCount := 0
	records := [][]string{}

	// Read records from the CSV reader
	for {
		rec, err := csvReader.Read()
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			// Attempt record cleanup if error occurs
			cleanedStr := utils.CleanRecord(string(data[nextOffset:csvReader.InputOffset()]))
			record, err := utils.ReadSingleRecord(cleanedStr)
			if err != nil {
				return nil, fmt.Errorf("read data row: %w", err)
			}

			rec = record
		}

		// update nextOffset to the next record's offset
		nextOffset = csvReader.InputOffset()

		// Update read count
		readCount++

		// Update records slice
		records = append(records, rec)
	}

	return records, nil
}
