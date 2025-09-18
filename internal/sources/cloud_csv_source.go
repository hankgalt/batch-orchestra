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

// Error constants and variables
const (
	ErrMsgCloudCSVReaderNil            = "cloud csv: reader is nil"
	ErrMsgCloudCSVSizeMustBePositive   = "cloud csv: size must be greater than 0"
	ErrMsgCloudCSVClientNotInitialized = "cloud csv: client is not initialized"
	ErrMsgCloudCSVObjectPathRequired   = "cloud csv: object path is required"
	ErrMsgCloudCSVBucketRequired       = "cloud csv: bucket name is required"
	ErrMsgCloudCSVUnsupportedProvider  = "cloud csv: unsupported provider, only 'gcs' is supported"
	ErrMsgCloudCSVMissingCreds         = "cloud csv: missing credentials path"
	ErrMsgCloudCSVObjectNotExist       = "cloud csv: object does not exist or error getting attributes"
	ErrMsgCloudCSVTransformerNil       = "cloud csv: transformer function is not set for cloud CSV source with headers"
)

var (
	ErrCloudCSVReaderNil            = errors.New(ErrMsgCloudCSVReaderNil)
	ErrCloudCSVSizeMustBePositive   = errors.New(ErrMsgCloudCSVSizeMustBePositive)
	ErrCloudCSVClientNotInitialized = errors.New(ErrMsgCloudCSVClientNotInitialized)
	ErrCloudCSVObjectPathRequired   = errors.New(ErrMsgCloudCSVObjectPathRequired)
	ErrCloudCSVBucketRequired       = errors.New(ErrMsgCloudCSVBucketRequired)
	ErrCloudCSVUnsupportedProvider  = errors.New(ErrMsgCloudCSVUnsupportedProvider)
	ErrCloudCSVMissingCreds         = errors.New(ErrMsgCloudCSVMissingCreds)
	ErrCloudCSVObjectNotExist       = errors.New(ErrMsgCloudCSVObjectNotExist)
	ErrCloudCSVTransformerNil       = errors.New(ErrMsgCloudCSVTransformerNil)
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
		return 0, ErrCloudCSVReaderNil
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
	size      int64
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
	offset string,
	size uint,
) (<-chan *domain.BatchRecord, error) {
	// If size is 0 or negative, return an empty batch.
	if size <= 0 {
		return nil, ErrCloudCSVSizeMustBePositive
	}

	// Ensure client is initialized
	if s.client == nil {
		return nil, ErrCloudCSVClientNotInitialized
	}

	// If headers are enabled but transformer function is not set.
	if s.hasHeader && s.transFunc == nil {
		return nil, ErrCloudCSVTransformerNil
	}

	// Ensure object exists in the bucket
	obj := s.client.Bucket(s.bucket).Object(s.path)
	if _, err := obj.Attrs(ctx); err != nil {
		log.Println(ErrMsgCloudCSVObjectNotExist, err)
		return nil, ErrCloudCSVObjectNotExist
	}

	// Create a reader for the object
	rc, err := obj.NewReader(ctx)
	if err != nil {
		return nil, fmt.Errorf("cloud csv: error creating reader for object %s in bucket %s: %w", s.path, s.bucket, err)
	}
	defer func() {
		if err := rc.Close(); err != nil {
			log.Println("cloud csv: error closing reader", err)
		}
	}()

	offsetInt64, err := utils.ParseInt64(offset)
	if err != nil {
		return nil, fmt.Errorf("cloud csv: invalid offset %s: %w", offset, err)
	}

	// Set start index & done flag.
	startIndex := offsetInt64
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

	resStream := make(chan *domain.BatchRecord)
	if done {
		resStream <- &domain.BatchRecord{
			Start: offset,
			End:   offset,
			Done:  done,
		}
	}

	go func() {
		defer close(resStream)

		err := ReadCSVStream(
			ctx,
			data,
			numBytesRead,
			offsetInt64,
			s.delimiter,
			s.hasHeader,
			s.transFunc,
			resStream,
		)
		if err != nil {
			log.Printf("error reading CSV data stream - path: %s, offset: %s, error: %s", s.path, offset, err.Error())
		}
	}()

	return resStream, nil
}

// Next reads the next batch of CSV rows from the cloud storage (S3/GCS/Azure).
// It reads from the cloud storage at the specified offset and returns a batch of CSVRow.
// Currently only supports GCP Storage. Ensure the environment variable is set for GCP credentials
func (s *cloudCSVSource) Next(
	ctx context.Context,
	offset string,
	size uint,
) (*domain.BatchProcess, error) {
	bp := &domain.BatchProcess{
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
		return bp, ErrCloudCSVClientNotInitialized
	}

	// If headers are enabled but transformer function is not set.
	if s.hasHeader && s.transFunc == nil {
		return bp, ErrCloudCSVTransformerNil
	}

	// Ensure object exists in the bucket
	obj := s.client.Bucket(s.bucket).Object(s.path)
	if _, err := obj.Attrs(ctx); err != nil {
		log.Println(ErrMsgCloudCSVObjectNotExist, err)
		return nil, ErrCloudCSVObjectNotExist
	}

	// Create a reader for the object
	rc, err := obj.NewReader(ctx)
	if err != nil {
		return bp, fmt.Errorf("cloud csv: error creating reader for object %s in bucket %s: %w", s.path, s.bucket, err)
	}
	defer func() {
		if err := rc.Close(); err != nil {
			log.Println("cloud csv: error closing reader", err)
		}
	}()

	// Set done flag.
	done := false

	// Create a read-at adapter for the GCP Storage reader.
	// This allows us to read data at specific offsets.
	readAtAdapter := &GCPStorageReadAtAdapter{
		Reader: rc,
	}

	offsetInt64, err := utils.ParseInt64(offset)
	if err != nil {
		return nil, fmt.Errorf("cloud csv: invalid offset %s: %w", offset, err)
	}

	// Read data bytes from the object at the specified offset
	data := make([]byte, size)
	numBytesRead, err := readAtAdapter.ReadAt(data, offsetInt64)
	if err != nil && err != io.EOF {
		return bp, fmt.Errorf("error reading object %s in bucket %s at offset %s: %w", s.path, s.bucket, offset, err)
	}

	// If read data is less than requested, cursor reached EOF, set Done
	if uint(numBytesRead) < size {
		done = true
	}

	records, nextOffset, err := ReadCSVBatch(
		ctx,
		data,
		numBytesRead,
		offsetInt64,
		s.delimiter,
		s.hasHeader,
		s.transFunc,
	)
	if err != nil {
		bp.Records = records
		bp.NextOffset = utils.Int64ToString(nextOffset)
		bp.Done = done

		return bp, err
	}

	bp.Records = records
	bp.NextOffset = utils.Int64ToString(nextOffset)
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
		return nil, ErrCloudCSVObjectPathRequired
	}

	if c.Bucket == "" {
		return nil, ErrCloudCSVBucketRequired
	}

	if c.Delimiter == 0 {
		c.Delimiter = ',' // default
	}

	if c.Provider == "" {
		c.Provider = "gcs" // default to GCS
	}

	if c.Provider != "gcs" {
		return nil, ErrCloudCSVUnsupportedProvider
	}

	// Ensure the environment variable is set for GCP credentials
	cPath := os.Getenv("GOOGLE_APPLICATION_CREDENTIALS")
	if cPath == "" {
		return nil, ErrCloudCSVMissingCreds
	}

	client, err := storage.NewClient(ctx)
	if err != nil {
		return nil, fmt.Errorf("cloud csv: failed to create storage client: %w", err)
	}

	src := &cloudCSVSource{
		provider:  c.Provider,
		bucket:    c.Bucket,
		path:      c.Path,
		delimiter: c.Delimiter,
		hasHeader: c.HasHeader,
	}

	obj := client.Bucket(c.Bucket).Object(c.Path)
	if attrs, err := obj.Attrs(ctx); err != nil {
		if err := client.Close(); err != nil {
			log.Println("cloud csv: error closing client:", err)
		}
		log.Println("cloud csv: object does not exist:", err)
		return nil, ErrCloudCSVObjectNotExist
	} else {
		if attrs.Size <= 0 {
			if err := client.Close(); err != nil {
				log.Println("cloud csv: error closing client:", err)
			}
			return nil, ErrCloudCSVSizeMustBePositive
		}
		src.size = attrs.Size
	}

	if c.HasHeader {
		rc, err := obj.NewReader(ctx)
		if err != nil {
			return nil, fmt.Errorf("cloud csv: error creating reader for object %s in bucket %s: %w", c.Path, c.Bucket, err)
		}
		defer func() {
			if err := rc.Close(); err != nil {
				log.Println("cloud csv: error closing reader:", err)
			}
		}()

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
