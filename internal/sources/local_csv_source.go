package sources

import (
	"context"
	"encoding/csv"
	"errors"
	"fmt"
	"io"
	"log"
	"os"

	"github.com/hankgalt/batch-orchestra/pkg/domain"
	"github.com/hankgalt/batch-orchestra/pkg/utils"
)

// Error constants and variables
const (
	ErrMsgLocalCSVTransformerNil     = "transformer function is not set for local CSV source with headers"
	ErrMsgLocalCSVSizeMustBePositive = "size must be greater than 0"
	ErrMsgLocalCSVPathRequired       = "local csv: path is required"
	ErrMsgLocalCSVFileNotFound       = "local csv: error opening file"
)

var (
	ErrLocalCSVTransformerNil     = errors.New(ErrMsgLocalCSVTransformerNil)
	ErrLocalCSVSizeMustBePositive = errors.New(ErrMsgLocalCSVSizeMustBePositive)
	ErrLocalCSVPathRequired       = errors.New(ErrMsgLocalCSVPathRequired)
	ErrLocalCSVFileNotFound       = errors.New(ErrMsgLocalCSVFileNotFound)
)

const (
	LocalCSVSource = "local-csv-source"
)

// Local CSV source.
type localCSVSource struct {
	path      string
	delimiter rune
	hasHeader bool
	transFunc domain.TransformerFunc // transformer function to apply to each row
}

// Name of the source.
func (s *localCSVSource) Name() string { return LocalCSVSource }

// Close closes the local CSV source.
func (s *localCSVSource) Close(ctx context.Context) error {
	// No resources to close for local CSV source
	return nil
}

// Next reads the next batch of CSV rows from the local file.
// It reads from the file at the specified offset and returns a batch of CSVRow.
func (s *localCSVSource) Next(
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
		return bp, ErrLocalCSVSizeMustBePositive
	}

	// Open the local CSV file.
	f, err := os.Open(s.path)
	if err != nil {
		log.Println(ErrMsgLocalCSVFileNotFound, err)
		return bp, ErrLocalCSVFileNotFound
	}
	defer f.Close()

	// If headers are enabled but transformer function is not set.
	if s.hasHeader && s.transFunc == nil {
		return bp, ErrLocalCSVTransformerNil
	}

	// Read data bytes from the file at the specified offset
	data := make([]byte, size)
	numBytesRead, err := f.ReadAt(data, int64(offset))
	if err != nil && err != io.EOF {
		return bp, fmt.Errorf("local csv: error reading file %s at offset %d: %w", s.path, offset, err)
	}

	// If read data is less than requested, it means we reached EOF, set Done
	done := false
	if uint(numBytesRead) < size {
		done = true
	}

	records, nextOffset, err := ReadCSVBatch(
		ctx,
		data,
		numBytesRead,
		int64(offset),
		s.delimiter,
		s.hasHeader,
		s.transFunc,
	)
	if err != nil {
		bp.Records = records
		bp.NextOffset = nextOffset
		bp.Done = done

		return bp, err
	}

	bp.Records = records
	bp.NextOffset = nextOffset
	bp.Done = done

	return bp, nil
}

func (s *localCSVSource) NextStream(
	ctx context.Context,
	offset uint64,
	size uint,
) (<-chan *domain.BatchRecord[domain.CSVRow], error) {
	// If size is 0 or negative, return an empty batch.
	if size <= 0 {
		return nil, ErrLocalCSVSizeMustBePositive
	}

	// Open the local CSV file.
	f, err := os.Open(s.path)
	if err != nil {
		log.Println(ErrMsgLocalCSVFileNotFound, err)
		return nil, ErrLocalCSVFileNotFound
	}
	defer f.Close()

	// If headers are enabled but transformer function is not set.
	if s.hasHeader && s.transFunc == nil {
		return nil, ErrLocalCSVTransformerNil
	}

	// Set start index to the specified offset.
	startIndex := int64(offset)

	// Read data bytes from the file at the specified offset
	data := make([]byte, size)
	numBytesRead, err := f.ReadAt(data, startIndex)
	if err != nil && err != io.EOF {
		return nil, fmt.Errorf("local csv: error reading file %s at offset %d: %w", s.path, startIndex, err)
	}

	// If read data is less than requested, it means we reached EOF, set Done
	done := false
	if uint(numBytesRead) < size {
		done = true
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

		err := ReadCSVStream(
			ctx,
			data,
			numBytesRead,
			int64(offset),
			s.delimiter,
			s.hasHeader,
			s.transFunc,
			resStream,
		)
		if err != nil {
			log.Printf("error reading CSV data stream - path: %s, offset: %d, error: %s", s.path, offset, err.Error())
		}
	}()

	return resStream, nil
}

// Local CSV source config.
type LocalCSVConfig struct {
	Path         string
	Delimiter    rune // e.g., ',', '|'
	HasHeader    bool
	MappingRules map[string]domain.Rule // mapping rules for the CSV rows
}

// Name of the source.
func (c LocalCSVConfig) Name() string { return LocalCSVSource }

// BuildSource builds a local CSV source from the config.
// It reads headers if HasHeader is true and caches it.
func (c LocalCSVConfig) BuildSource(
	ctx context.Context,
) (domain.Source[domain.CSVRow], error) {
	if c.Path == "" {
		return nil, ErrLocalCSVPathRequired
	}

	delim := c.Delimiter
	if delim == 0 {
		delim = ',' // default
	}

	src := &localCSVSource{
		path:      c.Path,
		delimiter: delim,
		hasHeader: c.HasHeader,
	}

	// If CSV file has headers, set cleaned headers.
	if c.HasHeader {
		f, err := os.Open(c.Path)
		if err != nil {
			log.Println(ErrMsgLocalCSVFileNotFound, err)
			return nil, ErrLocalCSVFileNotFound
		}
		defer f.Close()

		r := csv.NewReader(f)
		r.Comma = delim
		r.FieldsPerRecord = -1

		h, err := r.Read()
		if err != nil {
			if errors.Is(err, io.EOF) {
				// empty file: treat as no headers
				h = nil
			} else {
				return nil, fmt.Errorf("local csv: read header: %w", err)
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

	return src, nil
}
