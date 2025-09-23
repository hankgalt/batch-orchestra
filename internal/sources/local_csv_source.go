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

const LocalCSVSource = "local-csv-source"

// Error constants and variables
const (
	ErrMsgLocalCSVTransformerNil     = "transformer function is not set for local CSV source with headers"
	ErrMsgLocalCSVSizeMustBePositive = "size must be greater than 0"
	ErrMsgLocalCSVPathRequired       = "local csv: path is required"
	ErrMsgLocalCSVFileNotFound       = "local csv: error opening file"
	ERR_INVALID_OFFSET               = "local csv: invalid offset, must be string"
	ERR_PARSING_OFFSET               = "local csv: error parsing offset, must be int64"
)

var (
	ErrLocalCSVTransformerNil     = errors.New(ErrMsgLocalCSVTransformerNil)
	ErrLocalCSVSizeMustBePositive = errors.New(ErrMsgLocalCSVSizeMustBePositive)
	ErrLocalCSVPathRequired       = errors.New(ErrMsgLocalCSVPathRequired)
	ErrLocalCSVFileNotFound       = errors.New(ErrMsgLocalCSVFileNotFound)
	ErrInvalidOffset              = errors.New(ERR_INVALID_OFFSET)
	ErrParsingOffset              = errors.New(ERR_PARSING_OFFSET)
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
	offset any,
	size uint,
) (*domain.BatchProcess, error) {
	offsetStr, ok := offset.(string)
	if !ok {
		return nil, ErrInvalidOffset
	}

	bp := &domain.BatchProcess{
		Records:     nil,
		NextOffset:  offsetStr,
		StartOffset: offsetStr,
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

	offsetInt64, err := utils.ParseInt64(offsetStr)
	if err != nil {
		return bp, ErrInvalidOffset
	}

	// Read data bytes from the file at the specified offset
	data := make([]byte, size)
	numBytesRead, err := f.ReadAt(data, offsetInt64)
	if err != nil && err != io.EOF {
		return bp, fmt.Errorf("llocalCSVSource:Next - error reading file %s at offset %s: %w", s.path, offsetStr, err)
	}

	// If read data is less than requested, it means we reached EOF, set Done
	done := false
	if numBytesRead < int(size) {
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

func (s *localCSVSource) NextStream(
	ctx context.Context,
	offset any,
	size uint,
) (<-chan *domain.BatchRecord, error) {
	offsetStr, ok := offset.(string)
	if !ok {
		return nil, fmt.Errorf("localCSVSource:NextStream - invalid offset type %T, must be string", offset)
	}

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

	offsetInt64, err := utils.ParseInt64(offsetStr)
	if err != nil {
		return nil, ErrParsingOffset
	}

	// Set start index to the specified offset.
	startIndex := offsetInt64

	// Read data bytes from the file at the specified offset
	data := make([]byte, size)
	numBytesRead, err := f.ReadAt(data, startIndex)
	if err != nil && err != io.EOF {
		return nil, fmt.Errorf("localCSVSource:NextStream - error reading file %s at offset %d: %w", s.path, startIndex, err)
	}

	// If read data is less than requested, it means we reached EOF, set Done
	done := false
	if uint(numBytesRead) < size {
		done = true
	}

	resStream := make(chan *domain.BatchRecord)

	if done {
		resStream <- &domain.BatchRecord{
			Start: string(offsetStr),
			End:   string(offsetStr),
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
			log.Printf("localCSVSource:NextStream - error reading CSV data stream - path: %s, offset: %s, error: %s", s.path, offsetStr, err.Error())
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
	size         int64
}

// Name of the source.
func (c *LocalCSVConfig) Name() string { return LocalCSVSource }

// Size returns the size of the local CSV file.
func (c *LocalCSVConfig) Size() int64 {
	return c.size
}

// BuildSource builds a local CSV source from the config.
// It reads headers if HasHeader is true and caches it.
func (c *LocalCSVConfig) BuildSource(
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

	if fileInfo, err := os.Stat(c.Path); err != nil {
		fmt.Printf("Error getting file info: %v\n", err)
	} else {
		c.size = fileInfo.Size()
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
