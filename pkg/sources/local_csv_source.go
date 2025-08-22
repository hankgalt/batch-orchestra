package sources

import (
	"bytes"
	"context"
	"encoding/csv"
	"errors"
	"fmt"
	"io"
	"os"

	"github.com/hankgalt/batch-orchestra/pkg/domain"
	"github.com/hankgalt/batch-orchestra/pkg/utils"
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
func (s *localCSVSource) Next(ctx context.Context, offset uint64, size uint) (*domain.BatchProcess[domain.CSVRow], error) {
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

	// Open the local CSV file.
	f, err := os.Open(s.path)
	if err != nil {
		return bp, fmt.Errorf("open file: %w", err)
	}
	defer f.Close()

	// If headers are enabled but transformer function is not set.
	if s.hasHeader && s.transFunc == nil {
		return bp, fmt.Errorf("transformer function is not set for local CSV source with headers")
	}

	// Set start index to the specified offset.
	startIndex := int64(offset)

	// Read data bytes from the file at the specified offset
	data := make([]byte, size)
	numBytesRead, err := f.ReadAt(data, startIndex)
	if err != nil && err != io.EOF {
		return bp, fmt.Errorf("error reading file %s at offset %d: %w", s.path, startIndex, err)
	}

	// If read data is less than requested, it means we reached EOF, set Done
	done := false
	if uint(numBytesRead) < size {
		done = true
	}

	// get last line break index to drop partial record
	i := bytes.LastIndex(data, []byte{'\n'})
	if i < 0 {
		// If no newline found, we read till the end, set nextOffset to numBytesRead
		i = numBytesRead - 1
	}

	// create data buffer for bytes upto last line break
	buffer := bytes.NewBuffer(data[:i+1])

	// Create a CSV reader with the buffer
	csvReader := csv.NewReader(buffer)
	csvReader.Comma = s.delimiter
	csvReader.FieldsPerRecord = -1 // Allow variable number of fields per record

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
					Start: uint64(nextOffset),
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

		// Create a BatchRecord for the current record
		br := domain.BatchRecord[domain.CSVRow]{
			Start: uint64(startIndex),
			End:   uint64(csvReader.InputOffset()),
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
		br.Data = row

		// Update records slice & read count
		bp.Records = append(bp.Records, &br)
	}

	bp.NextOffset = offset + uint64(nextOffset)
	bp.Done = done

	return bp, nil
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
func (c LocalCSVConfig) BuildSource(ctx context.Context) (domain.Source[domain.CSVRow], error) {
	if c.Path == "" {
		return nil, errors.New("local csv: path is required")
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
			return nil, fmt.Errorf("local csv: open: %w", err)
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
