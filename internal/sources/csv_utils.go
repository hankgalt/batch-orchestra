package sources

import (
	"bytes"
	"context"
	"encoding/csv"
	"errors"
	"fmt"
	"io"

	"github.com/hankgalt/batch-orchestra/pkg/domain"
	"github.com/hankgalt/batch-orchestra/pkg/utils"
)

func ReadCSVBatch(
	ctx context.Context,
	data []byte,
	numBytesRead int,
	offset int64,
	delimiter rune,
	hasHeader bool,
	transFunc domain.TransformerFunc,
) ([]*domain.BatchRecord, uint64, error) {
	if len(data) == 0 || numBytesRead == 0 {
		return nil, uint64(offset), nil
	}

	// Set start index to the specified offset.
	startIndex := offset

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
	csvReader.Comma = delimiter
	csvReader.FieldsPerRecord = -1 // Allow variable number of fields per record

	// Initialize start & next offsets
	nextOffset := csvReader.InputOffset()

	// Initialize read count and records slice
	readCount := 0

	records := []*domain.BatchRecord{}

	// Read records from the CSV reader
	for {
		// allow cancellation
		select {
		case <-ctx.Done():
			return records, uint64(offset) + uint64(nextOffset), ctx.Err()
		default:
		}

		rec, err := csvReader.Read()
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			// Attempt record cleanup if error occurs
			cleanedStr := utils.CleanRecord(string(data[nextOffset:csvReader.InputOffset()]))
			if record, err := utils.ReadSingleRecord(cleanedStr); err != nil {
				records = append(records, &domain.BatchRecord{
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
			} else {
				rec = record
			}
		}

		// startIndex & nextOffset will be 0 for the first read, skip headers
		if startIndex <= 0 && nextOffset <= 0 && hasHeader {
			// Update startIndex to the next record's offset
			startIndex = nextOffset

			// update nextOffset to the next record's offset
			nextOffset = csvReader.InputOffset()

			// update row read count
			readCount++

			continue
		}

		// Clean record & create a CSVRow from the transformed record
		rec = utils.CleanAlphaNumericsArr(rec, []rune{'.', '-', '_', '#', '&', '@'})
		res := transFunc(rec)
		row := domain.CSVRow{}
		for k, v := range res {
			st, ok := v.(string)
			if !ok {
				row[k] = ""
			}
			row[k] = st
		}

		// Create a BatchRecord for the current csv record
		br := domain.BatchRecord{
			Start: uint64(startIndex),
			End:   uint64(csvReader.InputOffset()),
			Data:  row,
		}

		// Update records slice & read count
		records = append(records, &br)

		// Update startIndex to the next record's offset
		startIndex = nextOffset

		// update nextOffset to the next record's offset
		nextOffset = csvReader.InputOffset()

		// update row read count
		readCount++

	}

	return records, uint64(offset) + uint64(nextOffset), nil
}

func ReadCSVStream(
	ctx context.Context,
	data []byte,
	numBytesRead int,
	offset int64,
	delimiter rune,
	hasHeader bool,
	transFunc domain.TransformerFunc,
	resStream chan<- *domain.BatchRecord,
) error {
	if len(data) == 0 || numBytesRead == 0 {
		return nil
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
	csvReader.Comma = delimiter
	csvReader.FieldsPerRecord = -1 // Allow variable number of fields per record

	// Initialize start & next offsets
	nextOffset := csvReader.InputOffset()

	// Initialize read count and records slice
	readCount := 0

	// Set start index to the specified offset.
	startIndex := offset

	// Read records from the CSV reader
	for {
		// allow cancellation
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		rec, err := csvReader.Read()
		if err != nil {
			if errors.Is(err, io.EOF) {
				return nil
			}
			// Attempt record cleanup if error occurs
			cleanedStr := utils.CleanRecord(string(data[nextOffset:csvReader.InputOffset()]))
			record, err := utils.ReadSingleRecord(cleanedStr)
			if err != nil {
				resStream <- &domain.BatchRecord{
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
		if startIndex <= 0 && nextOffset <= 0 && hasHeader {
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
		res := transFunc(rec)

		row := domain.CSVRow{}
		for k, v := range res {
			st, ok := v.(string)
			if !ok {
				row[k] = ""
			}
			row[k] = st
		}

		resStream <- &domain.BatchRecord{
			Start: uint64(startIndex),
			End:   uint64(csvReader.InputOffset()),
			Data:  row,
		}
	}
}
