package sources

import (
	"container/heap"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"

	"github.com/comfforts/logger"
	"github.com/hankgalt/batch-orchestra/pkg/domain"
	"github.com/hankgalt/batch-orchestra/pkg/utils"

	hp "github.com/hankgalt/batch-orchestra/pkg/utils/heap"
)

const LocalJSONSource = "local-json-source"
const Divider = "##--##"

const ERR_LOCAL_JSON_INVALID_OFFSET = "local json: invalid offset"
const ERR_LOCAL_JSON_INVALID_OFFSET_STR = "local json: invalid offset, must be string"
const ERR_LOCAL_JSON_DIR_OPEN = "local json: open directory"
const ERR_LOCAL_JSON_FILE_OPEN = "local json: open file"
const ERR_LOCAL_JSON_FILE_PATH = "local json: path is required"
const ERR_LOCAL_JSON_SIZE_INVALID = "local json: size must be greater than 0"

var ErrLocalJSONDirOpen = errors.New(ERR_LOCAL_JSON_DIR_OPEN)
var ErrLocalJSONFileOpen = errors.New(ERR_LOCAL_JSON_FILE_OPEN)
var ErrLocalJSONPathRequired = errors.New(ERR_LOCAL_JSON_FILE_PATH)
var ErrLocalJSONSizeInvalid = errors.New(ERR_LOCAL_JSON_SIZE_INVALID)
var ErrLocalJSONInvalidOffset = errors.New(ERR_LOCAL_JSON_INVALID_OFFSET)
var ErrLocalJSONInvalidOffsetStr = errors.New(ERR_LOCAL_JSON_INVALID_OFFSET_STR)

type localJSONSource struct {
	path    string
	fileKey string
}

// Name of the source.
func (s *localJSONSource) Name() string { return LocalJSONSource }

// Close closes the local JSON source.
func (s *localJSONSource) Close(ctx context.Context) error {
	// No resources to close for local JSON source
	return nil
}

// Next reads the next batch of JSON objects from the local file.
// It reads from the file at the specified offset and returns a batch of JSON objects.
func (s *localJSONSource) Next(ctx context.Context, offset any, size uint) (*domain.BatchProcess, error) {
	l, err := logger.LoggerFromContext(ctx)
	if err != nil {
		l = logger.GetSlogLogger()
	}

	// Validate offset type
	if !domain.IsIdOffset(offset) {
		l.Error("local json: invalid offset, must be map[string]any{\"id\": <id_string>}", "offset-type", offset)
		return nil, ErrLocalJSONInvalidOffset
	}

	offsetId, err := domain.GetOffsetId(offset)
	if err != nil {
		l.Error("local json: error getting offset id", "error", err.Error())
		return nil, ErrLocalJSONInvalidOffset
	}

	// If size is 0 or negative, return error.
	if size <= 0 {
		return nil, ErrLocalJSONSizeInvalid
	}

	// Split id & initialize file offset & file path vars
	idArr := strings.Split(offsetId, Divider)
	fileOffset := idArr[0]
	filePath := ""

	// build file path
	if len(idArr) == 2 {
		// If in-process Id, build file path from given offset
		filePath = filepath.Join(s.path, s.fileKey+"-"+fileOffset+".json")
	} else {
		if len(idArr) != 1 || offsetId != "0" {
			return nil, ErrLocalJSONInvalidOffset
		}
		// build file path for first batch
		fp, err := getFirstBatchFilePath(ctx, s.path, s.fileKey)
		if err != nil {
			l.Error("error getting first batch file path", "error", err.Error())
			return nil, err
		}
		filePath = fp
	}

	// open file
	file, err := os.Open(filePath)
	if err != nil {
		l.Error("error opening local json file", "path", filePath, "error", err.Error())
		return nil, err
	}
	defer file.Close() // Ensure the file is closed

	// read file
	var data domain.BatchProcessingResult
	decoder := json.NewDecoder(file)
	err = decoder.Decode(&data)
	if err != nil {
		l.Error("error decoding local json file", "path", filePath, "error", err.Error())
		return nil, err
	}

	// if there are no batches in file, return error
	if len(data.Offsets) <= 1 {
		l.Error("no batches in json file", "file", filePath)
		return nil, errors.New("no batches in json file")
	}

	l.Debug(
		"current json source",
		"file", s.fileKey+"-"+fileOffset+".json",
		"num-records-processed", data.NumRecords,
		"num-batches-processed", data.NumBatches,
		"file-offset", fileOffset,
		"num-batches", len(data.Batches),
	)

	// if starting from beginning, build & return first batch
	if len(idArr) == 1 && offsetId == "0" {
		return buildFirstBatch(ctx, filePath, fileOffset, &data)
	}

	// parse start offset index
	val1, err := strconv.ParseInt(idArr[1], 10, 64)
	if err != nil {
		return nil, err
	}
	startOffsetIdx := int(val1)

	// if current file has more batches to process, build & return next batch
	if startOffsetIdx+1 < len(data.Offsets) {
		return buildNextBatch(ctx, filePath, fileOffset, startOffsetIdx, &data)
	}

	// all batches from current file are processed, get next file offset
	nextFileOffset := data.Offsets[startOffsetIdx]
	nextFileOffsetStr, ok := nextFileOffset.(string)
	if !ok {
		l.Error("invalid next file offset type, must be string", "type", nextFileOffset)
		return nil, ErrLocalJSONInvalidOffset
	}

	// build & return next file batch
	return buildNextFileBatch(ctx, s.path, s.fileKey, nextFileOffsetStr)
}

// Local JSON source config.
type LocalJSONSourceConfig struct {
	Path    string
	FileKey string
}

// Name of the source.
func (c *LocalJSONSourceConfig) Name() string { return LocalJSONSource }

// BuildSource builds a local JSON source from the config.
func (c *LocalJSONSourceConfig) BuildSource(ctx context.Context) (domain.Source[any], error) {
	l, err := logger.LoggerFromContext(ctx)
	if err != nil {
		l = logger.GetSlogLogger()
	}

	if c.Path == "" {
		l.Error("local json source: path is required")
		return nil, ErrLocalJSONPathRequired
	}

	return &localJSONSource{
		path:    c.Path,
		fileKey: c.FileKey,
	}, nil
}

func getFirstBatchFilePath(ctx context.Context, dirPath, fileKey string) (string, error) {
	l, err := logger.LoggerFromContext(ctx)
	if err != nil {
		l = logger.GetSlogLogger()
	}

	// read directory and find smallest file offset
	entries, err := os.ReadDir(dirPath)
	if err != nil {
		l.Error("error reading local json directory", "path", dirPath, "error", err.Error())
		return "", ErrLocalJSONDirOpen
	}

	// Initialize variables for tracking file offsets of last file & current 3 files
	lastOffsetFileName := ""
	lastFileOffset := int64(0)
	fileMap := map[int64]string{}
	errFileMap := map[string]string{}

	// use a max-heap to keep track of the 3 current file offsets
	maxHeap := &hp.MaxHeap[int64]{}
	heap.Init(maxHeap)

	// iterate over directory entries
	for _, entry := range entries {
		// ignore directories, non-json files and files not matching the file key
		if entry.IsDir() || !strings.HasPrefix(entry.Name(), fileKey) || filepath.Ext(entry.Name()) != ".json" {
			continue
		}

		// extract file offset from file name
		toks := strings.Split(entry.Name(), "-")
		if len(toks) > 1 {
			// extract file offset
			fileOffset := strings.Split(toks[len(toks)-1], ".")[0]
			fileOffsetInt64, err := utils.ParseInt64(fileOffset)
			if err != nil {
				l.Error("error parsing file offset", "file", entry.Name(), "error", err.Error())
				continue
			}

			// ignore error & cleanup files for determining current offset file
			// keep track of error files for later processing
			if toks[len(toks)-2] == "error" || toks[len(toks)-2] == "cleanup" {
				if toks[len(toks)-2] == "error" {
					errFileMap[fileOffset] = entry.Name()
				}
				continue
			}

			// track last file offset, which is the largest offset seen so far
			if fileOffsetInt64 >= lastFileOffset {
				lastFileOffset = fileOffsetInt64
				lastOffsetFileName = entry.Name()
			}

			// push to max-heap & file map
			heap.Push(maxHeap, fileOffsetInt64)
			fileMap[fileOffsetInt64] = entry.Name()

			// if heap size exceeds 3, remove the largest offset
			if maxHeap.Len() > 3 {
				popped := heap.Pop(maxHeap)
				poppedInt64 := popped.(int64)
				delete(fileMap, poppedInt64)
			}
		}
	}

	// get current 3 file offsets from max-heap
	out := make([]int64, maxHeap.Len())
	for i := range out {
		outInt64 := heap.Pop(maxHeap).(int64)
		out[i] = outInt64
	}
	slices.Sort(out)
	l.Debug("json file directory", "path", dirPath)

	// open next file
	filePath := filepath.Join(dirPath, lastOffsetFileName)
	file, err := os.Open(filePath)
	if err != nil {
		l.Error("error opening local json file", "path", filePath, "error", err.Error())
	}
	defer file.Close() // Ensure the file is closed

	// read file
	var data domain.BatchProcessingResult
	decoder := json.NewDecoder(file)
	if err = decoder.Decode(&data); err != nil {
		l.Error("error decoding last json file", "path", filePath, "error", err.Error())
	} else {
		l.Debug("last json file", "path", filePath, "num-records", data.NumRecords, "num-batches", data.NumBatches)
	}

	// check if there are any error files with offset >= last file offset, if there is, it is current error
	errFilesToProcess := []string{}
	for k, v := range errFileMap {
		fileOffsetInt64, err := utils.ParseInt64(k)
		if err != nil {
			l.Error("error parsing error file offset", "file", v, "error", err.Error())
			continue
		}
		if fileOffsetInt64 < lastFileOffset {
			l.Info("error resolved", "file", v)
			continue
		}
		errFilesToProcess = append(errFilesToProcess, v)
	}
	if len(errFilesToProcess) > 0 {
		l.Debug("error files", "err-files", errFilesToProcess)
	}

	// build file path for first batch
	return filepath.Join(dirPath, fileMap[out[0]]), nil
}

func buildFirstBatch(
	ctx context.Context,
	filePath string,
	fileOffset string,
	data *domain.BatchProcessingResult,
) (*domain.BatchProcess, error) {
	startOffsetIdx := 0
	nextOffsetIdx := 1

	return buildBatch(ctx, fileOffset, startOffsetIdx, nextOffsetIdx, filePath, data)
}

func buildNextBatch(
	ctx context.Context,
	filePath string,
	fileOffset string,
	startOffsetIdx int,
	data *domain.BatchProcessingResult,
) (*domain.BatchProcess, error) {
	// update next offset index
	nextOffsetIdx := startOffsetIdx + 1

	return buildBatch(ctx, fileOffset, startOffsetIdx, nextOffsetIdx, filePath, data)
}

func buildNextFileBatch(
	ctx context.Context,
	dirPath string,
	fileKey string,
	nextFileOffsetStr string,
) (*domain.BatchProcess, error) {
	l, err := logger.LoggerFromContext(ctx)
	if err != nil {
		l = logger.GetSlogLogger()
	}

	// get next file path & check if file exists
	nextFilePath := filepath.Join(dirPath, fileKey+"-"+nextFileOffsetStr+".json")
	if _, err := os.Stat(nextFilePath); err != nil {
		l.Error("error next json file not found", "path", nextFilePath, "error", err.Error())
		return nil, err
	}

	// reset start & next offset idx
	startOffsetIdx := 0
	nextOffsetIdx := 1

	// update file offset
	fileOffset := nextFileOffsetStr

	// open next file
	file, err := os.Open(nextFilePath)
	if err != nil {
		l.Error("error opening local json file", "path", nextFilePath, "error", err.Error())
		return nil, ErrLocalJSONFileOpen
	}
	defer file.Close() // Ensure the file is closed

	// read file
	var data domain.BatchProcessingResult
	decoder := json.NewDecoder(file)
	err = decoder.Decode(&data)
	if err != nil {
		l.Error("error decoding local json file", "path", nextFilePath, "error", err.Error())
		return nil, err
	}

	if len(data.Offsets) <= 1 {
		l.Error("no batches in json file", "file", nextFilePath)
		return nil, errors.New("no batches in json file")
	}

	return buildBatch(ctx, fileOffset, startOffsetIdx, nextOffsetIdx, nextFilePath, &data)
}

func buildBatch(
	ctx context.Context,
	fileOffset string,
	startOffsetIdx, nextOffsetIdx int,
	filePath string,
	data *domain.BatchProcessingResult,
) (*domain.BatchProcess, error) {
	l, err := logger.LoggerFromContext(ctx)
	if err != nil {
		l = logger.GetSlogLogger()
	}

	if _, ok := data.Offsets[startOffsetIdx].(string); !ok {
		l.Error("invalid start offset type, must be string", "type", data.Offsets[startOffsetIdx])
		return nil, ErrLocalJSONInvalidOffsetStr
	}
	if _, ok := data.Offsets[nextOffsetIdx].(string); !ok {
		l.Error("invalid next offset type, must be string", "type", data.Offsets[nextOffsetIdx])
		return nil, ErrLocalJSONInvalidOffsetStr
	}

	startOffset := map[string]any{
		"id": fmt.Sprintf("%s%s%d", fileOffset, Divider, startOffsetIdx),
	}

	// build processed batch id & get batch
	processedbatchId := fmt.Sprintf("batch-%s-%s", data.Offsets[startOffsetIdx], data.Offsets[nextOffsetIdx])
	pb, ok := data.Batches[processedbatchId]
	if !ok {
		l.Error("batch not found in json file", "batch-id", processedbatchId, "file", filePath)
		return nil, fmt.Errorf("batch not found in json file: %s", processedbatchId)
	}
	l.Debug("next json file batch", "file", filePath, "start-offset-idx", startOffsetIdx, "next-offset-idx", nextOffsetIdx)

	// build next offset & set in batch
	nextOffset := map[string]any{
		"id": fmt.Sprintf("%s%s%d", fileOffset, Divider, nextOffsetIdx),
	}

	bp := &domain.BatchProcess{
		StartOffset: startOffset,
		NextOffset:  nextOffset,
		Records: []*domain.BatchRecord{
			{
				Data:  pb,
				Start: startOffset,
				End:   nextOffset,
			},
		},
		Done: false,
	}

	if nextOffsetIdx+1 >= len(data.Offsets) && data.Done {
		bp.Done = true
	}

	return bp, nil
}
