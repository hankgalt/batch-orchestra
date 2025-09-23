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

const ERR_LOCAL_JSON_DIR_OPEN = "local json: open directory"
const ERR_LOCAL_JSON_FILE_OPEN = "local json: open file"
const ERR_LOCAL_JSON_FILE_PATH = "local json: path is required"
const ERR_LOCAL_JSON_SIZE_INVALID = "local json: size must be greater than 0"

var ErrLocalJSONDirOpen = errors.New(ERR_LOCAL_JSON_DIR_OPEN)
var ErrLocalJSONFileOpen = errors.New(ERR_LOCAL_JSON_FILE_OPEN)
var ErrLocalJSONPathRequired = errors.New(ERR_LOCAL_JSON_FILE_PATH)
var ErrLocalJSONSizeInvalid = errors.New(ERR_LOCAL_JSON_SIZE_INVALID)

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
	co, ok := offset.(domain.CustomOffset[domain.HasId])
	if !ok {
		l.Error("local json: invalid offset, must be CustomOffset[HasId]", "offset-type", offset)
		return nil, ErrInvalidOffset
	}

	// If size is 0 or negative, return error.
	if size <= 0 {
		return nil, ErrLocalJSONSizeInvalid
	}

	bp := &domain.BatchProcess{
		Records:     nil,
		NextOffset:  offset,
		StartOffset: offset,
		Done:        false,
	}

	// get file offset
	idArr := strings.Split(co.Val.GetId(), Divider)
	fileOffset := idArr[0]

	filePath := ""

	if len(idArr) == 2 {
		// build file path from given offset
		filePath = filepath.Join(s.path, s.fileKey+"-"+fileOffset+".json")
	} else {
		if len(idArr) != 1 || co.Val.GetId() != "0" {
			return nil, ErrInvalidOffset
		}

		// If starting from the beginning (id = "0"), start with smallest file offset in the directory.
		l.Debug("starting from beginning")

		// read directory and find smallest file offset
		entries, err := os.ReadDir(s.path)
		if err != nil {
			l.Error("error reading local json directory", "path", s.path, "error", err.Error())
			return nil, ErrLocalJSONDirOpen
		}

		currOffsetFileName := ""
		currFileOffset := int64(0)
		fileMap := map[int64]string{}
		errFileMap := map[string]string{}

		// use a max-heap to keep track of the 3 smallest file offsets
		maxHeap := &hp.MaxHeap[int64]{}
		heap.Init(maxHeap)

		// iterate over directory entries
		for _, entry := range entries {
			// ignore directories, non-json files and files not matching the file key
			if entry.IsDir() || !strings.HasPrefix(entry.Name(), s.fileKey) || filepath.Ext(entry.Name()) != ".json" {
				continue
			}

			toks := strings.Split(entry.Name(), "-")
			if len(toks) > 1 {
				// ignore error & cleanup files for determining current offset file
				if toks[len(toks)-2] == "error" || toks[len(toks)-2] == "cleanup" {
					if toks[len(toks)-2] == "error" {
						errFileMap[fileOffset] = entry.Name()
					}
					continue
				}

				// extract file offset
				fileOffset := strings.Split(toks[len(toks)-1], ".")[0]
				fileOffsetInt64, err := utils.ParseInt64(fileOffset)
				if err != nil {
					l.Error("error parsing file offset", "file", entry.Name(), "error", err.Error())
					continue
				}

				// track current file offset, which is the largest offset seen so far
				if fileOffsetInt64 >= currFileOffset {
					currFileOffset = fileOffsetInt64
					currOffsetFileName = entry.Name()
				}

				// push to max-heap & file map
				heap.Push(maxHeap, fileOffsetInt64)
				fileMap[fileOffsetInt64] = entry.Name()

				// if heap size exceeds 3, pop the largest offset
				if maxHeap.Len() > 3 {
					popped := heap.Pop(maxHeap)
					poppedInt64 := popped.(int64)
					delete(fileMap, poppedInt64)
				}
			}
		}

		// get smallest 3 file offsets from max-heap
		out := make([]int64, maxHeap.Len())
		for i := range out {
			outInt64 := heap.Pop(maxHeap).(int64)
			out[i] = outInt64
		}
		slices.Sort(out)
		l.Debug("json file directory", "path", s.path)
		l.Debug("files", "smallest-file", fileMap[out[0]], "current-file", currOffsetFileName)

		// check if there are any error files with offset >= current file offset, is current error
		errFilesToProcess := []string{}
		for k, v := range errFileMap {
			fileOffsetInt64, err := utils.ParseInt64(k)
			if err != nil {
				l.Error("error parsing error file offset", "file", v, "error", err.Error())
				continue
			}
			if fileOffsetInt64 < currFileOffset {
				continue
			}
			errFilesToProcess = append(errFilesToProcess, v)
		}
		if len(errFilesToProcess) > 0 {
			l.Debug("error files", "err-files", errFilesToProcess)
		}

		// build file path for first batch
		filePath = filepath.Join(s.path, fileMap[out[0]])
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

	// if no batches in file, return error
	if len(data.Offsets) <= 1 {
		l.Error("no batches in json file", "file", filePath)
		return nil, errors.New("no batches in json file")
	}

	// if starting from beginning, process first batch
	if len(idArr) == 1 && co.Val.GetId() == "0" {
		startOffsetIdx := 0
		nextOffsetIdx := 1

		// build processed batch id and get batch
		processedbatchId := fmt.Sprintf("batch-%s-%s", data.Offsets[startOffsetIdx], data.Offsets[nextOffsetIdx])
		pb, ok := data.Batches[processedbatchId]
		if !ok {
			l.Error("batch not found in json file", "batch-id", processedbatchId, "file", filePath)
			return nil, fmt.Errorf("batch not found in json file: %s", processedbatchId)
		}
		l.Debug("processing json file", "file", filePath, "start-offset-idx", startOffsetIdx, "next-offset-idx", nextOffsetIdx)

		// build batch records
		bp.Records = []*domain.BatchRecord{
			{
				Data:  pb,
				Start: startOffsetIdx,
				End:   nextOffsetIdx,
			},
		}

		// build next offset & set in batch
		nextOffsetId := fmt.Sprintf("%s%s%d", fileOffset, Divider, nextOffsetIdx)
		bp.NextOffset = domain.CustomOffset[domain.HasId]{
			Val: domain.JSONOffset{
				WithId: domain.WithId{
					Id: nextOffsetId,
				},
				Value: nextOffsetId,
			},
		}

		// return first batch
		return bp, nil
	}

	// parse start offset index
	val1, err := strconv.ParseInt(idArr[1], 10, 64)
	if err != nil {
		return nil, err
	}
	startOffsetIdx := int(val1)

	// if current file has more batches to process
	if startOffsetIdx+1 < len(data.Offsets) {
		// update next offset index
		nextOffsetIdx := startOffsetIdx + 1

		// build processed batch id & get batch
		processedbatchId := fmt.Sprintf("batch-%s-%s", data.Offsets[startOffsetIdx], data.Offsets[nextOffsetIdx])
		pb, ok := data.Batches[processedbatchId]
		if !ok {
			l.Error("batch not found in json file", "batch-id", processedbatchId, "file", filePath)
			return nil, fmt.Errorf("batch not found in json file: %s", processedbatchId)
		}
		l.Debug("processing json file", "file", filePath, "start-offset-idx", startOffsetIdx, "next-offset-idx", nextOffsetIdx)

		// build batch records
		bp.Records = []*domain.BatchRecord{
			{
				Data:  pb,
				Start: startOffsetIdx,
				End:   nextOffsetIdx,
			},
		}

		// build next offset & set in batch
		nextOffsetId := fmt.Sprintf("%s%s%d", fileOffset, Divider, nextOffsetIdx)
		bp.NextOffset = domain.CustomOffset[domain.HasId]{
			Val: domain.JSONOffset{
				WithId: domain.WithId{
					Id: nextOffsetId,
				},
				Value: nextOffsetId,
			},
		}

		// return batch
		return bp, nil
	}

	// all batches from current file are processed, check if there is a next file
	// set next file offset
	nextFileOffset := data.Offsets[startOffsetIdx]
	nextFileOffsetStr, ok := nextFileOffset.(string)
	if !ok {
		l.Error("invalid next file offset type, must be string", "type", nextFileOffset)
		return nil, ErrInvalidOffset
	}

	// get next file path & check if file exists
	nextFilePath := filepath.Join(s.path, s.fileKey+"-"+nextFileOffsetStr+".json")
	if _, err := os.Stat(nextFilePath); os.IsNotExist(err) {
		// if there is no next file & batch wasn't marked done, return error
		if !data.Done {
			return nil, fmt.Errorf("next json file not found: %s", nextFilePath)
		}

		// return last batch
		bp.Done = true
		return bp, nil
	} else if err != nil {
		l.Error("error stating next json file", "path", nextFilePath, "error", err.Error())
		return nil, err
	}

	// reset start & next offset idx
	startOffsetIdx = 0
	nextOffsetIdx := 1

	// update file offset
	fileOffset = nextFileOffsetStr

	// update start offset
	startOffsetId := fmt.Sprintf("%s%s%d", fileOffset, Divider, startOffsetIdx)
	startOffset := domain.CustomOffset[domain.HasId]{
		Val: domain.JSONOffset{
			WithId: domain.WithId{
				Id: startOffsetId,
			},
			Value: startOffsetId,
		},
	}
	bp.StartOffset = startOffset

	// open next file
	filePath = nextFilePath
	file, err = os.Open(filePath)
	if err != nil {
		l.Error("error opening local json file", "path", filePath, "error", err.Error())
		return nil, ErrLocalJSONFileOpen
	}
	defer file.Close() // Ensure the file is closed

	// read file
	decoder = json.NewDecoder(file)
	err = decoder.Decode(&data)
	if err != nil {
		l.Error("error decoding local json file", "path", filePath, "error", err.Error())
		return nil, err
	}

	if len(data.Offsets) <= 1 {
		l.Error("no batches in json file", "file", filePath)
		return nil, errors.New("no batches in json file")
	}

	// build processed batch id & get batch
	processedbatchId := fmt.Sprintf("batch-%s-%s", data.Offsets[startOffsetIdx], data.Offsets[nextOffsetIdx])
	pb, ok := data.Batches[processedbatchId]
	if !ok {
		l.Error("batch not found in json file", "batch-id", processedbatchId, "file", filePath)
		return nil, fmt.Errorf("batch not found in json file: %s", processedbatchId)
	}

	l.Debug("processing json file", "file", filePath, "start-offset-idx", startOffsetIdx, "next-offset-idx", nextOffsetIdx)

	nextOffsetId := fmt.Sprintf("%s%s%d", fileOffset, Divider, nextOffsetIdx)
	nextOffset := domain.CustomOffset[domain.HasId]{
		Val: domain.JSONOffset{
			WithId: domain.WithId{
				Id: nextOffsetId,
			},
			Value: nextOffsetId,
		},
	}

	bp.Records = []*domain.BatchRecord{
		{
			Data:  pb,
			Start: startOffsetIdx,
			End:   nextOffsetIdx,
		},
	}
	bp.NextOffset = nextOffset

	return bp, nil
}

// Local JSON source config.
type LocalJSONConfig struct {
	Path    string
	FileKey string
}

// Name of the source.
func (c *LocalJSONConfig) Name() string { return LocalJSONSource }

// BuildSource builds a local JSON source from the config.
func (c *LocalJSONConfig) BuildSource(ctx context.Context) (domain.Source[any], error) {
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
