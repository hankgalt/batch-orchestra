package utils

import (
	"fmt"
	"os"

	"github.com/hankgalt/batch-orchestra/pkg/domain"
)

const DEFAULT_DATA_DIR = "data"
const DEFAULT_DATA_PATH string = "scheduler"
const DEFAULT_FILE_NAME string = "Agents-sm.csv"

// BuildFileName constructs the file name using the FILE_NAME env variable or defaults to DEFAULT_FILE_NAME.
func BuildFileName() string {
	fileName := os.Getenv("FILE_NAME")
	if fileName == "" {
		fileName = DEFAULT_FILE_NAME
		fmt.Printf("FILE_NAME environment variable is not set, using default: %s\n", DEFAULT_FILE_NAME)
	}

	return fileName
}

// BuildFilePath constructs the file path using the DATA_DIR env variable or defaults to "<DEFAULT_DATA_DIR>/<DEFAULT_DATA_PATH>".
func BuildFilePath() (string, error) {
	dataDir := os.Getenv("DATA_DIR")
	if dataDir == "" {
		dataDir = DEFAULT_DATA_DIR
		fmt.Printf("DATA_DIR environment variable is not set, using default: %s\n", DEFAULT_DATA_DIR)
	}

	filePath := fmt.Sprintf("%s/%s", dataDir, DEFAULT_DATA_PATH)
	if _, err := os.Stat(filePath); os.IsNotExist(err) {
		fmt.Printf("Data path does not exist: %s\n", filePath)
		return "", fmt.Errorf("data path does not exist: %s", filePath)
	}

	return filePath, nil
}

func BuildCloudFileConfig() (domain.CloudFileConfig, error) {
	filePath, fileName := DEFAULT_DATA_PATH, BuildFileName()
	bucket := os.Getenv("BUCKET")
	if bucket == "" {
		return domain.CloudFileConfig{}, fmt.Errorf("BUCKET environment variable is not set")
	}

	return domain.CloudFileConfig{
		Name:   fileName,
		Path:   filePath,
		Bucket: bucket,
	}, nil
}
