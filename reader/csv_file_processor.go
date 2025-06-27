package reader

import (
	"context"
	"github.com/gocarina/gocsv"
	"os"
	"report-update-scripts/logger"
)

type CsvFileProcessor[T any] struct {
}

func (processor CsvFileProcessor[T]) Read(filePath string) ([]*T, error) {
	// 打开CSV文件
	file, err := os.Open(filePath)
	if err != nil {
		logger.Error(context.Background(), "os open filePath : %s, error : %+v", filePath, err)
		return nil, err
	}
	defer file.Close()

	// 读取CSV到结构体
	var csvDataArray []*T
	if err := gocsv.UnmarshalFile(file, &csvDataArray); err != nil {
		logger.Error(context.Background(), "UnmarshalFile filePath : %s, error : %+v", filePath, err)
		return csvDataArray, err
	}
	return csvDataArray, nil
}
