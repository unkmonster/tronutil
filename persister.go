package tronblocklistener

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
)

type Persister interface {
	SaveNextHeight(ctx context.Context, h int64) error
	LoadNextHeight(ctx context.Context) (int64, error)
}

var _ Persister = (*FilePersister)(nil)

type FilePersister struct {
	path string
}

func NewFilePersister(path string) *FilePersister {
	return &FilePersister{
		path: path,
	}
}

// LoadNextHeight implements [Persister].
func (f *FilePersister) LoadNextHeight(ctx context.Context) (int64, error) {
	data, err := os.ReadFile(f.path)
	if os.IsNotExist(err) {
		// 抑制文件不存在错误
		return 0, nil
	}
	if err != nil {
		return 0, fmt.Errorf("read file: %w", err)
	}

	rv, err := strconv.ParseInt(string(data), 10, 64)
	if err != nil {
		return 0, fmt.Errorf("parse %q: %w", string(data), err)
	}
	return rv, nil
}

// SaveNextHeight implements [Persister].
func (f *FilePersister) SaveNextHeight(ctx context.Context, h int64) error {
	data := []byte(strconv.FormatInt(h, 10))

	// 1. 在同一个目录下创建临时文件
	dir := filepath.Dir(f.path)
	tmpFile, err := os.CreateTemp(dir, "next_height_tmp_*")
	if err != nil {
		return err
	}
	tmpPath := tmpFile.Name()
	defer os.Remove(tmpPath) // 确保异常时清理

	// 2. 写入数据
	if _, err := tmpFile.Write(data); err != nil {
		tmpFile.Close()
		return err
	}

	// 3. 强制刷盘（防止掉电丢失）
	if err := tmpFile.Sync(); err != nil {
		tmpFile.Close()
		return err
	}
	tmpFile.Close()

	// 4. 原子替换原有文件
	return os.Rename(tmpPath, f.path)
}
