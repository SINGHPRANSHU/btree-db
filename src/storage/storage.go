package storage

import (
	"errors"
	"io"
	"os"
)

type Storage interface {
	Append(node []byte) (int64, error)
	GetAtPosition(position int64, nodeSize int64) ([]byte, error)
	UpdateAt(position int64, node []byte) error
}

type FileStorage struct {
	fileName string
}

func NewFileStorage(fileName string) *FileStorage {
	return &FileStorage{
		fileName: fileName,
	}
}
func (fs FileStorage) Append(node []byte) (int64, error) {
	file, err := os.OpenFile(fs.fileName, os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0644)
	if err != nil {
		return 0, errors.New("failed to open file")
	}
	defer file.Close()
	_, err = file.Write(node)
	if err != nil {
		return 0, errors.New("failed to write to file")
	}
	if err := file.Sync(); err != nil {
		return 0, errors.New("failed to sync file")
	}
	offset, err := file.Seek(0, io.SeekEnd)
	if err != nil {
		return 0, errors.New("failed to seek file")
	}
	return offset, nil

}

func (fs FileStorage) GetAtPosition(position int64, nodeSize int64) ([]byte, error) {
	file, err := os.OpenFile(fs.fileName, os.O_RDONLY, 0644)
	if err != nil {
		return nil, errors.New("failed to open file")
	}
	defer file.Close()
	file.Seek(position, 0)
	node := make([]byte, nodeSize)
	n, err := file.Read(node)
	if err != nil {
		return nil, errors.New("failed to read from file")
	}
	return node[:n], nil
}

func (fs FileStorage) UpdateAt(position int64, node []byte) error {
	file, err := os.OpenFile(fs.fileName, os.O_RDWR, 0644)
	if err != nil {
		return errors.New("failed to open file")
	}
	defer file.Close()
	_, err = file.Seek(position, 0)
	if err != nil {
		return errors.New("failed to seek file")
	}
	_, err = file.WriteAt(node, position)
	if err != nil {
		return errors.New("failed to write to file")
	}
	return nil
}
