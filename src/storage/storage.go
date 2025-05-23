package storage

import (
	"errors"
	"fmt"
	"io"
	"os"
	"sync"
)

type Storage interface {
	Append(node []byte) (int64, error)
	GetAtPosition(position int64, nodeSize int64) ([]byte, error)
	UpdateAt(position int64, node []byte) error
	CreateDirectory(folderName string) error
}

type FileStorage struct {
	mutex *sync.RWMutex
	fileName string
	storagePool *StoragePool
}

func NewFileStorage(fileName string, mutex *sync.RWMutex) *FileStorage {
	storagePool := NewStoragePool()
	return &FileStorage{
		fileName: fileName,
		mutex: mutex,
		storagePool: storagePool,
	}
}
func (fs FileStorage) Append(node []byte) (int64, error) {
	fs.storagePool.GetWorker()
	defer fs.storagePool.ReleaseWorker()
	fs.mutex.Lock()
	file, err := os.OpenFile(fs.fileName, os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0644)
	if err != nil {
		fmt.Println(err)
		return 0, errors.New("failed to open file")
	}
	defer fs.mutex.Unlock()
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
	fs.storagePool.GetWorker()
	defer fs.storagePool.ReleaseWorker()
	fs.mutex.RLock()
	defer fs.mutex.RUnlock()
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
	fs.storagePool.GetWorker()
	defer fs.storagePool.ReleaseWorker()
	fs.mutex.Lock()
	defer fs.mutex.Unlock()
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


func (fs FileStorage) CreateDirectory(folderName string) error {
	err := os.Mkdir(folderName, os.ModePerm)
	if err != nil && !os.IsExist(err) {
		fmt.Println("Error creating folder:", err)
		return err
	}
	return nil
}

func NewMutex() *sync.RWMutex {
	return &sync.RWMutex{}
}
