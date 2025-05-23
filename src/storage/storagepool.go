package storage

type StoragePool struct {
	workerPool	 chan int
}


func NewStoragePool() *StoragePool {
	workerPool := make(chan int, 4)
	workerPool <- 1
	workerPool <- 1
	workerPool <- 1
	workerPool <- 1
	return &StoragePool{
		workerPool: workerPool,
	}
}

func (storagePool *StoragePool) ReleaseWorker() {
	storagePool.workerPool <- 1
}

func (storagePool *StoragePool) GetWorker() {
	<-storagePool.workerPool
}