package bplustree

import (
	"encoding/binary"
	"os"

	"github.com/singhpranshu/btree-db/src/constant"
	"github.com/singhpranshu/btree-db/src/datatype"
	"github.com/singhpranshu/btree-db/src/storage"
)

func NewBPlusTree(size int64, indexName string, tableName string, tableMeta *datatype.TableMetadata) *BPlusTree {

	// check if index name already exist
	if _, err := os.Stat(constant.RootFolder + "/" + tableName + "/" + indexName); !os.IsNotExist(err) {
		panic("index already exists")
	}

	store := *storage.NewFileStorage(constant.RootFolder+"/"+tableName+"/"+indexName, storage.NewMutex())
	valueStore := *storage.NewFileStorage(constant.RootFolder+"/"+tableName+"/"+"value", storage.NewMutex())
	err := store.CreateDirectory(constant.RootFolder + "/" + tableName)
	if err != nil {
		panic("failed to create directory")
	}
	tableMeta.Save(tableName)
	table, err := datatype.Load(tableName)
	if err != nil {
		panic("failed to load table")
	}

	degree = 2 * size
	Root := &Node{
		Keys:       make([]int64, 2*size-1),
		Children:   make([]int64, 2*size),
		Leaf:       true,
		Count:      0,
		store:      store,
		table:      table,
		valueStore: valueStore,
	}
	btree := &BPlusTree{
		Size:       size,
		store:      store,
		table:      table,
		valueStore: valueStore,
	}

	rootPositionOnDisk := addPadding(CalculateNodeSize(degree))
	b := make([]byte, 8)
	binary.LittleEndian.PutUint64(b, uint64(CalculateNodeSize(degree)))
	for i := int64(0); i < 8; i++ {
		rootPositionOnDisk[i] = b[i]
	}
	// rootPositionOnDisk[0] = byte(CalculateNodeSize(degree))

	_, err = store.Append(rootPositionOnDisk)
	// TODO: handle err case

	_, err = store.Append(Root.Serialize())

	if err != nil {
		// TODO: handle err case
		panic("failed to append to file")
	}

	return btree
}
