package bplustree

import (
	"os"

	"github.com/singhpranshu/btree-db/src/constant"
	"github.com/singhpranshu/btree-db/src/datatype"
	"github.com/singhpranshu/btree-db/src/storage"
)

func LoadAllExistingBPlusTree() []*BPlusTree {
	dir, err := os.ReadDir(constant.RootFolder + "/")
	if err != nil {
		panic("failed to read directory")
	}
	var btrees []*BPlusTree
	for _, entry := range dir {
		if entry.IsDir() {
			btree := &BPlusTree{
				Size: 3,
			}
			degree = 2 * 3
			tableName := entry.Name()
			tableMeta, err := datatype.Load(tableName)
			if err != nil {
				panic("failed to load table")
			}
			btree.table = tableMeta
			tabledir, err := os.ReadDir(constant.RootFolder + "/" + tableName)
			if err != nil {
				panic("failed to read directory")
			}
			for _, entry := range tabledir {
				if !entry.IsDir() && entry.Name() != "schema" && entry.Name() != "value" {
					indexName := entry.Name()
					store := *storage.NewFileStorage(constant.RootFolder+"/"+tableName+"/"+indexName, storage.NewMutex())
					btree.store = store
				}
			}
			valueStore := *storage.NewFileStorage(constant.RootFolder+"/"+tableName+"/"+"value", storage.NewMutex())
			btree.valueStore = valueStore
			btrees = append(btrees, btree)

		}
	}
	return btrees
}
