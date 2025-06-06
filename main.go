package main

import (
	"fmt"
	"os"

	"github.com/singhpranshu/btree-db/src/bplustree"
	"github.com/singhpranshu/btree-db/src/connection"
	"github.com/singhpranshu/btree-db/src/constant"
	"github.com/singhpranshu/btree-db/src/datatype"
	"github.com/singhpranshu/btree-db/src/parser"
	"github.com/singhpranshu/btree-db/src/runner"
)

//                  25  50
//          10  17           25  35          50  70  90
// 5   6   7       10   12  15        17 20       25 30      35  40       50  60        70  80      90  100  110  120  1200000

func main() {
	// This is a placeholder for the main function.
	// The actual implementation will depend on the specific requirements of your application.

	// btree := btree.NewBTree(3)

	query := "Query"
	// tableName := "user"
	var btree *bplustree.BPlusTree

	if query == "Create" {
		indexName := "primaryKey"
		tableName := "user"
		// check if table name dir exist
		if _, err := os.Stat(constant.RootFolder + "/" + tableName); !os.IsNotExist(err) {
			panic("table already exists")
		}
		// os.Remove(constant.RootFolder + "/" + tableName + "/" + indexName)

		err := os.Mkdir(constant.RootFolder, os.ModePerm)
		if err != nil && !os.IsExist(err) {
			fmt.Println("Error creating folder:", err)
			panic("failed to root folder")
		}
		tableMeta := datatype.NewTableMetadata("user")
		tableMeta.AddType(datatype.NewInteger(8, "id"))
		tableMeta.AddType(datatype.NewChar(8, "name"))
		btree = bplustree.NewBPlusTree(3, indexName, tableName, tableMeta)
		btree.Insert(10, map[string]interface{}{"id": 10, "name": "pranshu"})

		btree.Insert(20, map[string]interface{}{"id": 20, "name": "pranshu"})

		btree.Insert(5, map[string]interface{}{"id": 5, "name": "pranshu"})

		btree.Insert(6, map[string]interface{}{"id": 6, "name": "pranshu"})
		btree.Insert(12, map[string]interface{}{"id": 12, "name": "pranshu"})
		btree.Insert(30, map[string]interface{}{"id": 30, "name": "pranshu"})

		btree.Insert(7, map[string]interface{}{"id": 7, "name": "pranshu"})

		btree.Insert(17, map[string]interface{}{"id": 17, "name": "pranshu"})

		btree.Insert(15, map[string]interface{}{"id": 15, "name": "pranshu"})

		btree.Insert(25, map[string]interface{}{"id": 25, "name": "pranshu"})

		btree.Insert(35, map[string]interface{}{"id": 35, "name": "pranshu"})

		btree.Insert(40, map[string]interface{}{"id": 40, "name": "pranshu"})

		btree.Insert(50, map[string]interface{}{"id": 50, "name": "pranshu"})

		btree.Insert(60, map[string]interface{}{"id": 60, "name": "pranshu"})

		btree.Insert(70, map[string]interface{}{"id": 70, "name": "pranshu"})

		btree.Insert(80, map[string]interface{}{"id": 80, "name": "pranshu"})

		btree.Insert(90, map[string]interface{}{"id": 90, "name": "pranshu"})

		btree.Insert(100, map[string]interface{}{"id": 100, "name": "pranshu"})

		btree.Insert(110, map[string]interface{}{"id": 110, "name": "pranshu"})

		btree.Insert(120, map[string]interface{}{"id": 120, "name": "pranshu"})
	} else {
		btrees := bplustree.LoadAllExistingBPlusTree()
		run := runner.NewRunner(btrees, parser.NewParser())
		err := connection.NewDBServer("localhost", "8081", run).Start()
		if err != nil {
			panic("failed to start db server: " + err.Error())
		}
		// for _, b := range btrees {
		// 	if b.GetTable().GetName() == tableName {
		// 		btree = b
		// 		break
		// 	}
		// }

	}

	// 25
	//  10  17   // 25 35 60
	// 5 6  7          // 10 12 15    //17 20    // 25  30 35    // 40  50    // 60  70  80 90 100

	// node := btree.GetRoot()

	// fmt.Println(node.Keys, node.Count)
	// nod, valData := btree.Search(120)
	// fmt.Println(nod.Keys, nod.Count, valData)

	// btree.Insert(1200000000000000, map[string]interface{}{"id": 1200000000000000, "name": "pranshu"})
	// nod, valData = btree.Search(90)
	// fmt.Println(nod.Keys, nod.Count, valData)
	// btree.Update(90, map[string]interface{}{"id": 1200000000000000, "name": "prashhu"})
	// nod, valData = btree.Search(90)
	// fmt.Println(nod.Keys, nod.Count, valData)
	// nod, valData = btree.Search(35)
	// fmt.Println(nod.Keys, nod.Count, valData)
	// btree.Delete(35)
	// nod, valData = btree.Search(30)
	// fmt.Println(nod.Keys, nod.Count, valData)

}
