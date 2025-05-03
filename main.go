package main

import (
	"fmt"
	"os"

	"github.com/singhpranshu/btree-db/src/bplustree"
)

func main() {
	// This is a placeholder for the main function.
	// The actual implementation will depend on the specific requirements of your application.

	// btree := btree.NewBTree(3)
	os.Remove("btree.db")
	btree := bplustree.NewBPlusTree(3)

	// 25
	//  10  17   // 25 35 60
	// 5 6  7          // 10 12 15    //17 20    // 25  30 35    // 40  50    // 60  70  80 90 100

	btree.Insert(10)
	fmt.Println(btree.Root.Keys, btree.Root.Count)
	btree.Insert(20)
	fmt.Println(btree.Root.Keys, btree.Root.Count)
	btree.Insert(5)
	fmt.Println(btree.Root.Keys, btree.Root.Count)
	btree.Insert(6)
	fmt.Println(btree.Root.Keys)
	btree.Insert(12)
	fmt.Println(btree.Root.Keys)
	btree.Insert(30)
	fmt.Println(btree.Root.Keys, btree.Root.Count)
	btree.Insert(7)
	fmt.Println(btree.Root.Keys, btree.Root.Count)
	btree.Insert(17)
	fmt.Println(btree.Root.Keys, btree.Root.Count)
	btree.Insert(15)
	fmt.Println(btree.Root.Keys, btree.Root.Count)
	btree.Insert(25)
	fmt.Println(btree.Root.Keys, btree.Root.Count)
	btree.Insert(35)
	fmt.Println(btree.Root.Keys, btree.Root.Count)
	btree.Insert(40)
	fmt.Println(btree.Root.Keys, btree.Root.Count)
	btree.Insert(50)
	fmt.Println(btree.Root.Keys, btree.Root.Count)
	btree.Insert(60)
	fmt.Println(btree.Root.Keys, btree.Root.Count)
	btree.Insert(70)
	fmt.Println(btree.Root.Keys, btree.Root.Count)
	btree.Insert(80)
	fmt.Println(btree.Root.Keys, btree.Root.Count)
	btree.Insert(90)
	fmt.Println(btree.Root.Keys, btree.Root.Count)
	btree.Insert(100)
	fmt.Println(btree.Root.Keys, btree.Root.Count)
	btree.Insert(110)
	fmt.Println(btree.Root.Keys, btree.Root.Count)
	btree.Insert(120)
	fmt.Println(btree.Root.Keys, btree.Root.Count)

	node := btree.Root

	// for !node.Leaf {
	// 	node = node.Children[0]
	// }
	// fmt.Println("start")
	// for node.Next != nil {
	// 	fmt.Println(node.Keys, node.Count)
	// 	node = node.Next
	// }
	fmt.Println(node.Keys, node.Count)

}
