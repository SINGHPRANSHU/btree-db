package bplustree

import (
	"encoding/binary"

	"github.com/singhpranshu/btree-db/src/datatype"
	"github.com/singhpranshu/btree-db/src/storage"
	"github.com/singhpranshu/btree-db/src/transform"
)

// var store storage.Storage
var degree int64

type Node struct {
	Keys       []int64
	Children   []int64
	Leaf       bool
	Count      int64
	Next       int64
	offset     int64
	store      storage.Storage
	table      *datatype.TableMetadata
	valueStore storage.Storage
}

type BPlusTree struct {
	Size       int64
	store      storage.Storage
	table      *datatype.TableMetadata
	valueStore storage.Storage
}

func (btree *BPlusTree) GetStore() storage.Storage {
	return btree.store
}
func (btree *BPlusTree) GetValueStore() storage.Storage {
	return btree.valueStore
}
func (btree *BPlusTree) GetTable() *datatype.TableMetadata {
	return btree.table
}
func (btree *BPlusTree) GetDegree() int64 {
	return degree
}

func (node *Node) getChild(position int64) *Node {
	if node.Leaf {
		return nil
	}
	res, err := node.store.GetAtPosition(position, CalculateNodeSize(degree))
	if err != nil {
		return nil
	}
	fetchedNode := Deserialize(res, node.store, node.table, node.valueStore)
	fetchedNode.offset = position
	fetchedNode.store = node.store
	return fetchedNode
}

func (node *Node) setChild(position int64, newNodePosition int64) error {
	node.Children[position] = newNodePosition
	// err := store.UpdateAt(node.offset, node.Serialize())
	// if err != nil {
	// 	panic("failed to update node")
	// }

	return nil
}
func (node *Node) save() error {
	err := node.store.UpdateAt(node.offset, node.Serialize())
	if err != nil {
		panic("failed to update node")
	}

	return nil
}

func (node *Node) saveValue(value map[string]interface{}) int64 {
	byteData := transform.TransformTableValue(node.table, value)
	pos, err := node.valueStore.Append(byteData)
	if err != nil {
		panic("failed to append to file")
	}
	types := node.table.GetTypes()
	for _, typ := range types {
		if typ.GetRepresent() == "Integer" {
			pos -= int64(typ.GetSize())
		} else if typ.GetRepresent() == "Char" {
			pos -= int64(typ.GetSize())
		} else {
			panic("unsupported data type")
		}
	}
	return pos
}

func (node *Node) updateValue(value map[string]interface{}, position int64) error {
	byteData := transform.TransformTableValue(node.table, value)
	err := node.valueStore.UpdateAt(position, byteData)
	if err != nil {
		panic("failed to update value")
	}
	return nil
}

func (t *BPlusTree) getRootPosition() (int64, error) {
	rootPosition, err := t.store.GetAtPosition(0, CalculateNodeSize(degree))
	if err != nil {
		return 0, err
	}
	pos := rootPosition[:8]
	ipos := int64(binary.LittleEndian.Uint64(pos))
	return ipos, nil
}

func (t *BPlusTree) GetRoot() *Node {
	rootPosition, err := t.getRootPosition()
	if err != nil {
		return nil
	}
	rootNode, err := t.store.GetAtPosition(rootPosition, CalculateNodeSize(degree))
	if err != nil {
		return nil
	}
	node := Deserialize(rootNode, t.store, t.table, t.valueStore)
	node.offset = rootPosition
	node.store = t.store
	return node
}

// 10 17  25  35  50
// 5 6 7  //10 12  15   // 17 20      // 25 30     // 35 40     // 50  60  70  80

func (t *BPlusTree) Insert(key int64, value map[string]interface{}) {
	root := t.GetRoot()
	if root.Count == 2*t.Size-1 {
		newNode := &Node{
			Keys:       make([]int64, 2*t.Size-1),
			Children:   make([]int64, 2*t.Size),
			Leaf:       false,
			Count:      0,
			store:      t.store,
			table:      t.table,
			valueStore: t.valueStore,
		}
		rootPosition, err := t.store.Append(newNode.Serialize())
		if err != nil {
			panic("failed to append to file")
		}
		rootPosition -= CalculateNodeSize(degree)
		newNode.offset = rootPosition

		rootPositionOnDisk := addPadding(CalculateNodeSize(degree))
		b := make([]byte, 8)
		binary.LittleEndian.PutUint64(b, uint64(rootPosition))
		for i := int64(0); i < 8; i++ {
			rootPositionOnDisk[i] = b[i]
		}
		t.store.UpdateAt(0, rootPositionOnDisk)
		prevRoot := root
		newNode.setChild(0, prevRoot.offset)
		newNode.save()
		t.splitChild(newNode, 0)
		t.insertNonFull(newNode, key, value)
	} else {
		t.insertNonFull(root, key, value)
	}
}
func (t *BPlusTree) insertNonFull(node *Node, key int64, value map[string]interface{}) {
	i := node.Count - 1
	if node.Leaf {
		node.Count++
		// node.Keys[node.Count-1] = 0
		for i >= 0 && key < node.Keys[i] {
			node.Keys[i+1] = node.Keys[i]
			node.Children[i+1] = node.Children[i]
			i--
		}
		node.Keys[i+1] = key
		pos := node.saveValue(value)
		node.Children[i+1] = pos
		node.save()
	} else {
		for i >= 0 && key < node.Keys[i] {
			i--
		}
		i++
		child := node.getChild(node.Children[i])
		if child.Count == 2*t.Size-1 {
			t.splitChild(node, i)
			if key > node.Keys[i] {
				i++
			}
		}
		child = node.getChild(node.Children[i])
		t.insertNonFull(child, key, value)
	}
}
func (t *BPlusTree) splitChild(parent *Node, index int64) {
	child := parent.getChild(parent.Children[index])
	newChild := &Node{
		Keys:       make([]int64, 2*t.Size-1),
		Children:   make([]int64, 2*t.Size),
		Leaf:       child.Leaf,
		Count:      t.Size,
		store:      t.store,
		table:      t.table,
		valueStore: t.valueStore,
	}

	for j := int64(0); j < t.Size; j++ {
		newChild.Keys[j] = child.Keys[j+t.Size-1]
	}
	for j := int64(0); j <= t.Size; j++ {
		// newChild.Children[j] = child.Children[j+t.Size-1]
		newChild.setChild(j, child.Children[j+t.Size-1])
	}

	child.Count = t.Size - 1
	parent.Count++
	for j := int64(parent.Count - 1); j > index; j-- {
		// parent.Children[j+1] = parent.Children[j]
		parent.setChild(j+1, parent.Children[j])
	}
	offset, err := t.store.Append(newChild.Serialize())
	if err != nil {
		panic("failed to append to file")
	}
	offset -= CalculateNodeSize(degree)
	newChild.offset = offset
	// parent.Children[index+1] = newChild
	parent.setChild(index+1, newChild.offset)
	for j := parent.Count - 2; j >= index; j-- {
		parent.Keys[j+1] = parent.Keys[j]
	}
	parent.Keys[index] = newChild.Keys[0]
	if child.Leaf {
		temp := child.Next
		child.Next = newChild.offset
		newChild.Next = temp
	}
	parent.save()
	newChild.save()
	child.save()
}

// keys =   3 * 8byte
// children = 4 * 4byte
// 40 + 48 + 8 + 8 + 1 =

func (node *Node) Serialize() []byte {
	resultByteArr := make([]byte, 0)
	for i := int64(0); i < int64(len(node.Keys)); i++ {
		b := make([]byte, 8)
		binary.LittleEndian.PutUint64(b, uint64(node.Keys[i]))
		resultByteArr = append(resultByteArr, b...)
	}
	for i := int64(0); i < int64(len(node.Children)); i++ {
		b := make([]byte, 8)
		binary.LittleEndian.PutUint64(b, uint64(node.Children[i]))
		resultByteArr = append(resultByteArr, b...)
	}

	b := make([]byte, 8)
	binary.LittleEndian.PutUint64(b, uint64(node.Count))
	resultByteArr = append(resultByteArr, b...)
	if node.Leaf {
		resultByteArr = append(resultByteArr, 1)
	} else {
		resultByteArr = append(resultByteArr, 0)
	}
	b = make([]byte, 8)
	binary.LittleEndian.PutUint64(b, uint64(node.Next))
	resultByteArr = append(resultByteArr, b...)
	return resultByteArr
}

func Deserialize(data []byte, store storage.Storage, table *datatype.TableMetadata, valueStore storage.Storage) *Node {
	node := &Node{}
	next := data[len(data)-8:]
	// inext, _ := strconv.ParseInt(next, 10, 64)
	inext := int64(binary.LittleEndian.Uint64(next))
	node.Next = inext

	leaf := int64(data[len(data)-8-1 : len(data)-8][0])
	node.Leaf = leaf == 1

	count := data[len(data)-8-1-8 : len(data)-8-1]
	// icount, _ := strconv.ParseInt(count, 10, 64)
	icount := int64(binary.LittleEndian.Uint64(count))
	node.Count = icount

	node.Keys = make([]int64, degree-1)
	node.Children = make([]int64, degree)

	for i := int64(0); i < node.Count*8-1; i = i + 8 {
		key := data[i : i+8]
		ikey := int64(binary.LittleEndian.Uint64(key))

		node.Keys[i/8] = ikey
	}

	for i := int64(0) + 8*(degree-1); i <= 8*(degree-1)+node.Count*8; i = i + 8 {
		child := data[i : i+8]
		ichild := int64(binary.LittleEndian.Uint64(child))
		node.Children[(i-8*(degree-1))/8] = ichild
	}

	node.store = store
	node.table = table
	node.valueStore = valueStore
	return node
}

func CalculateNodeSize(degree int64) int64 {
	pointerSize := int64(8)
	val := 1 + (degree * pointerSize) + pointerSize + ((degree - 1) * 8) + 8
	i := val % 8
	return val
	if i == 0 {
		return val
	}
	return val + (8 - i)
}

func addPadding(size int64) []byte {
	byteArr := []byte{}
	for i := int64(0); i < size; i++ {
		byteArr = append(byteArr, byte(0))
	}
	return byteArr
}

func (t *BPlusTree) Search(key int64) (*Node, map[string]interface{}) {
	root := t.GetRoot()
	node := t.search(root, key)
	if node == nil {
		panic("key not found")
	}
	node.store = t.store
	node.table = t.table
	tab := node.table.GetTypes()
	size := 0
	for _, typ := range tab {
		if typ.GetRepresent() == "Integer" {
			size += typ.GetSize()
		} else if typ.GetRepresent() == "Char" {
			size += typ.GetSize()
		} else {
			panic("unsupported data type")
		}
	}
	i := 0
	for i = 0; i < int(node.Count); i++ {
		if node.Keys[i] == key {
			break
		}
	}
	valueByte, err := node.valueStore.GetAtPosition(node.Children[i], int64(size))
	if err != nil {
		panic("failed to get value")
	}
	mapValue := transform.TransformTableValueToMap(node.table, valueByte)

	return node, mapValue
}
func (t *BPlusTree) search(node *Node, key int64) *Node {
	if node == nil {
		return nil
	}
	i := node.Count - 1
	for i >= 0 && key < node.Keys[i] {
		i--
	}
	if i < node.Count && i >= 0 && node.Leaf && key == node.Keys[i] {
		return node
	}
	i++
	if node.Leaf {
		panic("key not found")
	}
	child := node.getChild(node.Children[i])
	return t.search(child, key)
}

func (t *BPlusTree) Delete(key int64) {
	root := t.GetRoot()
	if root == nil {
		return
	}
	t.delete(root, key)
}

func (t *BPlusTree) delete(node *Node, key int64) {
	if node == nil {
		return
	}
	i := node.Count - 1
	for i >= 0 && key < node.Keys[i] {
		i--
	}
	if i < node.Count && i >= 0 && node.Leaf && key == node.Keys[i] {
		node.Count--
		for j := i; j < node.Count; j++ {
			node.Keys[j] = node.Keys[j+1]
			node.Children[j] = node.Children[j+1]
		}
		node.save()
		return
	}
	if node.Leaf {
		panic("key not found")
	}
	i++
	child := node.getChild(node.Children[i])
	t.delete(child, key)
	child = node.getChild(node.Children[i])
	if child.Count < t.Size-1 {
		t.rebalance(node, i)
	}
	child = node.getChild(node.Children[i])
	println("Deleted key:", key)
}

//                  25  50
//          10  17                                      25  35          50  70  90
// 5   6   7       10   12  15        17 20       25 30     35  40       50  60        70  80      90  100  110  120  1200000

func (t *BPlusTree) rebalance(parent *Node, index int64) {
	child := parent.getChild(parent.Children[index])
	if index > 0 {
		leftSibling := parent.getChild(parent.Children[index-1])
		if leftSibling.Count > t.Size-1 {
			if parent.Keys[index] != child.Keys[0] {
				child.Keys = append([]int64{parent.Keys[index]}, child.Keys...)
				child.Children[0] = leftSibling.Children[leftSibling.Count]
			} else {
				child.Children = append([]int64{leftSibling.Children[leftSibling.Count]}, child.Children...)
			}
			child.Count++
			leftSibling.Count--
			parent.Keys[index] = leftSibling.Keys[leftSibling.Count-1]
			leftSibling.save()
			child.save()
			parent.save()
			return
		}
	}

	if index < parent.Count {
		rightSibling := parent.getChild(parent.Children[index+1])
		if rightSibling.Count > t.Size-1 {
			// child.Keys = append(child.Keys, parent.Keys[index])
			// child.Children = append(child.Children, rightSibling.Children[0])
			child.Keys[child.Count] = parent.Keys[index]
			child.Children[child.Count+1] = rightSibling.Children[0]
			child.Count++
			parent.Keys[index] = rightSibling.Keys[1]
			for j := 0; j < int(rightSibling.Count); j++ {
				rightSibling.Keys[j] = rightSibling.Keys[j+1]
				rightSibling.Children[j] = rightSibling.Children[j+1]
			}
			rightSibling.Count--
			rightSibling.save()
			child.save()
			parent.save()
			return
		}
	}

	if index > 0 {
		leftSibling := parent.getChild(parent.Children[index-1])
		if parent.Keys[index-1] != child.Keys[0] {
			leftSibling.Keys[leftSibling.Count] = parent.Keys[index-1]
			leftSibling.Children[leftSibling.Count+1] = child.Children[0]
			leftSibling.Count++
		}
		for j := int64(0); j < int64(child.Count); j++ {
			leftSibling.Keys[leftSibling.Count+j] = child.Keys[j]
			leftSibling.Children[leftSibling.Count+j+1] = child.Children[j]
		}
		leftSibling.Count += child.Count
		leftSibling.Children[leftSibling.Count] = child.Children[child.Count]
		parent.deleteChild(index)
		leftSibling.save()
	} else {
		rightSibling := parent.getChild(parent.Children[index+1])
		child.Keys[child.Count] = parent.Keys[index-1]
		for j := int64(0); j < int64(rightSibling.Count); j++ {
			child.Keys[child.Count+j] = rightSibling.Keys[j]
			child.Children[child.Count+j] = rightSibling.Children[j]
		}
		child.Count += rightSibling.Count
		child.Children[child.Count] = rightSibling.Children[rightSibling.Count]
		parent.deleteChild(index + 1)
		child.save()
	}
	parent.save()
}
func (node *Node) deleteChild(index int64) {
	i := index
	for i = index; i < node.Count-1; i++ {
		node.Keys[i] = node.Keys[i+1]
		node.Children[i+1] = node.Children[i+2]
	}
	node.Count--
	node.save()
}

func (t *BPlusTree) Update(key int64, newValue map[string]interface{}) {
	node, oldValue := t.Search(key)
	if node == nil {
		panic("key not found")
	}
	if oldValue == nil {
		panic("old value not found")
	}
	node.updateValue(newValue, node.Children[0])
}
