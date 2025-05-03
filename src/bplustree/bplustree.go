package bplustree

import (
	"encoding/binary"
	"fmt"

	"github.com/singhpranshu/btree-db/src/storage"
)

var store storage.Storage
var degree int64

type Node struct {
	Keys     []int64
	Children []int64
	Leaf     bool
	Count    int64
	Next     int64
	offset   int64
}

type BPlusTree struct {
	Size int64
	Root *Node
}

func (node *Node) getChild(position int64) *Node {
	if node.Leaf {
		return nil
	}
	res, err := store.GetAtPosition(position, CalculateNodeSize(degree))
	if err != nil {
		return nil
	}
	fetchedNode := Deserialize(res)
	fetchedNode.offset = position
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
	err := store.UpdateAt(node.offset, node.Serialize())
	if err != nil {
		panic("failed to update node")
	}

	return nil
}

func NewBPlusTree(size int64) *BPlusTree {
	store = *storage.NewFileStorage("btree.db")
	degree = 2 * size
	btree := &BPlusTree{
		Root: &Node{
			Keys:     make([]int64, 2*size-1),
			Children: make([]int64, 2*size),
			Leaf:     true,
			Count:    0,
		},
		Size: size,
	}

	rootPositionOnDisk := addPadding(CalculateNodeSize(degree))
	b := make([]byte, 8)
	binary.LittleEndian.PutUint64(b, uint64(CalculateNodeSize(degree)))
	for i := int64(0); i < 8; i++ {
		rootPositionOnDisk[i] = b[i]
	}
	// rootPositionOnDisk[0] = byte(CalculateNodeSize(degree))

	_, err := store.Append(rootPositionOnDisk)
	// TODO: handle err case

	_, err = store.Append(btree.Root.Serialize())

	if err != nil {
		// TODO: handle err case
		panic("failed to append to file")
	}

	return btree
}

func getRootPosition() (int64, error) {
	rootPosition, err := store.GetAtPosition(0, CalculateNodeSize(degree))
	if err != nil {
		return 0, err
	}
	pos := rootPosition[:8]
	ipos := int64(binary.LittleEndian.Uint64(pos))
	return ipos, nil
}

func GetRoot() *Node {
	rootPosition, err := getRootPosition()
	if err != nil {
		return nil
	}
	rootNode, err := store.GetAtPosition(rootPosition, CalculateNodeSize(degree))
	if err != nil {
		return nil
	}
	node := Deserialize(rootNode)
	node.offset = rootPosition
	return node
}
// 10 17  25  35  50
// 5 6 7  //10 12  15   // 17 20      // 25 30     // 35 40     // 50  60  70  80 

func (t *BPlusTree) Insert(key int64) {
	fmt.Println(key)
	root := GetRoot()
	if key == 90 {
		fmt.Println("debugger stop")
	}
	if root.Count == 2*t.Size-1 {
		newNode := &Node{
			Keys:     make([]int64, 2*t.Size-1),
			Children: make([]int64, 2*t.Size),
			Leaf:     false,
			Count:    0,
		}
		rootPosition, err := store.Append(newNode.Serialize())
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
		store.UpdateAt(0, rootPositionOnDisk)
		prevRoot := root
		newNode.setChild(0, prevRoot.offset)
		newNode.save()
		t.splitChild(newNode, 0)
		t.insertNonFull(newNode, key)
	} else {
		t.insertNonFull(root, key)
	}
}
func (t *BPlusTree) insertNonFull(node *Node, key int64) {
	i := node.Count - 1
	if node.Leaf {
		node.Count++
		node.Keys[node.Count-1] = 0
		for i >= 0 && key < node.Keys[i] {
			node.Keys[i+1] = node.Keys[i]
			i--
		}
		node.Keys[i+1] = key
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
		t.insertNonFull(child, key)
	}
}
func (t *BPlusTree) splitChild(parent *Node, index int64) {
	child := parent.getChild(parent.Children[index])
	newChild := &Node{
		Keys:     make([]int64, 2*t.Size-1),
		Children: make([]int64, 2*t.Size),
		Leaf:     child.Leaf,
		Count:    t.Size,
	}

	for j := int64(0); j < t.Size; j++ {
		newChild.Keys[j] = child.Keys[j+t.Size-1]
	}
	if !child.Leaf {
		for j := int64(0); j <= t.Size; j++ {
			// newChild.Children[j] = child.Children[j+t.Size-1]
			newChild.setChild(j, child.Children[j+t.Size-1])
		}
	}
	child.Count = t.Size - 1
	parent.Count++
	for j := int64(parent.Count - 1); j > index; j-- {
		// parent.Children[j+1] = parent.Children[j]
		parent.setChild(j+1, parent.Children[j])
	}
	offset, err := store.Append(newChild.Serialize())
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

func Deserialize(data []byte) *Node {
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
	if !node.Leaf {
		for i := int64(0) + 8*(degree-1); i <= 8*(degree-1) + node.Count*8; i = i + 8 {
			child := data[i : i+8]
			ichild := int64(binary.LittleEndian.Uint64(child))
			node.Children[(i - 8*(degree-1))/8] = ichild
		}
	}
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
