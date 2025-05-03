package btree

type Node struct {
	Keys    []int64
	Chidren []*Node
	Leaf    bool
	Count   int
}

type BTree struct {
	Size int
	Root *Node
}

func NewBTree(size int) *BTree {
	return &BTree{
		Root: &Node{
			Keys:    make([]int64, 2*size-1),
			Chidren: make([]*Node, 2*size),
			Leaf:    true,
			Count:   0,
		},
		Size: size,
	}
}

func (t *BTree) Insert(key int64) {
	if t.Root.Count == 2*t.Size-1 {
		newNode := &Node{
			Keys:    make([]int64, 2*t.Size-1),
			Chidren: make([]*Node, 2*t.Size),
			Leaf:    false,
			Count:   0,
		}
		prevRoot := t.Root
		t.Root = newNode
		t.Root.Chidren[0] = prevRoot
		t.splitChild(t.Root, 0)
		t.insertNonFull(t.Root, key)
	} else {
		t.insertNonFull(t.Root, key)
	}
}

func (t *BTree) insertNonFull(node *Node, key int64) {
	i := node.Count - 1
	if node.Leaf {
		node.Count++
		node.Keys[node.Count-1] = 0
		for i >= 0 && key < node.Keys[i] {
			node.Keys[i+1] = node.Keys[i]
			i--
		}
		node.Keys[i+1] = key
	} else {
		for i >= 0 && key < node.Keys[i] {
			i--
		}
		i++
		if node.Chidren[i].Count == 2*t.Size-1 {
			t.splitChild(node, i)
			if key > node.Keys[i] {
				i++
			}
		}
		t.insertNonFull(node.Chidren[i], key)
	}
}

func (t *BTree) splitChild(parent *Node, index int) {
	child := parent.Chidren[index]
	newChild := &Node{
		Keys:    make([]int64, 2 * t.Size-1),
		Chidren: make([]*Node, 2 * t.Size),
		Leaf:    child.Leaf,
		Count:   t.Size - 1,
	}

	for i := 0; i < t.Size-1; i++ {
		newChild.Keys[i] = child.Keys[i+t.Size]
	}
	if !child.Leaf {
		for i := 0; i < t.Size; i++ {
			newChild.Chidren[i] = child.Chidren[i+t.Size]
		}
	}
	child.Count = t.Size - 1
	parent.Chidren[index+1] = newChild
	parent.Keys[index] = child.Keys[t.Size-1]
	parent.Count++
}
