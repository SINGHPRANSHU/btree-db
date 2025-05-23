package datatype


type DataType interface {
	GetName() string
	GetSize() int
	GetRepresent() string
}
type Integer struct {
	name string
	size int
	represent string
}


type Char struct {
	name string
	size int
	represent string
	
}
// size is in bytes
func NewInteger(size int, name string) *Integer {
	return &Integer{
		size: size,
		name: name,
		represent: "Integer",
	}
}
func NewChar(size int, name string) *Char {
	return &Char{
		size: size,
		name: name,
		represent: "Char",
	}
}
func (i *Integer) GetSize() int {
	return i.size
}
func (i *Char) GetSize() int {
	return i.size
} 

func (i *Integer) GetName() string {
	return i.name
}
func (i *Char) GetName() string {
	return i.name
}

func (i *Integer) GetRepresent() string {
	return i.represent
}
func (i *Char) GetRepresent() string {
	return i.represent
}


