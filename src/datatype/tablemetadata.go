package datatype

import (
	"encoding/binary"
	"os"

	"github.com/singhpranshu/btree-db/src/constant"
)

type TableMetadata struct {
	name  string
	types []DataType
}

func (t *TableMetadata) GetName() string {
	return t.name
}

func NewTableMetadata(name string) *TableMetadata {
	return &TableMetadata{
		name:  name,
		types: []DataType{},
	}
}
func (t *TableMetadata) AddType(dataType DataType) {
	t.types = append(t.types, dataType)
}
func (t *TableMetadata) GetTypes() []DataType {
	return t.types
}

// This will contain 64 bit for size of type and 64 bit for column name length and 64 bit for type len and type and the name length
func (t *TableMetadata) Serialize() []byte {
	var serialized []byte
	nameBytes := []byte(t.name)
	nameLen := len(nameBytes)
	b := make([]byte, 8)
	binary.LittleEndian.PutUint64(b, uint64(nameLen))
	serialized = append(serialized, b...)
	serialized = append(serialized, nameBytes...)
	for _, dataType := range t.types {
		b := make([]byte, 8)
		binary.LittleEndian.PutUint64(b, uint64(dataType.GetSize()))
		serialized = append(serialized, b...)

		columnNameLen := len(dataType.GetName())
		binary.LittleEndian.PutUint64(b, uint64(columnNameLen))
		serialized = append(serialized, b...)

		typeNameLen := len(dataType.GetRepresent())
		binary.LittleEndian.PutUint64(b, uint64(typeNameLen))
		serialized = append(serialized, b...)

		serialized = append(serialized, []byte(dataType.GetRepresent())...)

		serialized = append(serialized, []byte(dataType.GetName())...)

	}
	return serialized
}

func Deserialize(data []byte) *TableMetadata {
	tableMetadata := &TableMetadata{}
	i := 0
	nameLen := binary.LittleEndian.Uint64(data[i : i+8])
	i += 8
	name := string(data[i : i+int(nameLen)])
	i += int(nameLen)
	tableMetadata.name = name
	for i < len(data) {
		size := binary.LittleEndian.Uint64(data[i : i+8])
		i += 8
		nameLen := binary.LittleEndian.Uint64(data[i : i+8])
		i += 8
		typeLen := binary.LittleEndian.Uint64(data[i : i+8])
		i += 8
		typeName := string(data[i : i+int(typeLen)])
		i += int(typeLen)
		name := string(data[i : i+int(nameLen)])
		i += int(nameLen)
		var dataType DataType
		if typeName == "Integer" {
			dataType = NewInteger(int(size), name)
		} else if typeName == "Char" {
			dataType = NewChar(int(size), name)
		} else {
			panic("Unknown data type")
		}
		tableMetadata.AddType(dataType)
	}
	return tableMetadata
}

func (t *TableMetadata) Save(tableName string) error {
	serialized := t.Serialize()
	err := os.WriteFile(constant.RootFolder+"/"+tableName+"/"+"schema", serialized, 0644)
	if err != nil {
		return err
	}
	return nil
}

func Load(tableName string) (*TableMetadata, error) {
	data, err := os.ReadFile(constant.RootFolder + "/" + tableName + "/schema")
	if err != nil {
		return nil, err
	}
	return Deserialize(data), nil
}
