package transform

import (
	"encoding/binary"

	"github.com/singhpranshu/btree-db/src/datatype"
)

func TransformTableValue(table *datatype.TableMetadata, value map[string]interface{}) []byte {
	var serialized []byte
	for _, dataType := range table.GetTypes() {
		switch dataType.GetRepresent() {
		case "Integer":
			v := int64(value[dataType.GetName()].(int))
			b := make([]byte, 8)
			binary.LittleEndian.PutUint64(b, uint64(v))
			serialized = append(serialized, b...)
		case "Char":
			b := make([]byte, dataType.GetSize())
			v := []byte(value[dataType.GetName()].(string))
			for i := 0; i < len(v); i++ {
				b[i] = v[i]
			}
			serialized = append(serialized, b...)
		default:
			panic("unsupported data type")
		}
	}
	return serialized
}

func TransformTableValueToMap(table *datatype.TableMetadata, data []byte) map[string]interface{} {
	result := make(map[string]interface{})
	i := 0
	for _, dataType := range table.GetTypes() {
		switch dataType.GetRepresent() {
		case "Integer":
			v := binary.LittleEndian.Uint64(data[i : i+8])
			result[dataType.GetName()] = int64(v)
			i += 8
		case "Char":
			v := string(data[i : i+dataType.GetSize()])
			result[dataType.GetName()] = v
			i += dataType.GetSize()
		default:
			panic("unsupported data type")
		}
	}
	return result
}
