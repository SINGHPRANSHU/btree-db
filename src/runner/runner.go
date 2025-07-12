package runner

import (
	"fmt"
	"os"
	"strconv"

	"github.com/singhpranshu/btree-db/src/bplustree"
	"github.com/singhpranshu/btree-db/src/constant"
	"github.com/singhpranshu/btree-db/src/datatype"
	"github.com/singhpranshu/btree-db/src/parser"
)

type Runner struct {
	Btrees []*bplustree.BPlusTree
	Parser *parser.Parser
}

func NewRunner(btrees []*bplustree.BPlusTree, parser *parser.Parser) *Runner {
	return &Runner{
		Btrees: btrees,
		Parser: parser,
	}
}
func (r *Runner) Run(query string) (interface{}, error) {
	// Parse the query
	parsedQuery, err := r.Parser.Parse(query)
	if err != nil {
		return nil, err
	}
	tableName, ok := parsedQuery.Data[parser.TableIndex][0].(string)
	if !ok {
		panic("table name not found in query")
	}
	var btree *bplustree.BPlusTree
	for _, b := range r.Btrees {
		if b.GetTable().GetName() == tableName {
			btree = b
			break
		}
	}
	if btree == nil && parsedQuery.StatementType != parser.CreateStmtIndex && parsedQuery.StatementType != parser.DropStmtIndex && parsedQuery.StatementType != parser.InsertStmtIndex {
		panic("table not found in btrees")
	}

	switch parsedQuery.StatementType {
	case parser.SelectStmtIndex:
		parsedValues, ok := parsedQuery.Data[parser.WhereValueIndex][0].(string)
		if !ok {
			panic("where value not found in query")
		}
		fmt.Println("parsedValues", parsedValues, parsedValues == "120")
		parseIntId, err := strconv.Atoi(parsedValues)
		if err != nil {
			panic("failed to parse id value")
		}

		_, data := btree.Search(int64(parseIntId))
		return data, nil
	case parser.CreateStmtIndex:
		parsedCol, ok := parsedQuery.Data[parser.CreateColumnIndex]
		if !ok {
			panic("column names not found in query")
		}
		parsedTypes, ok := parsedQuery.Data[parser.CreateTypeIndex]
		if !ok {
			panic("column types not found in query")
		}
		parsedSize, ok := parsedQuery.Data[parser.CreateSizeIndex]
		if !ok {
			panic("column sizes not found in query")
		}
		err := os.Mkdir(constant.RootFolder, os.ModePerm)
		if err != nil && !os.IsExist(err) {
			fmt.Println("Error creating folder:", err)
			panic("failed to root folder")
		}
		tableMeta := datatype.NewTableMetadata(tableName)
		for i := 0; i < len(parsedCol); i++ {
			colName, ok := parsedCol[i].(string)
			if !ok {
				panic("column name is not a string")
			}
			colType, ok := parsedTypes[i].(string)
			if !ok {
				panic("column type is not a string")
			}
			colSize, ok := parsedSize[i].(string)
			if !ok {
				panic("column size is not an int64")
			}
			colIntSize, err := strconv.Atoi(colSize)
			if err != nil {
				panic("failed to parse column size: " + colSize)
			}
			var dataType datatype.DataType
			if colType == "Integer" {
				dataType = datatype.NewInteger(int(colIntSize), colName)
			} else if colType == "Char" {
				dataType = datatype.NewChar(int(colIntSize), colName)
			} else {
				panic("unsupported data type: " + colType)
			}
			tableMeta.AddType(dataType)
		}
		btree = bplustree.NewBPlusTree(3, "primaryKey", tableName, tableMeta)
		r.Btrees = append(r.Btrees, btree)
		return nil, nil

	case parser.InsertStmtIndex:
		colValues, ok := parsedQuery.Data[parser.InsertColIndex]
		if !ok {
			panic("column values not found in query")
		}
		valValues, ok := parsedQuery.Data[parser.InsertValIndex]
		if !ok {
			panic("value values not found in query")
		}

		if len(colValues) != len(valValues) {
			panic("number of columns and values do not match")
		}
		//btree.Insert(10, map[string]interface{}{"id": 10, "name": "pranshu"})
		tableTypes := btree.GetTable().GetTypes()

		data := make(map[string]interface{})
		for i := 0; i < len(colValues); i++ {
			colName, ok := colValues[i].(string)
			if !ok {
				panic("column name is not a string")
			}
			data[colName] = interface{}(valValues[i])
		}

		for _, tableType := range tableTypes {
			colname := tableType.GetName()
			coltype := tableType.GetRepresent()
			colsize := tableType.GetSize()

			if _, ok := data[colname]; !ok {
				panic(fmt.Sprintf("column %s not found in data", colname))
			}
			if _, ok := data[colname]; !ok {
				panic(fmt.Sprintf("column %s not found in data", colname))
			}
			if coltype == "Integer" {
				val, err := strconv.Atoi(data[colname].(string))
				if err != nil {
					panic(fmt.Sprintf("column %s is not an integer: %v", colname, err))
				}
				if val < 0 {
					panic(fmt.Sprintf("column %s exceeds size %d", colname, colsize))
				}
				data[colname] = int(val) // Store as int64 for consistency
			} else if coltype == "Char" {
				if _, ok := data[colname].(string); !ok {
					panic(fmt.Sprintf("column %s is not a string", colname))
				}
				if len(data[colname].(string)) > colsize {
					panic(fmt.Sprintf("column %s exceeds size %d", colname, colsize))
				}
			} else {
				panic(fmt.Sprintf("unsupported data type: %s", coltype))
			}
		}
		if _, ok := data["id"].(int); !ok {
			fmt.Printf("%T", data["id"])
			panic("id is not an int")
		}

		btree.Insert(int64(data["id"].(int)), data)
		return nil, nil

	default:
		panic("unsupported statement type")
	}
}
