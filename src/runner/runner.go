package runner

import (
	"fmt"
	"strconv"

	"github.com/singhpranshu/btree-db/src/bplustree"
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
	var btree *bplustree.BPlusTree
	for _, b := range r.Btrees {
		if b.GetTable().GetName() == tableName {
			btree = b
			break
		}
	}
	if btree == nil {
		panic("table not found in btrees")
	}

	if !ok {
		panic("table name not found in query")
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
	case parser.InsertStmtIndex:
		panic("insert not implemented yet")
	default:
		panic("unsupported statement type")
	}
}
