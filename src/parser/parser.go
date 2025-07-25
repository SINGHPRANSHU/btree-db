package parser

import (
	"errors"
	"strings"
)

type Parser struct {
	lexer *Lexer
}

type ParsedData struct {
	StatementType int
	Data          map[int][]interface{}
}

func NewParser() *Parser {
	return &Parser{
		lexer: NewLexer(),
	}
}

func (p *Parser) Parse(statement string) (*ParsedData, error) {
	tokens := p.lexer.tokenize(statement)
	if len(tokens) == 0 {
		panic("no statement passed") // No tokens to parse
	}

	switch tokens[0] {
	case "SELECT":
		return p.ParseSelect(tokens)
	case "INSERT":
		return p.ParseInsert(tokens)
	case "UPDATE":
		return p.ParseUpdate(tokens)
	case "DELETE":
		return p.ParseDelete(tokens)
	case "CREATE":
		return p.parseCreate(tokens)
	case "DROP":
		return p.parseDrop(tokens)
	default:
		panic("invalid statement") // Unsupported statement type

	}
}

func (p *Parser) ParseSelect(tokens []string) (*ParsedData, error) {
	// Implement the parsing logic for SELECT statements
	//SELECT * FROM TableName WHERE ID = 2
	if len(tokens) != 8 || tokens[2] != TokenFrom || tokens[4] != TokenWhere || tokens[6] != TokenEqual {
		panic("incorrect select statement") // No statement to parse
	}
	return &ParsedData{
		StatementType: SelectStmtIndex,
		Data: map[int][]interface{}{
			TableIndex:       {tokens[3]},
			WhereColumnIndex: {tokens[5]},
			WhereValueIndex:  {tokens[7]},
		}}, nil

}
func (p *Parser) ParseInsert(tokens []string) (*ParsedData, error) {
	// Implement the parsing logic for INSERT statements
	//btree.Insert(10, map[string]interface{}{"id": 10, "name": "pranshu"})
	// insert into tests (col1, col2) values (val1, val2)
	if len(tokens) < 6 {
		panic("incorrect insert statement") // No statement to parse
	}
	col, val, err := p.getInsertColAndVal(tokens)
	if err != nil {
		panic("failed to parse insert statement: " + err.Error()) // Invalid insert statement
	}

	columns := make([]interface{}, 0)
	values := make([]interface{}, 0)

	for i := 0; i < len(col); i++ {
		columns = append(columns, col[i])
		values = append(values, val[i])
	}

	return &ParsedData{
		StatementType: InsertStmtIndex,
		Data: map[int][]interface{}{
			TableIndex:     {tokens[2]},
			InsertColIndex: columns,
			InsertValIndex: values,
		}}, nil
}
func (p *Parser) ParseUpdate(tokens []string) (*ParsedData, error) {
	// Implement the parsing logic for UPDATE statements
	panic("update statement not implemented") // No statement to parse
	return nil, nil
}
func (p *Parser) ParseDelete(tokens []string) (*ParsedData, error) {
	// Implement the parsing logic for DELETE statements
	// dELETE FROM TableName WHERE ID = 2
	if len(tokens) != 7 {
		panic("incorrect delete statement") // No statement to parse
	}
	return &ParsedData{
		StatementType: DeleteStmtIndex,
		Data: map[int][]interface{}{
			TableIndex:       {tokens[2]},
			WhereColumnIndex: {tokens[4]},
			WhereValueIndex:  {tokens[6]},
		}}, nil
}

func (p *Parser) parseCreate(tokens []string) (*ParsedData, error) {
	// Implement the parsing logic for CREATE statements
	//CREATE TABLE TableName Column1 DataType1 size Column2 DataType2 size
	if len(tokens) < 6 || len(tokens)%3 != 0 || tokens[1] != TokenTable {
		panic("incorrect create statement") // No statement to parse
	}
	tableName := tokens[2]
	columns := make([]interface{}, 0)
	values := make([]interface{}, 0)
	sizes := make([]interface{}, 0)
	for i := 3; i < len(tokens); i = i + 3 {
		columns = append(columns, tokens[i])
		values = append(values, tokens[i+1])
		sizes = append(sizes, tokens[i+2])
	}
	return &ParsedData{
		StatementType: CreateStmtIndex,
		Data: map[int][]interface{}{
			TableIndex:        {tableName},
			CreateColumnIndex: columns,
			CreateTypeIndex:   values,
			CreateSizeIndex:   sizes,
		}}, nil
}
func (p *Parser) parseDrop(tokens []string) (*ParsedData, error) {
	// Implement the parsing logic for DROP statements
	// DROP TABLE TableName
	panic("drop statement not implemented") // No statement to parse
	return nil, nil
}

func (p *Parser) getInsertColAndVal(tokens []string) ([]string, []string, error) {
	stmt := strings.Join(tokens, " ")

	// insert into tests
	//  (col1, col2) values
	//  (val1, val2)
	splitOpenParenthesis := strings.Split(stmt, "(")

	if len(splitOpenParenthesis) != 3 {
		return nil, nil, errors.New("invalid stmt") // Invalid insert statement
	}

	//  col1, col2) values
	colPart := splitOpenParenthesis[1]

	//  col1, col2
	//  values
	colSplitCloseParenthesis := strings.Split(colPart, ")")
	if len(colSplitCloseParenthesis) != 2 {
		return nil, nil, errors.New("invalid stmt") // Invalid insert statement
	}

	// val1, val2)
	valPart := splitOpenParenthesis[2]

	// val1, val2
	valSplitCloseParenthesis := strings.Split(valPart, ")")
	if len(valSplitCloseParenthesis) != 2 {
		return nil, nil, errors.New("invalid stmt") // Invalid insert statement
	}

	return trimStrArr(strings.Split(colSplitCloseParenthesis[0], ",")), trimStrArr(strings.Split(valSplitCloseParenthesis[0], ",")), nil
}

func trimStrArr(arr []string) []string {
	var result []string
	for _, str := range arr {
		trimmed := strings.TrimSpace(str)
		result = append(result, trimmed)
	}
	return result
}
