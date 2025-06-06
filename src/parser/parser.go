package parser

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
	panic("insert statement not implemented") // No statement to parse
	return nil, nil
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
