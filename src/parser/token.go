package parser

import "strings"

const (
	TableIndex = iota
	WhereColumnIndex
	WhereValueIndex
	CreateColumnIndex
	CreateTypeIndex
	CreateSizeIndex
	DropTableIndex
	InsertColIndex
	InsertValIndex
)

const (
	SelectStmtIndex = iota
	InsertStmtIndex
	UpdateStmtIndex
	DeleteStmtIndex
	CreateStmtIndex
	DropStmtIndex
)

const (
	TokenSelect = "SELECT"
	TokenInsert = "INSERT"
	TokenUpdate = "UPDATE"
	TokenDelete = "DELETE"
	TokenFrom   = "FROM"
	TokenWhere  = "WHERE"
	TokenCreate = "CREATE"
	TokenDrop   = "DROP"
	TokenEqual  = "="
	TokenValues = "VALUES"
	TokenTable  = "TABLE"
	TokenInto   = "INTO"
)

type Lexer struct {
	// Add fields for the lexer, such as input source, current position, etc.
}

func NewLexer() *Lexer {
	return &Lexer{}
}

func (l *Lexer) tokenize(statement string) []string {
	// Implement the tokenization logic here
	// For now, return an empty slice
	stmtArr := strings.Split(statement, " ")
	filStmtArr := []string{}

	for _, token := range stmtArr {
		if token != "" {
			filStmtArr = append(filStmtArr, token)
		}
	}
	return filStmtArr
}

type Select struct {
	Columns []string
	Table   string
	Where   string
}
type Insert struct {
	Table  string
	Values []string
}
type Update struct {
	Table  string
	Values map[string]string // column name to value
	Where  string
}
type Delete struct {
	Table string
	Where string
}
