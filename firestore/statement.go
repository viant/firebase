package firestore

import (
	"context"
	"database/sql/driver"
	"errors"
	"fmt"
	"github.com/viant/firebase/shared"
	"github.com/viant/sqlparser"
)

type Statement struct {
	SQL      string
	numInput int
	kind     sqlparser.Kind
	conn     *connection
	ctx      context.Context
}

// checkQueryParameters counts the number of parameters in the query
func (s *Statement) checkQueryParameters() {
	// Basic parameter detection; improve as needed
	s.numInput = checkQueryParameters(s.SQL)
}

// NumInput returns the number of placeholder parameters
func (s *Statement) NumInput() int {
	return s.numInput
}

// Close closes the statement
func (s *Statement) Close() error {
	// No resources to release in this implementation
	return nil
}

// Exec executes a non-query statement
func (s *Statement) Exec(args []driver.Value) (driver.Result, error) {
	return nil, errors.New("Exec with driver.Value not supported; use ExecContext")
}

// ExecContext executes a non-query statement with context
func (s *Statement) ExecContext(ctx context.Context, args []driver.NamedValue) (driver.Result, error) {
	switch s.kind {
	case sqlparser.KindInsert:
		return s.execInsert(ctx, args)
	case sqlparser.KindUpdate:
		return s.execUpdate(ctx, args)
	case sqlparser.KindDelete:
		return s.execDelete(ctx, args)
	case sqlparser.KindCreateTable:
		return s.execCreateTable(ctx, args)
	case sqlparser.KindDropTable:
		return s.execDropTable(ctx, args)
	// Add other DDL methods as needed
	default:
		return nil, fmt.Errorf("unsupported exec statement: %s", s.SQL)
	}
}

// Query runs query
func (s *Statement) Query(args []driver.Value) (driver.Rows, error) {
	params := shared.Values(args).Named()
	return s.QueryContext(context.Background(), params)
}

// QueryContext executes a query statement with context
func (s *Statement) QueryContext(ctx context.Context, args []driver.NamedValue) (driver.Rows, error) {
	switch s.kind {
	case sqlparser.KindSelect:
		return s.querySelect(ctx, args)
	default:
		return nil, fmt.Errorf("unsupported query statement: %s", s.SQL)
	}
}

// checkQueryParameters counts the number of parameters in the SQL query
func checkQueryParameters(query string) int {
	count := 0
	inQuote := false
	for i, c := range query {
		switch c {
		case '\'':
			if i > 1 && inQuote && query[i-1] == '\\' {
				continue
			}
			inQuote = !inQuote
		case '?', '$':
			if !inQuote {
				count++
			}
		}
	}
	return count
}

// Helper function to convert []driver.NamedValue to []interface{}
func convertNamedValuesToInterfaceSlice(args []driver.NamedValue) []interface{} {
	result := make([]interface{}, len(args))
	for i, arg := range args {
		result[i] = arg.Value
	}
	return result
}
