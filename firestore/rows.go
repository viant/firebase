package firestore

import (
	"database/sql/driver"
	"fmt"
	"github.com/viant/sqlparser/expr"
	"github.com/viant/sqlparser/query"
	"io"
	"reflect"
)

// Rows represents a result set for a SQL query
type Rows struct {
	dryRun      bool
	columns     []string
	columnTypes []string
	values      [][]interface{}
	currentRow  int
}

// Columns returns the names of the columns
func (r *Rows) Columns() []string {
	return r.columns
}

// Close closes the rows iterator
func (r *Rows) Close() error {
	return nil
}

// Next moves the cursor to the next row
func (r *Rows) Next(dest []driver.Value) error {
	if r.currentRow >= len(r.values) || r.dryRun {
		return io.EOF
	}

	// Copy the current row's values to dest
	for i, val := range r.values[r.currentRow] {
		dest[i] = val
	}
	r.currentRow++
	return nil
}

// ColumnTypeScanType returns the ScanType of the column at the given index
func (r *Rows) ColumnTypeScanType(index int) reflect.Type {
	if len(r.values) == 0 || index >= len(r.values[0]) {
		return nil
	}
	return reflect.TypeOf(r.values[0][index])
}

// ColumnTypeDatabaseTypeName returns the database type name of the column
func (r *Rows) ColumnTypeDatabaseTypeName(index int) string {
	rType := r.ColumnTypeScanType(index)
	if rType != nil {
		ret := rType.Name()
		if ret != "" {
			return ret
		}
		if rType == reflect.TypeOf([]byte{}) {
			return "BLOB"
		}
	}
	// Return a generic type as Firebase is schemaless
	return "TEXT"
}

// ColumnTypeNullable reports whether the column may be null
// ColumnTypeNullable reports whether the column may be null
func (r *Rows) ColumnTypeNullable(index int) (nullable, ok bool) {
	if index >= len(r.columns) {
		if r.columns[index] == DocIDColumn {
			return false, true
		}
	}
	return true, true
}

// NewRows creates a new result set from map values
func NewRows(results []map[string]interface{}, dryRun bool, selectStmt *query.Select) *Rows {
	rows := &Rows{
		dryRun:     dryRun,
		currentRow: 0,
	}

	if len(results) == 0 {
		rows.columns = []string{}
		rows.values = [][]interface{}{}
		return rows
	}

	// Determine columns from SELECT statement
	rows.columns = extractColumns(selectStmt, results[0])
	rows.columnTypes = make([]string, len(rows.columns))

	// Transform map values to row values
	rows.values = make([][]interface{}, len(results))
	for i, result := range results {
		row := make([]interface{}, len(rows.columns))
		for j, col := range rows.columns {
			row[j] = result[col]

			// For the first row, determine column types
			if i == 0 && result[col] != nil {
				rows.columnTypes[j] = reflect.TypeOf(result[col]).String()
			}
		}
		rows.values[i] = row
	}

	return rows
}

// extractColumns determines the columns to include in the result set
func extractColumns(selectStmt *query.Select, firstRow map[string]interface{}) []string {
	// Check if SELECT * is used
	if selectStmt.List.IsStarExpr() {
		// Include all columns in the result
		columns := make([]string, 0, len(firstRow))
		for col := range firstRow {
			columns = append(columns, col)
		}
		return columns
	}

	// Use specified columns from SELECT clause
	columns := make([]string, len(selectStmt.List))
	for i, item := range selectStmt.List {
		switch anExpr := item.Expr.(type) {
		case *expr.Ident:
			columns[i] = anExpr.Name
		case *expr.Selector:
			columns[i] = anExpr.Name
			if anExpr.X != nil {
				if ident, ok := anExpr.X.(*expr.Ident); ok {
					columns[i] = fmt.Sprintf("%s.%s", ident.Name, anExpr.Name)
				}
			}
		default:
			columns[i] = fmt.Sprintf("col%d", i)
		}
	}

	return columns
}
