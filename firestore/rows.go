package firestore

import (
	"database/sql/driver"
	"fmt"
	"github.com/viant/sqlparser"
	"github.com/viant/sqlparser/query"
	"reflect"
)

type Rows struct {
	columns []string
	values  [][]interface{}
	index   int
}

// NewRows creates a new Rows instance from Firebase data
func NewRows(results []map[string]interface{}, selectStmt *query.Select) *Rows {
	columns := make([]string, 0)

	isStar := selectStmt.List.IsStarExpr()
	if len(selectStmt.List) > 0 && !isStar {
		for _, item := range selectStmt.List {
			colName := sqlparser.Stringify(item.Expr)
			columns = append(columns, colName)
		}
	} else {
		// No specific columns requested or SELECT *
		if len(results) > 0 {
			for col := range results[0] {
				columns = append(columns, col)
			}
		}
	}

	values := make([][]interface{}, len(results))
	for i, record := range results {
		row := make([]interface{}, len(columns))
		for j, colName := range columns {
			row[j] = record[colName]
		}
		values[i] = row
	}

	return &Rows{
		columns: columns,
		values:  values,
	}
}

// Columns returns the names of the columns
func (r *Rows) Columns() []string {
	return r.columns
}

// Close closes the Rows, preventing further enumeration
func (r *Rows) Close() error {
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
		return rType.Name()
	}
	// Return a generic type as Firebase is schemaless
	return "TEXT"
}

func (r *Rows) Next(dest []driver.Value) error {
	if r.index >= len(r.values) {
		return fmt.Errorf("no more rows")
	}
	for i := range dest {
		dest[i] = r.values[r.index][i]
	}
	r.index++
	return nil
}
