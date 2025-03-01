package realtime

import (
	"database/sql/driver"
	"github.com/viant/sqlparser"
	"github.com/viant/sqlparser/query"
	"io"
	"reflect"
)

// Rows implements the driver.Rows interface
type Rows struct {
	columns []string
	values  [][]interface{}
	index   int
}

// NewRows creates a new Rows instance from Firebase data
func NewRows(data interface{}, selectStmt *query.Select) *Rows {

	rows := &Rows{
		index: -1,
	}

	var columnsSet bool

	switch records := data.(type) {
	case map[string]interface{}:
		for key, record := range records {
			rowValues, columns := extractRowValuesWithKey(record, key, selectStmt)
			if !columnsSet {
				rows.columns = columns
				columnsSet = true
			}
			rows.values = append(rows.values, rowValues)
		}
	default:
		// Handle other data types as needed
	}

	return rows
}

// Columns returns the names of the columns
func (r *Rows) Columns() []string {
	return r.columns
}

// Close closes the Rows, preventing further enumeration
func (r *Rows) Close() error {
	// No resources to release in this implementation
	return nil
}

// Next prepares the next result row for reading
func (r *Rows) Next(dest []driver.Value) error {
	r.index++
	if r.index >= len(r.values) {
		return io.EOF
	}
	row := r.values[r.index]
	for i, val := range row {
		dest[i] = val
	}
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

// ColumnTypeNullable reports whether the column may be null
func (r *Rows) ColumnTypeNullable(index int) (nullable, ok bool) {
	return true, true
}

// Helper function to extract row values from a record with its key
func extractRowValuesWithKey(record interface{}, key string, selectStmt *query.Select) ([]interface{}, []string) {
	rowValues := []interface{}{}
	columns := []string{}
	recMap, ok := record.(map[string]interface{})
	if !ok {
		// So record is primitive, e.g., float64, string, etc.
		// In this case, we can consider the key as the column name and record as the value
		if len(selectStmt.List) == 0 || selectStmt.List.IsStarExpr() {
			rowValues = append(rowValues, record)
			columns = append(columns, key)
		} else {
			for _, item := range selectStmt.List {
				colName := sqlparser.Stringify(item.Expr)
				if colName == key {
					rowValues = append(rowValues, record)
					columns = append(columns, colName)
				}
				// else, no matching column
			}
		}
	} else {
		// handle recMap
		if len(selectStmt.List) == 0 || selectStmt.List.IsStarExpr() {
			for k, v := range recMap {
				rowValues = append(rowValues, v)
				columns = append(columns, k)
			}
		} else {
			for _, item := range selectStmt.List {
				colName := sqlparser.Stringify(item.Expr)
				val := recMap[colName]
				rowValues = append(rowValues, val)
				columns = append(columns, colName)
			}
		}
	}
	return rowValues, columns
}
