package firestore

import (
	"github.com/viant/sqlparser/expr"
	"github.com/viant/sqlparser/insert"
	"github.com/viant/sqlparser/query"
)

const DocIDColumn = "id"

// IsDocIDColumn checks if a column name is the document ID column
func IsDocIDColumn(columnName string) bool {
	return columnName == DocIDColumn
}

// HasDocIDInColumns checks if the document ID column is present in the column list
func HasDocIDInColumns(columns []string) bool {
	for _, column := range columns {
		if IsDocIDColumn(column) {
			return true
		}
	}
	return false
}

// FindDocIDValueInInsert finds the document ID value in the INSERT statement values
func FindDocIDValueInInsert(insertStmt *insert.Statement, args []interface{}) (string, bool, error) {
	columnNames := insertStmt.Columns
	docIDIndex := -1

	for i, col := range columnNames {
		if IsDocIDColumn(col) {
			docIDIndex = i
			break
		}
	}

	if docIDIndex == -1 {
		return "", false, nil
	}

	// Find docid value in the VALUES clause
	columnsCount := len(columnNames)
	valuesCount := len(insertStmt.Values)
	batchSize := valuesCount / columnsCount

	if batchSize > 1 {
		return "", false, nil // Multiple document inserts with docid not supported
	}

	valueExpr := insertStmt.Values[docIDIndex].Expr
	eval := &evaluator{args: args}
	argIndex := 0

	value, err := eval.evaluateExprWithArgIndex(valueExpr, &argIndex)
	if err != nil {
		return "", false, err
	}

	if value == nil {
		return "", false, nil
	}

	return value.(string), true, nil
}

// FindDocIDInWhere extracts document ID from WHERE docid = 'value' clause
func FindDocIDInWhere(qualify *expr.Qualify, args []interface{}) (string, bool, error) {
	if qualify == nil || qualify.X == nil {
		return "", false, nil
	}

	// Process the WHERE clause to find docid = value condition
	var docID string
	var found bool
	var err error

	argIndex := 0
	// Process binary expression
	switch whereExpr := qualify.X.(type) {
	case *expr.Binary:
		if whereExpr.Op != "=" {
			return "", false, nil
		}

		// Check if left side is docid column
		colName, ok := whereExpr.X.(*expr.Ident)
		if !ok {
			return "", false, nil
		}

		if !IsDocIDColumn(colName.Name) {
			return "", false, nil
		}

		// Extract the document ID value from the right side
		eval := &evaluator{args: args}
		value, err := eval.evaluateExpr(whereExpr.Y, &argIndex)
		if err != nil {
			return "", false, err
		}

		if value != nil {
			docID = value.(string)
			found = true
		}
	}

	return docID, found, err
}

// AddDocIDToResults adds document ID to query results
func AddDocIDToResults(selectStmt *query.Select, results []map[string]interface{}) {
	// Check if docid is requested in the SELECT clause
	needDocID := false
	for _, item := range selectStmt.List {
		if ident, ok := item.Expr.(*expr.Ident); ok && IsDocIDColumn(ident.Name) {
			needDocID = true
			break
		}
	}

	// Handle special case: SELECT * also includes docid
	if !needDocID && selectStmt.List.IsStarExpr() {
		needDocID = true
	}

	if !needDocID {
		return
	}

	// Add docid to all results
	for _, result := range results {
		// Skip if docid already exists (should not happen)
		if _, exists := result[DocIDColumn]; exists {
			continue
		}

		// Add document ID from the "id" field if it exists
		if id, exists := result["id"]; exists {
			result[DocIDColumn] = id
		}
	}
}
