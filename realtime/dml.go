package realtime

import (
	"context"
	"database/sql/driver"
	"firebase.google.com/go/db"
	"fmt"
	"github.com/viant/sqlparser"
	"github.com/viant/sqlparser/expr"
	"github.com/viant/sqlparser/node"
)

// Implementation of insert operation
func (s *Statement) execInsert(ctx context.Context, args []driver.NamedValue) (driver.Result, error) {
	// Parse the INSERT statement
	insertStmt, err := sqlparser.ParseInsert(s.SQL)
	if err != nil {
		return nil, fmt.Errorf("failed to parse insert statement: %v", err)
	}

	table := sqlparser.TableName(insertStmt)
	ref := s.conn.client.NewRef(table)

	// Prepare the evaluator with the provided arguments
	placeholderValues := convertNamedValuesToInterfaceSlice(args)
	eval := &evaluator{args: placeholderValues}
	argIndex := 0

	rowsAffected := int64(0)
	var lastInsertID string

	columnNames := insertStmt.Columns
	columnsCount := len(columnNames)
	valuesCount := len(insertStmt.Values)
	if valuesCount%columnsCount != 0 {
		return nil, fmt.Errorf("number of values is not a multiple of the number of columns")
	}
	batchSize := valuesCount / columnsCount

	for batchIndex := 0; batchIndex < batchSize; batchIndex++ {
		data := make(map[string]interface{})

		for colIndex := 0; colIndex < columnsCount; colIndex++ {
			valueExpr := insertStmt.Values[batchIndex*columnsCount+colIndex].Expr
			value, err := eval.evaluateExprWithArgIndex(valueExpr, &argIndex)
			if err != nil {
				return nil, fmt.Errorf("failed to evaluate value for column %s: %v", columnNames[colIndex], err)
			}
			data[columnNames[colIndex]] = value
		}

		// Use Push() to create a new unique key under the table
		newRef, err := ref.Push(ctx, data)
		if err != nil {
			return nil, fmt.Errorf("failed to insert data: %v", err)
		}
		lastInsertID = newRef.Key
		rowsAffected++
	}

	return &Result{
		rowsAffected: rowsAffected,
		insertID:     lastInsertID,
	}, nil
}

// Implementation of update operation
func (s *Statement) execUpdate(ctx context.Context, args []driver.NamedValue) (driver.Result, error) {
	// Parse the UPDATE statement
	updateStmt, err := sqlparser.ParseUpdate(s.SQL)
	if err != nil {
		return nil, fmt.Errorf("failed to parse update statement: %v", err)
	}

	table := sqlparser.TableName(updateStmt)

	// Build the reference with WHERE clause
	ref := s.conn.client.NewRef(table)
	var queryRef *db.Query
	if updateStmt.Qualify != nil && updateStmt.Qualify.X != nil {
		queryRef, err = applyDmlWhereClause(ref, updateStmt.Qualify.X, args)
		if err != nil {
			return nil, err
		}
	}

	// Fetch data to update (Firebase Realtime Database doesn't support direct updates with queries)
	var results map[string]interface{}
	if queryRef != nil {
		if err := queryRef.Get(ctx, &results); err != nil {
			return nil, fmt.Errorf("failed to get data for update: %v", err)
		}
	} else {
		if err := ref.Get(ctx, &results); err != nil {
			return nil, fmt.Errorf("failed to get data for update: %v", err)
		}
	}

	if len(results) == 0 {
		return &Result{
			rowsAffected: 0,
		}, nil
	}

	// Prepare the data to update
	eval := &evaluator{args: convertNamedValuesToInterfaceSlice(args)}
	argIndex := 0

	rowsAffected := int64(0)

	// Update each matching record
	for key, record := range results {
		updatedRecord, ok := record.(map[string]interface{})
		if !ok {
			continue
		}

		for _, setItem := range updateStmt.Set {
			col := sqlparser.Stringify(setItem.Column)
			valueExpr := setItem.Expr
			value, err := eval.evaluateExprWithArgIndex(valueExpr, &argIndex)
			if err != nil {
				return nil, fmt.Errorf("failed to evaluate value for column %s: %v", col, err)
			}
			updatedRecord[col] = value
		}

		err := ref.Child(key).Set(ctx, updatedRecord)
		if err != nil {
			return nil, fmt.Errorf("failed to update data for key %s: %v", key, err)
		}
		rowsAffected++
	}

	return &Result{
		rowsAffected: rowsAffected,
	}, nil
}

// Implementation of delete operation
func (s *Statement) execDelete(ctx context.Context, args []driver.NamedValue) (driver.Result, error) {
	deleteStmt, err := sqlparser.ParseDelete(s.SQL)
	if err != nil {
		return nil, fmt.Errorf("failed to parse delete statement: %v", err)
	}

	table := sqlparser.TableName(deleteStmt)
	ref := s.conn.client.NewRef(table)
	var queryRef *db.Query
	if deleteStmt.Qualify != nil && deleteStmt.Qualify.X != nil {
		queryRef, err = applyDmlWhereClause(ref, deleteStmt.Qualify.X, args)
		if err != nil {
			return nil, err
		}
	}

	// Fetch data to delete
	var results map[string]interface{}
	if queryRef != nil {
		if err := queryRef.Get(ctx, &results); err != nil {
			return nil, fmt.Errorf("failed to get data for delete: %v", err)
		}
	} else {
		if err := ref.Get(ctx, &results); err != nil {
			return nil, fmt.Errorf("failed to get data for delete: %v", err)
		}
	}

	if len(results) == 0 {
		return &Result{
			rowsAffected: 0,
		}, nil
	}

	// Delete each matching record
	rowsAffected := int64(0)
	for key := range results {
		err := ref.Child(key).Delete(ctx)
		if err != nil {
			return nil, fmt.Errorf("failed to delete data for key %s: %v", key, err)
		}
		rowsAffected++
	}

	return &Result{
		rowsAffected: rowsAffected,
	}, nil
}

// Helper functions to apply WHERE clause
func applyDmlWhereClause(ref *db.Ref, where node.Node, args []driver.NamedValue) (*db.Query, error) {
	eval := &evaluator{args: convertNamedValuesToInterfaceSlice(args)}
	var queryRef *db.Query
	// Implement logic to transform SQL WHERE clause into Firebase queries
	// For simplicity, only basic equality conditions are handled here
	switch amExpr := where.(type) {
	case *expr.Binary:
		if amExpr.Op != "=" {
			return nil, fmt.Errorf("only equality conditions are supported in WHERE clause")
		}
		colName, ok := amExpr.X.(*expr.Ident)
		if !ok {
			return nil, fmt.Errorf("invalid column name in WHERE clause")
		}
		value, err := eval.evaluateExpr(amExpr.Y)
		if err != nil {
			return nil, fmt.Errorf("could not resolve value in WHERE clause: %v", err)
		}
		queryRef = ref.OrderByChild(colName.Name).EqualTo(value)
	default:
		return nil, fmt.Errorf("unsupported WHERE clause")
	}
	return queryRef, nil
}
