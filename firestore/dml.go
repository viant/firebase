package firestore

import (
	"context"
	"database/sql/driver"
	"fmt"
	"github.com/viant/sqlparser/expr"

	"cloud.google.com/go/firestore"
	"github.com/viant/sqlparser"
)

// Implementation of insert operation
func (s *Statement) execInsert(ctx context.Context, args []driver.NamedValue) (driver.Result, error) {
	// Parse the INSERT statement
	insertStmt, err := sqlparser.ParseInsert(s.SQL)
	if err != nil {
		return nil, fmt.Errorf("failed to parse insert statement: %v", err)
	}

	collectionName := sqlparser.TableName(insertStmt)
	collectionRef := s.conn.client.Collection(collectionName)

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

		// Add a new document with auto-generated ID
		docRef, _, err := collectionRef.Add(ctx, data)
		if err != nil {
			return nil, fmt.Errorf("failed to insert data: %v", err)
		}
		lastInsertID = docRef.ID
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

	collectionName := sqlparser.TableName(updateStmt)
	collectionRef := s.conn.client.Collection(collectionName)

	// Build the query based on WHERE clause
	queryRef, err := buildFirestoreQuery(collectionRef, updateStmt.Qualify, args)
	if err != nil {
		return nil, err
	}

	// Fetch documents to update
	docIter := queryRef.Documents(ctx)
	defer docIter.Stop()

	eval := &evaluator{args: convertNamedValuesToInterfaceSlice(args)}
	argIndex := 0

	rowsAffected := int64(0)

	// Update each matching document
	for {
		doc, err := docIter.Next()
		if err != nil {
			break
		}

		updates := []firestore.Update{}

		for _, setItem := range updateStmt.Set {
			col := sqlparser.Stringify(setItem.Column)
			valueExpr := setItem.Expr
			value, err := eval.evaluateExprWithArgIndex(valueExpr, &argIndex)
			if err != nil {
				return nil, fmt.Errorf("failed to evaluate value for column %s: %v", col, err)
			}
			updates = append(updates, firestore.Update{
				Path:  col,
				Value: value,
			})
		}

		_, err = doc.Ref.Update(ctx, updates)
		if err != nil {
			return nil, fmt.Errorf("failed to update document %s: %v", doc.Ref.ID, err)
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

	collectionName := sqlparser.TableName(deleteStmt)
	collectionRef := s.conn.client.Collection(collectionName)

	// Build the query based on WHERE clause
	queryRef, err := buildFirestoreQuery(collectionRef, deleteStmt.Qualify, args)
	if err != nil {
		return nil, err
	}

	// Fetch documents to delete
	docIter := queryRef.Documents(ctx)
	defer docIter.Stop()

	rowsAffected := int64(0)

	// Delete each matching document
	for {
		doc, err := docIter.Next()
		if err != nil {
			break
		}

		_, err = doc.Ref.Delete(ctx)
		if err != nil {
			return nil, fmt.Errorf("failed to delete document %s: %v", doc.Ref.ID, err)
		}
		rowsAffected++
	}

	return &Result{
		rowsAffected: rowsAffected,
	}, nil
}

// Helper function to build Firestore query from WHERE clause
func buildFirestoreQuery(collectionRef *firestore.CollectionRef, qualify *expr.Qualify, args []driver.NamedValue) (query firestore.Query, err error) {
	if qualify == nil || qualify.X == nil {
		// No WHERE clause; return all documents
		return collectionRef.Query, nil
	}

	eval := &evaluator{args: convertNamedValuesToInterfaceSlice(args)}
	query = collectionRef.Query

	switch amExpr := qualify.X.(type) {
	case *expr.Binary:
		colName, ok := amExpr.X.(*expr.Ident)
		if !ok {
			return query, fmt.Errorf("invalid column name in WHERE clause")
		}
		value, err := eval.evaluateExpr(amExpr.Y)
		if err != nil {
			return query, fmt.Errorf("could not resolve value in WHERE clause: %v", err)
		}
		switch amExpr.Op {
		case "=":
			query = query.Where(colName.Name, "==", value)
		case ">", ">=", "<", "<=":
			query = query.Where(colName.Name, amExpr.Op, value)
		default:
			return query, fmt.Errorf("unsupported operator in WHERE clause: %s", amExpr.Op)
		}
	default:
		return query, fmt.Errorf("unsupported WHERE clause")
	}

	return query, nil
}
