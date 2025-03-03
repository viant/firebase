package firestore

import (
	"context"
	"database/sql/driver"
	"fmt"
	"github.com/viant/sqlparser/expr"
	"strings"

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

	// Get the table selector which may contain document path expressions
	selector := sqlparser.TableSelector(insertStmt)
	var collectionRef *firestore.CollectionRef

	if selector != nil && selector.Expression != "" {
		// Handle collection[id=?].subcollection format
		parent, subCollection, err := parseDocumentPath(selector, args)
		if err != nil {
			return nil, err
		}
		collectionRef = s.conn.client.Collection(parent).Doc(subCollection.docID).Collection(subCollection.collName)
	} else {
		// Standard collection reference
		collectionName := sqlparser.TableName(insertStmt)
		collectionRef = s.conn.client.Collection(collectionName)
	}

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
		var customDocID string
		hasCustomDocID := false

		for colIndex := 0; colIndex < columnsCount; colIndex++ {
			columnName := columnNames[colIndex]
			valueExpr := insertStmt.Values[batchIndex*columnsCount+colIndex].Expr
			value, err := eval.evaluateExprWithArgIndex(valueExpr, &argIndex)
			if err != nil {
				return nil, fmt.Errorf("failed to evaluate value for column %s: %v", columnNames[colIndex], err)
			}

			// Check if this is a docid column
			if IsDocIDColumn(columnName) {
				if value != nil {
					customDocID = fmt.Sprintf("%v", value)
					hasCustomDocID = true
				}
				// Don't add docid to the values map
				continue
			}

			data[columnName] = value
		}

		var docRef *firestore.DocumentRef

		// If a custom document ID was provided, use it
		if hasCustomDocID && customDocID != "" {
			docRef = collectionRef.Doc(customDocID)
			_, err = docRef.Set(ctx, data)
			if err != nil {
				return nil, fmt.Errorf("failed to insert values with custom ID: %v", err)
			}
			lastInsertID = customDocID
		} else {
			// Add a new document with auto-generated ID
			docRef, _, err := collectionRef.Add(ctx, data)
			if err != nil {
				return nil, fmt.Errorf("failed to insert values: %v", err)
			}
			lastInsertID = docRef.ID
		}

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

	// Get the table selector which may contain document path expressions
	selector := sqlparser.TableSelector(updateStmt)
	var collectionRef *firestore.CollectionRef

	if selector != nil && selector.Expression != "" {
		// Handle collection[id=?].subcollection format
		parent, subCollection, err := parseDocumentPath(selector, args)
		if err != nil {
			return nil, err
		}
		collectionRef = s.conn.client.Collection(parent).Doc(subCollection.docID).Collection(subCollection.collName)
	} else {
		// Standard collection reference
		collectionName := sqlparser.TableName(updateStmt)
		collectionRef = s.conn.client.Collection(collectionName)
	}

	// Check if we have a document ID in the WHERE clause
	argsInterface := convertNamedValuesToInterfaceSlice(args)
	docID, hasDocID, err := FindDocIDInWhere(updateStmt.Qualify, argsInterface)
	if err != nil {
		return nil, err
	}

	// If we have a document ID, update the document directly
	if hasDocID {
		docRef := collectionRef.Doc(docID)

		// Prepare the updates
		updates := []firestore.Update{}
		eval := &evaluator{args: argsInterface}
		argIndex := 0

		for _, setItem := range updateStmt.Set {
			col := sqlparser.Stringify(setItem.Column)
			// Skip docid in updates
			if IsDocIDColumn(col) {
				continue
			}

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

		_, err = docRef.Update(ctx, updates)
		if err != nil {
			return nil, fmt.Errorf("failed to update document %s: %v", docID, err)
		}

		return &Result{
			rowsAffected: 1,
		}, nil
	}

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
			// Skip docid in updates
			if IsDocIDColumn(col) {
				continue
			}

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

	// Get the table selector which may contain document path expressions
	selector := sqlparser.TableSelector(deleteStmt)
	var collectionRef *firestore.CollectionRef

	if selector != nil && selector.Expression != "" {
		// Handle collection[id=?].subcollection format
		parent, subCollection, err := parseDocumentPath(selector, args)
		if err != nil {
			return nil, err
		}
		collectionRef = s.conn.client.Collection(parent).Doc(subCollection.docID).Collection(subCollection.collName)
	} else {
		// Standard collection reference
		collectionName := sqlparser.TableName(deleteStmt)
		collectionRef = s.conn.client.Collection(collectionName)
	}

	// Check if we have a document ID in the WHERE clause
	argsInterface := convertNamedValuesToInterfaceSlice(args)
	docID, hasDocID, err := FindDocIDInWhere(deleteStmt.Qualify, argsInterface)
	if err != nil {
		return nil, err
	}

	// If we have a document ID, delete the document directly
	if hasDocID {
		docRef := collectionRef.Doc(docID)
		_, err = docRef.Delete(ctx)
		if err != nil {
			return nil, fmt.Errorf("failed to delete document %s: %v", docID, err)
		}

		return &Result{
			rowsAffected: 1,
		}, nil
	}

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

	// Check if the WHERE clause is just docid = value (already handled)
	if binary, ok := qualify.X.(*expr.Binary); ok {
		if binary.Op == "=" {
			if ident, ok := binary.X.(*expr.Ident); ok && IsDocIDColumn(ident.Name) {
				// Skip this clause as it's handled directly by document reference
				return query, nil
			}
		}
	}

	var argIndex = 0
	switch amExpr := qualify.X.(type) {
	case *expr.Binary:
		colName, ok := amExpr.X.(*expr.Ident)
		if !ok {
			return query, fmt.Errorf("invalid column name in WHERE clause")
		}
		// Skip docid in query conditions
		if IsDocIDColumn(colName.Name) {
			return query, nil
		}

		value, err := eval.evaluateExpr(amExpr.Y, &argIndex)
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

// SubCollection represents a subcollection reference
type SubCollection struct {
	docID    string
	collName string
}

// parseDocumentPath parses collection[id=?].subcollection format
func parseDocumentPath(selector *expr.Selector, args []driver.NamedValue) (parentCollection string, subCollection *SubCollection, err error) {
	parentCollection = selector.Name

	// Parse expression to get document ID
	exprParts := strings.Split(selector.Expression, "=")
	if len(exprParts) != 2 {
		return "", nil, fmt.Errorf("invalid document path expression: %s", selector.Expression)
	}

	idField := strings.TrimSpace(exprParts[0])
	if idField != DocIDColumn {
		return "", nil, fmt.Errorf("only 'id' field is supported in document path expression, got: %s", idField)
	}

	// Handle placeholder for document ID
	docIDExpr := strings.TrimSpace(exprParts[1])
	var docID string

	if docIDExpr == "?" {
		// Find the first placeholder value in args
		for i, arg := range args {
			docID = fmt.Sprintf("%v", arg.Value)
			// Remove this argument as it's been used
			if i < len(args)-1 {
				args = append(args[:i], args[i+1:]...)
			} else {
				args = args[:i]
			}
			break
		}
	} else {
		// Direct value (without placeholder)
		docID = strings.Trim(docIDExpr, "'\"")
	}

	if docID == "" {
		return "", nil, fmt.Errorf("document ID not provided or is empty")
	}

	// Get subcollection name
	if selector.X == nil {
		return "", nil, fmt.Errorf("subcollection not specified")
	}

	switch x := selector.X.(type) {
	case *expr.Ident:
		subCollection = &SubCollection{
			docID:    docID,
			collName: x.Name,
		}
	case *expr.Selector:
		// Nested path not supported yet
		return "", nil, fmt.Errorf("nested subcollections not supported")
	default:
		return "", nil, fmt.Errorf("unsupported subcollection format")
	}

	return parentCollection, subCollection, nil
}
