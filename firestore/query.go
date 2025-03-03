package firestore

import (
	"cloud.google.com/go/firestore"
	"context"
	"database/sql/driver"
	"fmt"
	"github.com/viant/firebase/shared"
	"github.com/viant/sqlparser"
	"github.com/viant/sqlparser/expr"
	"github.com/viant/sqlparser/node"
	"github.com/viant/sqlparser/query"
)

// Implementation of select operation
func (s *Statement) querySelect(ctx context.Context, args []driver.NamedValue) (driver.Rows, error) {
	selectStmt, err := sqlparser.ParseQuery(s.SQL)
	if err != nil {
		return nil, fmt.Errorf("failed to parse select statement: %v", err)
	}

	// Handle raw expressions if present
	table := sqlparser.Stringify(selectStmt.From.X)
	if rawExpr, ok := selectStmt.From.X.(*expr.Raw); ok {
		if err = shared.RemapInnerQuery(selectStmt, rawExpr, &table); err != nil {
			return nil, err
		}
	}

	// Get collection reference - check if it's a subcollection path
	var collectionRef *firestore.CollectionRef

	// Check for collection[id=?].subcollection format
	selector := sqlparser.TableSelector(selectStmt)
	if selector != nil && selector.Expression != "" {

		if shared.IsDryRun(selectStmt) {
			collectionRef := s.conn.client.Collection(selector.Name)
			docIter := collectionRef.Documents(ctx)
			defer docIter.Stop()
			if doc, _ := docIter.Next(); doc != nil {
				args[0].Value = doc.Ref.ID
			}
		}

		// Handle collection[id=?].subcollection format
		parent, subCollection, err := parseDocumentPath(selector, args)
		if err != nil {
			return nil, err
		}

		collectionRef = s.conn.client.Collection(parent).Doc(subCollection.docID).Collection(subCollection.collName)
	} else {
		// Standard collection reference
		collectionName := sqlparser.TableName(selectStmt)
		collectionRef = s.conn.client.Collection(collectionName)
	}

	// Check if we have a WHERE clause with docid = 'value'
	argsInterface := convertNamedValuesToInterfaceSlice(args)
	docID, hasDocID, err := FindDocIDInWhere(selectStmt.Qualify, argsInterface)
	if err != nil {
		return nil, err
	}

	// If we have a docid filter, fetch the document directly
	if hasDocID {
		docRef := collectionRef.Doc(docID)
		doc, err := docRef.Get(ctx)
		if err != nil {
			return nil, fmt.Errorf("failed to get document by ID: %v", err)
		}

		// Create result from the single document
		var results []map[string]interface{}
		data := doc.Data()

		// Include the document ID as a field
		data[DocIDColumn] = doc.Ref.ID

		results = append(results, data)

		// Build Rows from results
		rows := NewRows(results, false, selectStmt)
		return rows, nil
	}

	// Build the query based on WHERE clause
	queryRef, dryRun, err := buildFirestoreSelectQuery(collectionRef, selectStmt, args)
	if err != nil {
		return nil, err
	}

	// Fetch documents
	docIter := queryRef.Documents(ctx)
	defer docIter.Stop()

	// Collect results
	var results []map[string]interface{}

	for {
		doc, err := docIter.Next()
		if err != nil {
			break
		}

		data := doc.Data()

		// Include the document ID as a field
		data[DocIDColumn] = doc.Ref.ID

		results = append(results, data)
		if dryRun { //just fetch one record
			break
		}
	}

	// Add docid to results if needed
	AddDocIDToResults(selectStmt, results)

	// Build Rows from results
	rows := NewRows(results, dryRun, selectStmt)

	return rows, nil
}

// Helper function to build Firestore query from SELECT statement
func buildFirestoreSelectQuery(collectionRef *firestore.CollectionRef, selectStmt *query.Select, args []driver.NamedValue) (firestore.Query, bool, error) {
	eval := &evaluator{args: convertNamedValuesToInterfaceSlice(args)}
	queryRef := collectionRef.Query

	argIndex := 0
	// Apply WHERE clause
	if selectStmt.Qualify != nil && selectStmt.Qualify.X != nil {
		// Skip processing if the WHERE clause is just docid = value (already handled)
		isDocIDOnly := false

		if binary, ok := selectStmt.Qualify.X.(*expr.Binary); ok {
			if binary.Op == "=" {
				if ident, ok := binary.X.(*expr.Ident); ok && IsDocIDColumn(ident.Name) {
					isDocIDOnly = true
				}
			}
		}

		if !isDocIDOnly {
			switch amExpr := selectStmt.Qualify.X.(type) {
			case *expr.Binary:

				if shared.IsFalsePredicate(amExpr) {
					return queryRef, true, nil
				}

				colName, ok := amExpr.X.(*expr.Ident)
				if !ok {
					return queryRef, false, fmt.Errorf("invalid column name in WHERE clause")
				}

				// Skip docid condition as it's handled separately
				if IsDocIDColumn(colName.Name) {
					break
				}

				value, err := eval.evaluateExpr(amExpr.Y, &argIndex)
				if err != nil {
					return queryRef, false, fmt.Errorf("could not resolve value in WHERE clause: %v", err)
				}
				switch amExpr.Op {
				case "=":
					queryRef = queryRef.Where(colName.Name, "==", value)
				case ">", ">=", "<", "<=":
					queryRef = queryRef.Where(colName.Name, amExpr.Op, value)
				default:
					return queryRef, false, fmt.Errorf("unsupported operator in WHERE clause: %s", amExpr.Op)
				}
			default:
				return queryRef, false, fmt.Errorf("unsupported WHERE clause")
			}
		}
	}

	// Apply LIMIT
	if selectStmt.Limit != nil {
		limitValue, err := parseExpressionValue(selectStmt.Limit)
		if err != nil {
			return queryRef, false, fmt.Errorf("failed to parse LIMIT value: %v", err)
		}
		limitInt, ok := limitValue.(int64)
		if !ok {
			return queryRef, false, fmt.Errorf("LIMIT value is not an integer")
		}
		queryRef = queryRef.Limit(int(limitInt))
	}

	// Apply OFFSET
	if selectStmt.Offset != nil {
		offsetValue, err := parseExpressionValue(selectStmt.Offset)
		if err != nil {
			return queryRef, false, fmt.Errorf("failed to parse OFFSET value: %v", err)
		}
		offsetInt, ok := offsetValue.(int64)
		if !ok {
			return queryRef, false, fmt.Errorf("OFFSET value is not an integer")
		}
		queryRef = queryRef.Offset(int(offsetInt))
	}

	// Apply ORDER BY
	if len(selectStmt.OrderBy) > 0 {
		for _, item := range selectStmt.OrderBy {
			colExpr := item.Expr
			colName, ok := colExpr.(*expr.Ident)
			if !ok {
				return queryRef, false, fmt.Errorf("unsupported ORDER BY expression")
			}
			// Skip docid order by as it's not supported directly
			if IsDocIDColumn(colName.Name) {
				continue
			}
			direction := firestore.Asc
			if item.Direction != "" {
				if item.Direction == "DESC" {
					direction = firestore.Desc
				}
			}
			queryRef = queryRef.OrderBy(colName.Name, direction)
		}
	}

	return queryRef, false, nil
}

// Helper function to parse expressions to values
func parseExpressionValue(exprNode node.Node) (interface{}, error) {
	switch v := exprNode.(type) {
	case *expr.Literal:
		return parseLiteralValue(v)
	default:
		return nil, fmt.Errorf("unsupported expression type: %T", v)
	}
}
