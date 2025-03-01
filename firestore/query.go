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
	table := sqlparser.Stringify(selectStmt.From.X)
	if rawExpr, ok := selectStmt.From.X.(*expr.Raw); ok {
		if err = shared.RemapInnerQuery(selectStmt, rawExpr, &table); err != nil {
			return nil, err
		}
	}

	collectionName := sqlparser.TableName(selectStmt)
	collectionRef := s.conn.client.Collection(collectionName)

	// Build the query based on WHERE clause
	queryRef, err := buildFirestoreSelectQuery(collectionRef, selectStmt, args)
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

		// Include the document ID as a field if needed
		if _, exists := data["id"]; !exists {
			data["id"] = doc.Ref.ID
		}

		results = append(results, data)
	}

	// Build Rows from results
	rows := NewRows(results, selectStmt)

	return rows, nil
}

// Helper function to build Firestore query from SELECT statement
func buildFirestoreSelectQuery(collectionRef *firestore.CollectionRef, selectStmt *query.Select, args []driver.NamedValue) (firestore.Query, error) {
	eval := &evaluator{args: convertNamedValuesToInterfaceSlice(args)}
	queryRef := collectionRef.Query

	// Apply WHERE clause
	if selectStmt.Qualify != nil && selectStmt.Qualify.X != nil {
		switch amExpr := selectStmt.Qualify.X.(type) {
		case *expr.Binary:
			colName, ok := amExpr.X.(*expr.Ident)
			if !ok {
				return queryRef, fmt.Errorf("invalid column name in WHERE clause")
			}
			value, err := eval.evaluateExpr(amExpr.Y)
			if err != nil {
				return queryRef, fmt.Errorf("could not resolve value in WHERE clause: %v", err)
			}
			switch amExpr.Op {
			case "=":
				queryRef = queryRef.Where(colName.Name, "==", value)
			case ">", ">=", "<", "<=":
				queryRef = queryRef.Where(colName.Name, amExpr.Op, value)
			default:
				return queryRef, fmt.Errorf("unsupported operator in WHERE clause: %s", amExpr.Op)
			}
		default:
			return queryRef, fmt.Errorf("unsupported WHERE clause")
		}
	}

	// Apply LIMIT
	if selectStmt.Limit != nil {
		limitValue, err := parseExpressionValue(selectStmt.Limit)
		if err != nil {
			return queryRef, fmt.Errorf("failed to parse LIMIT value: %v", err)
		}
		limitInt, ok := limitValue.(int64)
		if !ok {
			return queryRef, fmt.Errorf("LIMIT value is not an integer")
		}
		queryRef = queryRef.Limit(int(limitInt))
	}

	// Apply OFFSET
	if selectStmt.Offset != nil {
		offsetValue, err := parseExpressionValue(selectStmt.Offset)
		if err != nil {
			return queryRef, fmt.Errorf("failed to parse OFFSET value: %v", err)
		}
		offsetInt, ok := offsetValue.(int64)
		if !ok {
			return queryRef, fmt.Errorf("OFFSET value is not an integer")
		}
		queryRef = queryRef.Offset(int(offsetInt))
	}

	// Apply ORDER BY
	if len(selectStmt.OrderBy) > 0 {
		for _, item := range selectStmt.OrderBy {
			colExpr := item.Expr
			colName, ok := colExpr.(*expr.Ident)
			if !ok {
				return queryRef, fmt.Errorf("unsupported ORDER BY expression")
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

	return queryRef, nil
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
