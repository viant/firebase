package realtime

import (
	"context"
	"database/sql/driver"
	"firebase.google.com/go/db"
	"fmt"
	"github.com/viant/firebase/shared"
	"github.com/viant/sqlparser"
	"github.com/viant/sqlparser/expr"
	"github.com/viant/sqlparser/node"
	"github.com/viant/sqlparser/query"
)

// Implementation of select operation with pagination support
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
	ref := s.conn.client.NewRef(table)
	// Apply WHERE clause
	queryRef, err := applyWhereClause(ref, selectStmt.Qualify, args)
	if err != nil {
		return nil, err
	}

	// Apply LIMIT and OFFSET for pagination
	queryRef, err = applyLimitOffset(queryRef, selectStmt)
	if err != nil {
		return nil, err
	}

	// Fetch data
	var result interface{}
	if err := queryRef.Get(ctx, &result); err != nil {
		return nil, fmt.Errorf("failed to get data: %v", err)
	}

	// Convert result to rows
	rows := NewRows(result, selectStmt)

	return rows, nil
}
func applyWhereClause(ref *db.Ref, qualify *expr.Qualify, args []driver.NamedValue) (*db.Query, error) {
	if qualify == nil || qualify.X == nil {
		// No WHERE clause; order by key to support limit
		return ref.OrderByKey(), nil
	}

	eval := &evaluator{args: convertNamedValuesToInterfaceSlice(args)}
	var queryRef *db.Query

	// Handle basic equality and range conditions
	switch amExpr := qualify.X.(type) {
	case *expr.Binary:
		colName, ok := amExpr.X.(*expr.Ident)
		if !ok {
			return nil, fmt.Errorf("invalid column name in WHERE clause")
		}
		value, err := eval.evaluateExpr(amExpr.Y)
		if err != nil {
			return nil, fmt.Errorf("could not resolve value in WHERE clause: %v", err)
		}
		switch amExpr.Op {
		case "=":
			queryRef = ref.OrderByChild(colName.Name).EqualTo(value)
		case ">", ">=":
			queryRef = ref.OrderByChild(colName.Name).StartAt(value)
		case "<", "<=":
			queryRef = ref.OrderByChild(colName.Name).EndAt(value)
		default:
			return nil, fmt.Errorf("unsupported operator in WHERE clause: %s", amExpr.Op)
		}
	default:
		return nil, fmt.Errorf("unsupported WHERE clause")
	}
	return queryRef, nil
}

// Helper function to apply LIMIT and OFFSET for pagination
func applyLimitOffset(queryRef *db.Query, selectStmt *query.Select) (*db.Query, error) {
	if selectStmt.Limit != nil {
		limitValue, err := parseExpressionValue(selectStmt.Limit)
		if err != nil {
			return nil, fmt.Errorf("failed to parse LIMIT value: %v", err)
		}
		limitInt, ok := limitValue.(int)
		if !ok {
			return nil, fmt.Errorf("LIMIT value is not an integer")
		}
		// Apply limit
		queryRef = queryRef.LimitToFirst(limitInt)
	}
	if selectStmt.Offset != nil {
		// OFFSET is not directly supported in Firebase Realtime Database
		// To simulate OFFSET, you would need to use startAfter with a known key or value
		// This requires modifying your data model to include a sequential key or timestamp
		return nil, fmt.Errorf("OFFSET is not supported in Firebase Realtime Database queries")
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
