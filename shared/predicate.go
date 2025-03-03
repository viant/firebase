package shared

import (
	"github.com/viant/sqlparser/expr"
	"github.com/viant/sqlparser/query"
)

func IsFalsePredicate(binary *expr.Binary) bool {
	if binary.Op == "=" {
		if leftLiteral, ok := binary.X.(*expr.Literal); ok {
			if rightLiteral, ok := binary.Y.(*expr.Literal); ok {
				return !(leftLiteral.Value == rightLiteral.Value)
			}
		}
	}
	return false
}

func IsDryRun(selectStmt *query.Select) bool {
	if selectStmt.Qualify != nil && selectStmt.Qualify.X != nil {
		if binary, ok := selectStmt.Qualify.X.(*expr.Binary); ok {
			return IsFalsePredicate(binary)
		}

	}
	return false
}
