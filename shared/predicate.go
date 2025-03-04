package shared

import (
	"github.com/viant/sqlparser"
	"github.com/viant/sqlparser/expr"
	"github.com/viant/sqlparser/query"
	"strings"
)

func IsFalsePredicate(binary *expr.Binary) bool {
	if binary.Op == "=" {
		if leftLiteral, ok := binary.X.(*expr.Literal); ok {
			if rightLiteral, ok := binary.Y.(*expr.Literal); ok {
				return !(leftLiteral.Value == rightLiteral.Value)
			}
		}
	}
	where := sqlparser.Stringify(binary)
	return strings.Contains(where, "1=0") || strings.Contains(where, "1 = 0")
}

func IsDryRun(selectStmt *query.Select) bool {
	if selectStmt.Qualify != nil && selectStmt.Qualify.X != nil {
		if binary, ok := selectStmt.Qualify.X.(*expr.Binary); ok {
			return IsFalsePredicate(binary)
		}
	}
	return false
}
