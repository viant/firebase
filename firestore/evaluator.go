package firestore

import (
	"fmt"
	"strconv"

	"github.com/viant/sqlparser/expr"
	"github.com/viant/sqlparser/node"
)

type evaluator struct {
	args []interface{}
}

func (e *evaluator) evaluateExpr(n node.Node) (interface{}, error) {
	return e.evaluateExprWithArgIndex(n, nil)
}

func (e *evaluator) evaluateExprWithArgIndex(n node.Node, argIndex *int) (interface{}, error) {
	switch v := n.(type) {
	case *expr.Literal:
		return parseLiteralValue(v)
	case *expr.Placeholder:
		if argIndex == nil {
			return nil, fmt.Errorf("argIndex is nil")
		}
		if *argIndex >= len(e.args) {
			return nil, fmt.Errorf("not enough arguments provided")
		}
		value := e.args[*argIndex]
		*argIndex++
		return value, nil
	default:
		return nil, fmt.Errorf("unsupported expression type: %T", v)
	}
}

func parseLiteralValue(v *expr.Literal) (interface{}, error) {
	switch v.Kind {
	case "string":
		str := v.Value
		if len(str) >= 2 && ((str[0] == '\'' && str[len(str)-1] == '\'') || (str[0] == '"' && str[len(str)-1] == '"')) {
			str = str[1 : len(str)-1]
		}
		return str, nil
	case "int":
		return strconv.ParseInt(v.Value, 10, 64)
	case "float":
		return strconv.ParseFloat(v.Value, 64)
	case "bool":
		return strconv.ParseBool(v.Value)
	case "null":
		return nil, nil
	default:
		return nil, fmt.Errorf("unsupported literal kind: %s", v.Kind)
	}
}
