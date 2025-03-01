package realtime

import (
	"fmt"
	"github.com/viant/sqlparser/expr"
	"github.com/viant/sqlparser/node"
)

type evaluator struct {
	args     []interface{}
	argIndex int
}

func (e *evaluator) evaluateExpr(n node.Node) (interface{}, error) {
	switch v := n.(type) {
	case *expr.Literal:
		return parseLiteralValue(v)
	case *expr.Placeholder:
		if v.Name == "?" {
			if e.argIndex >= len(e.args) {
				return nil, fmt.Errorf("not enough arguments")
			}
			value := e.args[e.argIndex]
			e.argIndex++
			return value, nil
		}
		// Handle placeholders in the form $1, $2, etc.
		var index int
		if len(v.Name) > 1 && v.Name[0] == '$' {
			_, err := fmt.Sscanf(v.Name, "$%d", &index)
			if err != nil || index < 1 || index > len(e.args) {
				return nil, fmt.Errorf("invalid placeholder '%s'", v.Name)
			}
			return e.args[index-1], nil
		}
		return nil, fmt.Errorf("invalid placeholder '%s'", v.Name)

	default:
		return nil, fmt.Errorf("unsupported expression type: %T", n)
	}
}

// evaluateExprWithArgIndex evaluates an expression and returns its value, using an argument index
func (e *evaluator) evaluateExprWithArgIndex(exprNode node.Node, argIndex *int) (interface{}, error) {
	switch v := exprNode.(type) {
	case *expr.Literal:
		return parseLiteralValue(v)
	case *expr.Placeholder:
		if *argIndex >= len(e.args) {
			return nil, fmt.Errorf("not enough arguments provided for placeholder")
		}
		arg := e.args[*argIndex]
		*argIndex++
		return arg, nil
	default:
		return nil, fmt.Errorf("unsupported expression type: %T", v)
	}
}

// Helper function to parse literal values
func parseLiteralValue(lit *expr.Literal) (interface{}, error) {
	switch lit.Kind {
	case "string":
		return lit.Value, nil
	case "int":
		var intValue int
		_, err := fmt.Sscanf(lit.Value, "%d", &intValue)
		if err != nil {
			return nil, err
		}
		return intValue, nil
	case "numeric":
		var floatValue float64
		_, err := fmt.Sscanf(lit.Value, "%f", &floatValue)
		if err != nil {
			return nil, err
		}
		return floatValue, nil
	case "bool":
		var boolValue bool
		_, err := fmt.Sscanf(lit.Value, "%t", &boolValue)
		if err != nil {
			return nil, err
		}
		return boolValue, nil
	default:
		return nil, fmt.Errorf("unsupported literal kind: %s", lit.Kind)
	}
}
