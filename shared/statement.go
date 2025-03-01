package shared

import (
	"fmt"
	"github.com/viant/sqlparser"
	"github.com/viant/sqlparser/expr"
	"github.com/viant/sqlparser/query"
	"strings"
)

func RemapInnerQuery(aQuery *query.Select, rawExpr *expr.Raw, setName *string) error {
	var whiteList = make(map[string]*query.Item)
	if innerQuery, ok := rawExpr.X.(*query.Select); ok {
		*setName = sqlparser.Stringify(innerQuery.From.X)
		if aQuery.Qualify == nil {
			aQuery.Qualify = innerQuery.Qualify
		}
		if aQuery.GroupBy == nil {
			aQuery.GroupBy = innerQuery.GroupBy
		}

		if !innerQuery.List.IsStarExpr() {
			for i := 0; i < len(innerQuery.List); i++ {
				item := innerQuery.List[i]
				switch actual := innerQuery.List[i].Expr.(type) {
				case *expr.Ident, *expr.Selector:
					whiteList[sqlparser.Stringify(actual)] = item
				case *expr.Literal:
					whiteList[item.Alias] = item
				case *expr.Call:
					if item.Alias == "" {
						return fmt.Errorf("newmapper: %v missing alias in outer query: %s", sqlparser.Stringify(item), sqlparser.Stringify(innerQuery))
					}
					whiteList[item.Alias] = item
				default:
					return fmt.Errorf("newmapper: invalid expr %s in  outer query: %s", sqlparser.Stringify(actual), sqlparser.Stringify(innerQuery))
				}
			}

			updatedList := make([]*query.Item, 0)
			for i := 0; i < len(aQuery.List); i++ {
				item := aQuery.List[i]
				name := sqlparser.Stringify(item.Expr)
				if idx := strings.Index(name, "."); idx != -1 { //remve alias if needed
					name = name[idx+1:]
				}
				if len(whiteList) > 0 {
					innerItem, ok := whiteList[name]
					if !ok {
						return fmt.Errorf("invalid outer query column: %v, in %v", name, sqlparser.Stringify(aQuery))
					}
					updatedList = append(updatedList, innerItem)
				}
			}
			aQuery.List = updatedList
		}
	}
	return nil
}
