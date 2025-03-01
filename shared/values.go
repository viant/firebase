package shared

import (
	"database/sql/driver"
	"fmt"
)

// Values represents value slice
type Values []driver.Value

func (v Values) Named() []driver.NamedValue {
	var result []driver.NamedValue
	for i, value := range v {
		result = append(result, driver.NamedValue{Name: fmt.Sprintf("$%d", i+1), Value: value})
	}
	return result
}
