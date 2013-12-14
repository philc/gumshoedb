package core

import (
	"testing"
	"fmt"
)

func TestParseQuery(t *testing.T) {
	jsonString := `
		{"table":"the_table",
     "aggregates": [{"type": "sum", "name": "clicks", "column": "country"}],
     "groupings": [{"column": "country", "name":"country1"}]
		}`

	fmt.Println(ParseJsonQuery(jsonString))
}
