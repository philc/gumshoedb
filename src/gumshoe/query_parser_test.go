package gumshoe

import (
	"testing"

	. "github.com/cespare/a"
)

func TestParseQuery(t *testing.T) {
	jsonString := `
		{"table":"the_table",
	   "aggregates": [{"type": "sum", "name": "metric1", "column": "metric1"}],
	   "groupings": [{"column": "dim1", "name":"grouping-name"}],
     "filters": [{"type": "!=", "column": "at", "value": 1}]
		}`
	query, err := ParseJSONQuery(jsonString)
	Assert(t, err, IsNil)
	table := tableFixture()
	err = ValidateQuery(table, query)
	Assert(t, err, IsNil)
}
