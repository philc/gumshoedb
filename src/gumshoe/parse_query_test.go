package gumshoe

import (
	"strings"
	"testing"

	. "github.com/cespare/a"
)

func TestParseQuery(t *testing.T) {
	const queryString = `
		{
	   "aggregates": [{"type": "sum", "name": "metric1", "column": "metric1"}],
	   "groupings": [{"column": "dim1", "name":"grouping-name"}],
     "filters": [{"type": "!=", "column": "at", "value": 1}]
		}`
	query, err := ParseJSONQuery(strings.NewReader(queryString))
	Assert(t, err, IsNil)

	// Spot checks
	Assert(t, query.Aggregates[0].Type, Equals, AggregateSum)
	Assert(t, query.Groupings[0].Name, Equals, "grouping-name")
	Assert(t, query.Filters[0].Type, Equals, FilterNotEqual)
}
