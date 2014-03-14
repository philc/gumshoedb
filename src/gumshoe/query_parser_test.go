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
	db := new(DB)
	_, err := db.ParseJSONQuery(strings.NewReader(queryString))
	Assert(t, err, IsNil)
}
