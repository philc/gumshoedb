// +build ignore

package gumshoe

import (
	"testing"

	"utils"

	. "github.com/cespare/a"
)

func createQuery() Query {
	query := Query{"", []QueryAggregate{{"sum", "metric1", "metric1"}}, nil, nil}
	return query
}

func createTableFixtureForFilterTests() *FactTable {
	table := tableFixture()
	insertRow(table, 0, "string1", 1.0)
	insertRow(table, 0, "string2", 2.0)
	return table
}

func createTableFixtureForNilQueryTests() *FactTable {
	schema := NewSchema()
	schema.DimensionColumns = map[string]int{"dim1": TypeInt32}
	schema.MetricColumns = map[string]int{"metric1": TypeInt32}
	schema.StringColumns = []string{"dim1"}
	schema.TimestampColumn = "at"
	table := NewFactTable("", schema)
	insertRow(table, 0, "a", 1.0)
	insertRow(table, 0, "b", 2.0)
	insertRow(table, 0, nil, 4.0)
	return table
}

func runQuery(table *FactTable, query Query) []map[string]Untyped {
	return table.InvokeQuery(&query)["results"].([]map[string]Untyped)
}

func runWithFilter(table *FactTable, filter QueryFilter) []map[string]Untyped {
	query := createQuery()
	query.Filters = []QueryFilter{filter}
	return runQuery(table, query)
}

func runWithGroupBy(table *FactTable, filter QueryGrouping) []map[string]Untyped {
	query := createQuery()
	query.Groupings = []QueryGrouping{filter}
	return runQuery(table, query)
}

func TestInvokeQueryFiltersRowsUsingEqualsFilter(t *testing.T) {
	table := createTableFixtureForFilterTests()
	results := runWithFilter(table, QueryFilter{"=", "metric1", 2})
	Assert(t, results[0]["metric1"], Equals, 2.0)

	results = runWithFilter(table, QueryFilter{"=", "dim1", "string2"})
	Assert(t, results[0]["metric1"], Equals, 2.0)

	// These match zero rows.
	results = runWithFilter(table, QueryFilter{"=", "metric1", 3})
	Assert(t, results[0]["metric1"], Equals, 0.0)

	results = runWithFilter(table, QueryFilter{"=", "dim1", "non-existant"})
	Assert(t, results[0]["metric1"], Equals, 0.0)
}

func TestInvokeQueryFiltersRowsUsingLessThan(t *testing.T) {
	table := createTableFixtureForFilterTests()
	Assert(t, runWithFilter(table, QueryFilter{"<", "metric1", 2})[0]["metric1"], Equals, 1.0)
	// Matches zero rows.
	Assert(t, runWithFilter(table, QueryFilter{"<", "metric1", 1})[0]["metric1"], Equals, 0.0)
}

func TestInvokeQueryFiltersRowsUsingIn(t *testing.T) {
	table := createTableFixtureForFilterTests()
	Assert(t, runWithFilter(table, QueryFilter{"in", "metric1", []interface{}{2}})[0]["metric1"], Equals, 2.0)
	Assert(t, runWithFilter(table, QueryFilter{"in", "metric1", []interface{}{2, 1}})[0]["metric1"],
		Equals, 3.0)
	Assert(t, runWithFilter(table, QueryFilter{"in", "dim1", []interface{}{"string1"}})[0]["metric1"],
		Equals, 1.0)
	// These match zero rows.
	Assert(t, runWithFilter(table, QueryFilter{"in", "metric1", []interface{}{3}})[0]["metric1"], Equals, 0.0)
	Assert(t, runWithFilter(table, QueryFilter{"in", "dim1", []interface{}{"non-existant"}})[0]["metric1"],
		Equals, 0.0)
}

func TestInvokeQueryWorksWhenGroupingByAStringColumn(t *testing.T) {
	table := tableFixture()
	insertRow(table, 0, "string1", 1.0)
	insertRow(table, 0, "string1", 2.0)
	insertRow(table, 0, "string2", 5.0)
	result := runWithGroupBy(table, QueryGrouping{"", "dim1", "groupbykey"})
	Assert(t, result[0], utils.HasEqualJSON,
		map[string]Untyped{"groupbykey": "string1", "rowCount": 2, "metric1": 3})
	Assert(t, result[1], utils.HasEqualJSON,
		map[string]Untyped{"groupbykey": "string2", "rowCount": 1, "metric1": 5})
}

func TestGroupingWithATimeTransformFunctionWorks(t *testing.T) {
	// TODO(philc): Revisit this in light of the new storage schema.
	t.Skip()
	table := tableFixture()
	// col1 will be truncated into minutes when we group by it, so these rows represent 0 and 2 minutes
	// respectively.
	insertRow(table, 0, "", 0.0)
	insertRow(table, 0, "", 120.0)
	insertRow(table, 0, "", 150.0)
	result := runWithGroupBy(table, QueryGrouping{"minute", "metric1", "groupbykey"})
	Assert(t, result[0], utils.HasEqualJSON, map[string]Untyped{"groupbykey": 0, "rowCount": 1, "metric1": 0})
	Assert(t, result[1], utils.HasEqualJSON, map[string]Untyped{"groupbykey": 120, "rowCount": 2,
		"metric1": 270})
}

func TestAggregateQueryWithNilValues(t *testing.T) {
	table := createTableFixtureForNilQueryTests()
	results := runQuery(table, createQuery())
	Assert(t, results[0], utils.HasEqualJSON, map[string]Untyped{"metric1": 7, "rowCount": 3})
}

func TestFilterQueryWithNilValues(t *testing.T) {
	table := createTableFixtureForNilQueryTests()
	results := runWithFilter(table, QueryFilter{"=", "dim1", "a"})
	Assert(t, results[0], utils.HasEqualJSON, map[string]Untyped{"metric1": 1, "rowCount": 1})
	// TODO(philc): Enable equals filters by nil values on string columns.
	// results = runWithFilter(table, QueryFilter{"equals", "dim1", nil})
	// Assert(t, results[0], utils.HasEqualJSON, map[string]Untyped{"metric1": 5, "rowCount": 1})
}

func TestGroupByQueryWithNilValues(t *testing.T) {
	table := createTableFixtureForNilQueryTests()
	results := runWithGroupBy(table, QueryGrouping{"", "dim1", "groupbykey"})
	Assert(t, results, utils.HasEqualJSON, []interface{}{
		map[string]Untyped{"metric1": 1, "groupbykey": "a", "rowCount": 1},
		map[string]Untyped{"metric1": 2, "groupbykey": "b", "rowCount": 1},
	})
}
