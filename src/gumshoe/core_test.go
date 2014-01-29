package gumshoe

import (
	"encoding/json"
	"testing"

	. "github.com/cespare/a"
)

func convertToJSONAndBack(o interface{}) interface{} {
	b, err := json.Marshal(o)
	if err != nil {
		panic(err)
	}
	result := new(interface{})
	json.Unmarshal(b, result)
	return *result
}

// A variant of DeepEquals which is less finicky about which numeric type you're using in maps.
func HasEqualJSON(args ...interface{}) (ok bool, message string) {
	o1 := convertToJSONAndBack(args[0])
	o2 := convertToJSONAndBack(args[1])
	return DeepEquals(o1, o2)
}

func tableFixture() *FactTable {
	schema := NewSchema()
	schema.NumericColumns = map[string]int{"col1": TypeFloat32}
	schema.StringColumns = map[string]int{"col2": TypeFloat32}
	return NewFactTable("", 3, schema)
}

func insertRow(table *FactTable, column1Value Untyped, column2Value Untyped) {
	table.InsertRowMaps([]map[string]Untyped{{"col1": column1Value, "col2": column2Value}})
}

func createQuery() Query {
	query := Query{"", []QueryAggregate{{"sum", "col1", "col1"}}, nil, nil}
	return query
}

func TestConvertRowMapToRowArrayThrowsErrorForUnrecognizedColumn(t *testing.T) {
	_, err := tableFixture().convertRowMapToRowArray(map[string]Untyped{"col1": 5, "unknownColumn": 10})
	Assert(t, err, NotNil)
}

func createTableFixtureForFilterTests() *FactTable {
	table := tableFixture()
	insertRow(table, 1.0, "stringvalue1")
	insertRow(table, 2.0, "stringvalue2")
	return table
}

func createTableFixtureForNullQueryTests() *FactTable {
	schema := NewSchema()
	schema.NumericColumns = map[string]int{"col1": TypeFloat32}
	schema.StringColumns = map[string]int{"col2": TypeFloat32}
	table := NewFactTable("", 10, schema)
	insertRow(table, 1.0, "a")
	insertRow(table, 2.0, "b")
	insertRow(table, nil, "a")
	insertRow(table, 1.0, nil)
	insertRow(table, 2.0, "a")
	insertRow(table, 3.0, "b")
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
	results := runWithFilter(table, QueryFilter{"equal", "col1", 2})
	Assert(t, results[0]["col1"], Equals, 2.0)

	results = runWithFilter(table, QueryFilter{"equal", "col2", "stringvalue2"})
	Assert(t, results[0]["col1"], Equals, 2.0)

	// These match zero rows.
	results = runWithFilter(table, QueryFilter{"equal", "col1", 3})
	Assert(t, results[0]["col1"], Equals, 0.0)

	results = runWithFilter(table, QueryFilter{"equal", "col2", "non-existant"})
	Assert(t, results[0]["col1"], Equals, 0.0)
}

func TestInvokeQueryFiltersRowsUsingLessThan(t *testing.T) {
	table := createTableFixtureForFilterTests()
	Assert(t, runWithFilter(table, QueryFilter{"lessThan", "col1", 2})[0]["col1"], Equals, 1.0)
	// Matches zero rows.
	Assert(t, runWithFilter(table, QueryFilter{"lessThan", "col1", 1})[0]["col1"], Equals, 0.0)
}

func TestInvokeQueryFiltersRowsUsingIn(t *testing.T) {
	table := createTableFixtureForFilterTests()
	Assert(t, runWithFilter(table, QueryFilter{"in", "col1", []interface{}{2}})[0]["col1"], Equals, 2.0)
	Assert(t, runWithFilter(table, QueryFilter{"in", "col1", []interface{}{2, 1}})[0]["col1"], Equals, 3.0)
	Assert(t, runWithFilter(table, QueryFilter{"in", "col2", []interface{}{"stringvalue1"}})[0]["col1"],
		Equals, 1.0)
	// These match zero rows.
	Assert(t, runWithFilter(table, QueryFilter{"in", "col2", []interface{}{3}})[0]["col1"], Equals, 0.0)
	Assert(t, runWithFilter(table, QueryFilter{"in", "col2", []interface{}{"non-existant"}})[0]["col1"],
		Equals, 0.0)
}

func TestInvokeQueryWorksWhenGroupingByAStringColumn(t *testing.T) {
	table := tableFixture()
	insertRow(table, 1.0, "stringvalue1")
	insertRow(table, 2.0, "stringvalue1")
	insertRow(table, 5.0, "stringvalue2")
	result := runWithGroupBy(table, QueryGrouping{"", "col2", "groupbykey"})
	Assert(t, result[0], HasEqualJSON,
		map[string]Untyped{"groupbykey": "stringvalue1", "rowCount": 2, "col1": 3})
	Assert(t, result[1], HasEqualJSON,
		map[string]Untyped{"groupbykey": "stringvalue2", "rowCount": 1, "col1": 5})
}

func TestGroupingWithATimeTransformFunctionWorks(t *testing.T) {
	table := tableFixture()
	// col1 will be truncated into minutes when we group by it, so these rows represent 0 and 2 minutes
	// respectively.
	insertRow(table, 0.0, "")
	insertRow(table, 120.0, "")
	insertRow(table, 150.0, "")
	result := runWithGroupBy(table, QueryGrouping{"minute", "col1", "groupbykey"})
	Assert(t, result[0], HasEqualJSON, map[string]Untyped{"groupbykey": 0, "rowCount": 1, "col1": 0})
	Assert(t, result[1], HasEqualJSON, map[string]Untyped{"groupbykey": 120, "rowCount": 2, "col1": 270})
}

func TestInsertAndReadNullValues(t *testing.T) {
	table := tableFixture()
	insertRow(table, nil, "a")
	insertRow(table, 1.0, nil)
	insertRow(table, nil, nil)
	results := table.GetRowMaps(0, table.Count)
	Assert(t, results[0]["col1"], Equals, nil)
	Assert(t, results[0]["col2"], Equals, "a")
	Assert(t, results[1]["col1"].(float32), Equals, float32(1.0))
	Assert(t, results[1]["col2"], Equals, nil)
	Assert(t, results[2]["col1"], Equals, nil)
	Assert(t, results[2]["col2"], Equals, nil)
}

func TestAggregateQueryWithNullValues(t *testing.T) {
	table := createTableFixtureForNullQueryTests()
	results := runQuery(table, createQuery())
	Assert(t, results[0], HasEqualJSON, map[string]Untyped{"col1": 9, "rowCount": 6})
}

func TestFilterQueryWithNullValues(t *testing.T) {
	table := createTableFixtureForNullQueryTests()
	results := runWithFilter(table, QueryFilter{"lessThan", "col1", 2})
	Assert(t, results[0], HasEqualJSON, map[string]Untyped{"col1": 2, "rowCount": 2})
}

func TestGroupByQueryWithNullValues(t *testing.T) {
	table := createTableFixtureForNullQueryTests()
	results := runWithGroupBy(table, QueryGrouping{"", "col2", "groupbykey"})
	Assert(t, results, HasEqualJSON, []interface{}{
		map[string]Untyped{"col1": 3, "groupbykey": "a", "rowCount": 3},
		map[string]Untyped{"col1": 5, "groupbykey": "b", "rowCount": 2},
	})
}
