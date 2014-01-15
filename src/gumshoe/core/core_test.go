package core

import (
	json "encoding/json"
	. "github.com/smartystreets/goconvey/convey"
	"testing"
)

func tableFixture() *FactTable {
	return NewFactTable("", []string{"col1", "col2"})
}

func insertRow(table *FactTable, column1Value Untyped, column2Value Untyped) {
	InsertRowMaps(table, []map[string]Untyped{{"col1": column1Value, "col2": column2Value}})
}

func createQuery() Query {
	query := Query{"", []QueryAggregate{QueryAggregate{"sum", "col1", "col1"}}, nil, nil}
	return query
}

func convertToJsonAndBack(o interface{}) interface{} {
	b, err := json.Marshal(o)
	if err != nil {
		panic(err.Error)
	}
	result := new(interface{})
	json.Unmarshal(b, result)
	return *result
}

// A variant of DeepEqual/ShouldResemble which is less finicky about which numeric type you're using in maps.
func ShouldHaveEqualJson(actual interface{}, expected ...interface{}) string {
	o1 := convertToJsonAndBack(actual)
	o2 := convertToJsonAndBack(expected[0])
	return ShouldResemble(o1, o2)
}

func TestConvertRowMapToRowArray(t *testing.T) {
	Convey("throws an error for an unrecognized column", t, func() {
		_, error := convertRowMapToRowArray(tableFixture(), map[string]Untyped{"col1": 5, "unknownColumn": 10})
		So(error, ShouldNotBeNil)
	})
}

func TestInvokeQuery(t *testing.T) {
	table := tableFixture()
	insertRow(table, 1, "stringvalue1")
	insertRow(table, 2, "stringvalue2")

	runWithFilter := func(filter QueryFilter) []map[string]Untyped {
		query := createQuery()
		query.Filters = []QueryFilter{filter}
		return InvokeQuery(table, &query)["results"].([]map[string]Untyped)
	}

	runWithGroupBy := func(table *FactTable, filter QueryGrouping) []map[string]Untyped {
		query := createQuery()
		query.Groupings = []QueryGrouping{filter}
		return InvokeQuery(table, &query)["results"].([]map[string]Untyped)
	}

	Convey("Filters rows using equals", t, func() {
		results := runWithFilter(QueryFilter{"equal", "col1", 2})
		So(results[0]["col1"], ShouldEqual, 2)

		results = runWithFilter(QueryFilter{"equal", "col2", "stringvalue2"})
		So(results[0]["col1"], ShouldEqual, 2)

		// These match zero rows.
		results = runWithFilter(QueryFilter{"equal", "col1", 3})
		So(results[0]["col1"], ShouldEqual, 0)

		results = runWithFilter(QueryFilter{"equal", "col2", "non-existant"})
		So(results[0]["col1"], ShouldEqual, 0)
	})

	Convey("Filters rows using 'less than'", t, func() {
		So(runWithFilter(QueryFilter{"lessThan", "col1", 1})[0]["col1"], ShouldEqual, 0) // Matches zero rows.
		So(runWithFilter(QueryFilter{"lessThan", "col1", 2})[0]["col1"], ShouldEqual, 1)
	})

	Convey("Filters rows using 'in'", t, func() {
		So(runWithFilter(QueryFilter{"in", "col1", []interface{}{2}})[0]["col1"], ShouldEqual, 2)
		So(runWithFilter(QueryFilter{"in", "col1", []interface{}{2, 1}})[0]["col1"], ShouldEqual, 3)
		So(runWithFilter(QueryFilter{"in", "col2", []interface{}{"stringvalue1"}})[0]["col1"], ShouldEqual, 1)
		// These match zero rows.
		So(runWithFilter(QueryFilter{"in", "col2", []interface{}{3}})[0]["col1"], ShouldEqual, 0)
		So(runWithFilter(QueryFilter{"in", "col2", []interface{}{"non-existant"}})[0]["col1"], ShouldEqual, 0)
	})

	Convey("Group by works when grouping by a string column", t, func() {
		table := tableFixture()
		insertRow(table, 1, "stringvalue1")
		insertRow(table, 2, "stringvalue1")
		insertRow(table, 5, "stringvalue2")
		result := runWithGroupBy(table, QueryGrouping{"", "col2", "groupbykey"})
		So(result[0], ShouldHaveEqualJson,
			map[string]Untyped{"groupbykey": "stringvalue1", "rowCount": 2, "col1": 3})
		So(result[1], ShouldHaveEqualJson,
			map[string]Untyped{"groupbykey": "stringvalue2", "rowCount": 1, "col1": 5})
	})

	Convey("Group by works when grouping by an int column", t, func() {
		table := tableFixture()
		insertRow(table, 2, "stringvalue1")
		insertRow(table, 2, "stringvalue1")
		insertRow(table, 5, "stringvalue2")
		result := runWithGroupBy(table, QueryGrouping{"", "col1", "groupbykey"})
		So(result[0], ShouldHaveEqualJson, map[string]Untyped{"groupbykey": 2, "rowCount": 2, "col1": 4})
		So(result[1], ShouldHaveEqualJson, map[string]Untyped{"groupbykey": 5, "rowCount": 1, "col1": 5})
	})

	Convey("Grouping with a time transform function works", t, func() {
		table := tableFixture()
		// col1 will be truncated into minutes when we group by it, so these rows represent 0 and 2 minutes
		// respectively.
		insertRow(table, 0, "")
		insertRow(table, 120, "")
		insertRow(table, 150, "")
		result := runWithGroupBy(table, QueryGrouping{"minute", "col1", "groupbykey"})
		So(result[0], ShouldHaveEqualJson, map[string]Untyped{"groupbykey": 0, "rowCount": 1, "col1": 0})
		So(result[1], ShouldHaveEqualJson, map[string]Untyped{"groupbykey": 120, "rowCount": 2, "col1": 270})
	})
}
