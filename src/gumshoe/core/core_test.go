package core

import (
	"testing"
	. "github.com/smartystreets/goconvey/convey"
)

func tableFixture() *FactTable {
	return NewFactTable("", []string{"col1", "col2"})
}

func TestConvertRowMapToRowArray(t *testing.T) {
	Convey("throws an error for an unrecognized column", t, func() {
		_, error := convertRowMapToRowArray(tableFixture(), map[string]Untyped{"col1": 5, "unknownColumn": 10})
		So(error, ShouldNotBeNil)
	})
}

func insertRow(table *FactTable, column1Value Untyped, column2Value Untyped) {
	InsertRowMaps(table, []map[string]Untyped{{"col1": column1Value, "col2": column2Value,}})
}

func createQuery() Query {
	query := Query{"", []QueryAggregate{QueryAggregate{"sum", "col1", "col1"}}, nil, nil}
	return query
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

	Convey("Filters rows using 'less than'", t, func () {
		So(runWithFilter(QueryFilter{"lessThan", "col1", 1})[0]["col1"], ShouldEqual, 0) // Matches zero rows.
		So(runWithFilter(QueryFilter{"lessThan", "col1", 2})[0]["col1"], ShouldEqual, 1)
	})

	Convey("Filters rows using 'in'", t, func () {
		So(runWithFilter(QueryFilter{"in", "col1", []interface{}{2}})[0]["col1"], ShouldEqual, 2)
		So(runWithFilter(QueryFilter{"in", "col1", []interface{}{2, 1}})[0]["col1"], ShouldEqual, 3)
		So(runWithFilter(QueryFilter{"in", "col2", []interface{}{"stringvalue1"}})[0]["col1"], ShouldEqual, 1)
		// These match zero rows.
		So(runWithFilter(QueryFilter{"in", "col2", []interface{}{3}})[0]["col1"], ShouldEqual, 0)
		So(runWithFilter(QueryFilter{"in", "col2", []interface{}{"non-existant"}})[0]["col1"], ShouldEqual, 0)
	})
}
