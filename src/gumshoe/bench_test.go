// +build ignore
// Benchmarks which use the gumshoe core code paths in a representative way. These numbers should be compared
// to the ideal, simplified synthetic benchmarks to find areas for improvement.

package gumshoe_test

import (
	"fmt"
	"testing"
	"unsafe"

	"gumshoe"
	"utils"
)

const (
	BenchmarkRows    = 100000 // The row count to use in these benchmarks.
	BenchmarkColumns = 42
	tempDir          = "/tmp/gumshoe_benchmark"
)

var factTable *gumshoe.FactTable

func setup(b *testing.B) {
	if factTable == nil {
		factTable = setupFactTable()
		b.ResetTimer()
	}
}

func checkResult(b *testing.B, actual interface{}, expected interface{}) {
	ok, message := utils.HasEqualJSON(actual, expected)
	if !ok {
		b.Fatalf(message)
	}
}

// A query which only sums aggregates.
func BenchmarkAggregateQuery(b *testing.B) {
	setup(b)
	query := createQuery(nil, nil)
	if err := gumshoe.ValidateQuery(factTable, query); err != nil {
		b.Fatal(err)
	}
	b.ResetTimer()
	var result map[string]gumshoe.Untyped
	for i := 0; i < b.N; i++ {
		result = factTable.InvokeQuery(query)
	}
	checkResult(b, result["results"],
		[]map[string]gumshoe.Untyped{{"metric001": BenchmarkRows, "rowCount": BenchmarkRows}})
	setBytes(b)
}

// A query which filters rows by a single, simple filter function.
func BenchmarkFilterQuery(b *testing.B) {
	setup(b)
	// Metric 2 cycles between 0 and 1, so this will filter out 1/2 the columns.
	query := createQuery(nil, []gumshoe.QueryFilter{{">", "metric002", 0}})
	if err := gumshoe.ValidateQuery(factTable, query); err != nil {
		b.Fatal(err)
	}
	b.ResetTimer()
	var result map[string]gumshoe.Untyped
	for i := 0; i < b.N; i++ {
		result = factTable.InvokeQuery(query)
	}
	checkResult(b, result["results"],
		[]map[string]int{{"metric001": BenchmarkRows / 2, "rowCount": BenchmarkRows / 2}})
	setBytes(b)
}

// A query which groups by a column. Each column has 10 possible values, so the result set will contain 10 row
// aggregates.
func BenchmarkGroupByQuery(b *testing.B) {
	setup(b)
	groupCount := 2
	query := createQuery([]gumshoe.QueryGrouping{{"", "dim2", "dim2"}}, nil)
	if err := gumshoe.ValidateQuery(factTable, query); err != nil {
		panic(err)
	}
	b.ResetTimer()
	var result map[string]gumshoe.Untyped
	for i := 0; i < b.N; i++ {
		result = factTable.InvokeQuery(query)
	}
	expectedResult := make([]map[string]gumshoe.Untyped, groupCount)
	for i := range expectedResult {
		expectedResult[i] = map[string]gumshoe.Untyped{
			"metric001": BenchmarkRows / groupCount, "dim2": i, "rowCount": BenchmarkRows / groupCount}
	}
	checkResult(b, result["results"], expectedResult)
	setBytes(b)
}

// A query which groups by a column that is transformed using a time transform function.
func BenchmarkGroupByWithTimeTransformQuery(b *testing.B) {
	setup(b)
	query := createQuery([]gumshoe.QueryGrouping{{"hour", "dim2", "dim2"}}, nil)
	if err := gumshoe.ValidateQuery(factTable, query); err != nil {
		panic(err)
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		factTable.InvokeQuery(query)
	}
	setBytes(b)
}

func createQuery(groupings []gumshoe.QueryGrouping, filters []gumshoe.QueryFilter) *gumshoe.Query {
	return &gumshoe.Query{
		TableName:  "tableName",
		Aggregates: createQueryAggregates([]string{"metric001"}),
		Groupings:  groupings,
		Filters:    filters,
	}
}

// Creates a QueryAggregate structure which represents the sums of the given columns.
func createQueryAggregates(columns []string) []gumshoe.QueryAggregate {
	queryAggregates := make([]gumshoe.QueryAggregate, len(columns))
	for i, column := range columns {
		queryAggregates[i] = gumshoe.QueryAggregate{"sum", column, column}
	}
	return queryAggregates
}

// The test fact table is constructed to represent a realistic schema.
func setupFactTable() (table *gumshoe.FactTable) {
	metricCount := BenchmarkColumns - 2
	columnNames := make([]string, metricCount)
	for i := range columnNames {
		// The columns are named columni where i has up to 2 leading zeros.
		columnNames[i] = fmt.Sprintf("metric%03d", i)
	}
	schema := gumshoe.NewSchema()
	for _, column := range columnNames {
		schema.MetricColumns[column] = gumshoe.TypeInt32
	}
	schema.DimensionColumns["dim1"] = gumshoe.TypeInt16
	// Used for grouping operations.
	schema.DimensionColumns["dim2"] = gumshoe.TypeInt16
	schema.TimestampColumn = "at"
	// Note that we're using an in-memory table so that these benchmarks start up faster.
	table = gumshoe.NewFactTable("", schema)
	populateTableWithTestingData(table)
	return table
}

func populateTableWithTestingData(table *gumshoe.FactTable) {
	rows := make([]gumshoe.RowMap, BenchmarkRows)

	for i := range rows {
		row := make(gumshoe.RowMap, len(table.Schema.MetricColumns))
		row["metric000"] = 0.0
		row["metric001"] = float64(1)
		row["metric002"] = float64(i % 2)

		for j := 3; j < len(table.Schema.MetricColumns); j++ {
			row[fmt.Sprintf("metric%03d", j)] = float64(i % 10)
		}
		row["dim1"] = float64(i)     // Since this is unique per row, it prevents row collapsing.
		row["dim2"] = float64(i % 2) // We use this for grouping operations.
		row["at"] = 0
		rows[i] = row
	}

	if err := table.InsertRowMaps(rows); err != nil {
		panic(err)
	}
}

func setBytes(b *testing.B) {
	b.SetBytes(int64(BenchmarkRows * BenchmarkColumns * unsafe.Sizeof(float32(0))))
}
