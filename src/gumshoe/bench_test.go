// Benchmarks which use the gumshoe core code paths in a representative way. These numbers should be compared
// to the ideal, simplified synthetic benchmarks to find areas for improvement.

package gumshoe

import (
	"fmt"
	"testing"
	"time"
	"utils"
)

const (
	BenchmarkRows    = 100000 // The row count to use in these benchmarks.
	BenchmarkColumns = 42
	//tempDir          = "/tmp/gumshoe_benchmark"
)

var benchmarkDB *DB

func setup(b *testing.B) {
	if benchmarkDB == nil {
		benchmarkDB = setUpDB()
	}
	b.SetBytes(int64(BenchmarkRows * benchmarkDB.RowSize))
}

// TODO(caleb): Change a to accept a testing.TB rather than a *testing.T so that we can use Assert instead of
// this function (and throughout benchmarks generally).
func checkResult(b *testing.B, actual, expected interface{}) {
	ok, message := utils.HasEqualJSON(actual, expected)
	if !ok {
		b.Fatalf(message)
	}
}

func mustGetBenchmarkQueryResult(query *Query) []RowMap {
	results, err := benchmarkDB.GetQueryResult(query)
	if err != nil {
		panic(err)
	}
	return results
}

// A query which only sums aggregates.
func BenchmarkAggregateQuery(b *testing.B) {
	setup(b)
	query := createBenchmarkQuery(nil, nil)
	var results []RowMap
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		results = mustGetBenchmarkQueryResult(query)
	}
	checkResult(b, results, []RowMap{{"metric001": BenchmarkRows, "rowCount": BenchmarkRows}})
}

// A query which filters rows by a single, simple filter function.
func BenchmarkFilterQuery(b *testing.B) {
	setup(b)
	// Metric 2 cycles between 0 and 1, so this will filter out 1/2 the columns.
	query := createBenchmarkQuery(nil, []QueryFilter{{FilterGreaterThan, "metric002", 0.0}})
	b.ResetTimer()
	var results []RowMap
	for i := 0; i < b.N; i++ {
		results = mustGetBenchmarkQueryResult(query)
	}
	checkResult(b, results, []RowMap{{"metric001": BenchmarkRows / 2, "rowCount": BenchmarkRows / 2}})
}

//// A query which groups by a column. Each column has 10 possible values, so the result set will contain 10 row
//// aggregates.
//func BenchmarkGroupByQuery(b *testing.B) {
//setup(b)
//groupCount := 2
//query := createQuery([]gumshoe.QueryGrouping{{"", "dim2", "dim2"}}, nil)
//if err := gumshoe.ValidateQuery(factTable, query); err != nil {
//panic(err)
//}
//b.ResetTimer()
//var result map[string]gumshoe.Untyped
//for i := 0; i < b.N; i++ {
//result = factTable.InvokeQuery(query)
//}
//expectedResult := make([]map[string]gumshoe.Untyped, groupCount)
//for i := range expectedResult {
//expectedResult[i] = map[string]gumshoe.Untyped{
//"metric001": BenchmarkRows / groupCount, "dim2": i, "rowCount": BenchmarkRows / groupCount}
//}
//checkResult(b, result["results"], expectedResult)
//setBytes(b)
//}

//// A query which groups by a column that is transformed using a time transform function.
//func BenchmarkGroupByWithTimeTransformQuery(b *testing.B) {
//setup(b)
//query := createQuery([]gumshoe.QueryGrouping{{"hour", "dim2", "dim2"}}, nil)
//if err := gumshoe.ValidateQuery(factTable, query); err != nil {
//panic(err)
//}
//b.ResetTimer()
//for i := 0; i < b.N; i++ {
//factTable.InvokeQuery(query)
//}
//setBytes(b)
//}

func createBenchmarkQuery(groupings []QueryGrouping, filters []QueryFilter) *Query {
	return &Query{
		Aggregates: createBenchmarkQueryAggregates([]string{"metric001"}),
		Groupings:  groupings,
		Filters:    filters,
	}
}

func createBenchmarkQueryAggregates(columns []string) []QueryAggregate {
	aggregates := make([]QueryAggregate, len(columns))
	for i, name := range columns {
		aggregates[i] = QueryAggregate{Type: AggregateSum, Column: name, Name: name}
	}
	return aggregates
}

// setUpDB creates a test DB to represent a realistic schema.
func setUpDB() *DB {
	dimensions := make([]DimensionColumn, 2)
	// TODO(caleb): Had to change this from int16 to int32 for apples-to-apples comparison. New gumshoedb
	// doesn't accept oversized values :)
	dimensions[0] = makeDimensionColumn("dim1", "int32", false)
	dimensions[1] = makeDimensionColumn("dim2", "int16", false)
	numMetrics := BenchmarkColumns - 2
	metrics := make([]MetricColumn, numMetrics)
	for i := range metrics {
		metrics[i] = makeMetricColumn(fmt.Sprintf("metric%03d", i), "int32")
	}

	schema := schemaFixture()
	schema.DimensionColumns = dimensions
	schema.MetricColumns = metrics
	schema.TimestampColumn = makeColumn("at", "uint32")
	schema.SegmentSize = 1 << 24
	schema.FlushDuration = time.Hour // No flushing!

	db, err := Open(schema)
	if err != nil {
		panic(err)
	}
	populateBenchmarkDB(db)
	db.Flush()
	return db
}

func populateBenchmarkDB(db *DB) {
	rows := make([]RowMap, BenchmarkRows)
	for i := range rows {
		row := make(RowMap)
		row["metric000"] = 0.0
		row["metric001"] = float64(1)
		row["metric002"] = float64(i % 2)
		for j := 3; j < len(db.MetricColumns); j++ {
			row[fmt.Sprintf("metric%03d", j)] = float64(i % 10)
		}
		row["dim1"] = float64(i)     // Since this is unique per row, it prevents row collapsing.
		row["dim2"] = float64(i % 2) // We use this for grouping operations.
		row["at"] = float64(0)
		rows[i] = row
	}

	if err := db.Insert(rows); err != nil {
		panic(err)
	}
}
