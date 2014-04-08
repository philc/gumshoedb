// Benchmarks which use the gumshoe core code paths in a representative way. These numbers should be compared
// to the ideal, simplified synthetic benchmarks to find areas for improvement.

package gumshoe

import (
	"fmt"
	"testing"
	"time"
	"unsafe"
	"utils"
)

const (
	BenchmarkRows    = 100000 // The row count to use in these benchmarks.
	BenchmarkColumns = 42
	//tempDir          = "/tmp/gumshoe_benchmark"
)

// TODO(caleb): Look into test setup time
//var db *DB

//func setup() {
//if db == nil {
//db = setUpDB()
//}
//}

// TODO(caleb): Change a to accept a testing.TB rather than a *testing.T so that we can use Assert instead of
// this function (and throughout benchmarks generally).
func checkResult(b *testing.B, actual, expected interface{}) {
	ok, message := utils.HasEqualJSON(actual, expected)
	if !ok {
		b.Fatalf(message)
	}
}

// A query which only sums aggregates.
func BenchmarkAggregateQuery(b *testing.B) {
	//setup()
	db := setUpDB()
	query := createBenchmarkQuery(nil, nil)
	var results []RowMap
	var err error
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		results, err = db.GetQueryResult(query)
		if err != nil {
			b.Fatal(err)
		}
	}
	checkResult(b, results, []RowMap{{"metric001": BenchmarkRows, "rowCount": BenchmarkRows}})
	//setBytes(b)
}

// A query which filters rows by a single, simple filter function.
//func BenchmarkFilterQuery(b *testing.B) {
//setup(b)
//// Metric 2 cycles between 0 and 1, so this will filter out 1/2 the columns.
//query := createQuery(nil, []gumshoe.QueryFilter{{">", "metric002", 0}})
//if err := gumshoe.ValidateQuery(factTable, query); err != nil {
//b.Fatal(err)
//}
//b.ResetTimer()
//var result map[string]gumshoe.Untyped
//for i := 0; i < b.N; i++ {
//result = factTable.InvokeQuery(query)
//}
//checkResult(b, result["results"],
//[]map[string]int{{"metric001": BenchmarkRows / 2, "rowCount": BenchmarkRows / 2}})
//setBytes(b)
//}

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

//func createQuery(groupings []gumshoe.QueryGrouping, filters []gumshoe.QueryFilter) *gumshoe.Query {
//return &gumshoe.Query{
//TableName:  "tableName",
//Aggregates: createQueryAggregates([]string{"metric001"}),
//Groupings:  groupings,
//Filters:    filters,
//}
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

//// Creates a QueryAggregate structure which represents the sums of the given columns.
//func createQueryAggregates(columns []string) []gumshoe.QueryAggregate {
//queryAggregates := make([]gumshoe.QueryAggregate, len(columns))
//for i, column := range columns {
//queryAggregates[i] = gumshoe.QueryAggregate{"sum", column, column}
//}
//return queryAggregates
//}

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

func setBytes(b *testing.B) {
	b.SetBytes(int64(BenchmarkRows * BenchmarkColumns * unsafe.Sizeof(float32(0))))
}
