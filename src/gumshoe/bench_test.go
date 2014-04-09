// Benchmarks which use the gumshoe core code paths in a representative way. These numbers should be compared
// to the ideal, simplified synthetic benchmarks to find areas for improvement.

package gumshoe

import (
	"fmt"
	"testing"
	"utils"

	. "github.com/cespare/a"
)

const (
	BenchmarkRows    = 100000 // The row count to use in these benchmarks.
	BenchmarkColumns = 42
)

var benchmarkDB *DB

func setup(b *testing.B) {
	if benchmarkDB == nil {
		benchmarkDB = setUpDB()
	}
	b.SetBytes(int64(BenchmarkRows * benchmarkDB.RowSize))
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

	Assert(b, results[0], utils.DeepConvertibleEquals,
		RowMap{"metric001": BenchmarkRows, "rowCount": BenchmarkRows})
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
	Assert(b, results[0], utils.DeepConvertibleEquals,
		RowMap{"metric001": BenchmarkRows / 2, "rowCount": BenchmarkRows / 2})
}

// A query which groups by a column. Each column has 10 possible values, so the result set will contain 10 row
// aggregates.
func BenchmarkGroupByQuery(b *testing.B) {
	setup(b)
	query := createBenchmarkQuery([]QueryGrouping{{TimeTruncationNone, "dim3", "dim3"}}, nil)
	b.ResetTimer()
	var results []RowMap
	for i := 0; i < b.N; i++ {
		results = mustGetBenchmarkQueryResult(query)
	}
	b.StopTimer()
	groupCount := 2
	expectedResult := make([]RowMap, groupCount)
	for i := range expectedResult {
		expectedResult[i] = RowMap{
			"metric001": BenchmarkRows / groupCount, "dim3": i, "rowCount": BenchmarkRows / groupCount,
		}
	}
	Assert(b, results, utils.DeepConvertibleEquals, expectedResult)
}

// A query which groups by a column that is transformed using a time transform function.
func BenchmarkGroupByWithTimeTransformQuery(b *testing.B) {
	setup(b)
	query := createBenchmarkQuery([]QueryGrouping{{TimeTruncationHour, "dim2", "dim2"}}, nil)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		mustGetBenchmarkQueryResult(query)
	}
}

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
	dimensions := []DimensionColumn{
		makeDimensionColumn("dim1", "uint32", false),
		makeDimensionColumn("dim2", "uint32", false),
		makeDimensionColumn("dim3", "uint16", false),
	}
	numMetrics := BenchmarkColumns - len(dimensions)
	metrics := make([]MetricColumn, numMetrics)
	for i := range metrics {
		metrics[i] = makeMetricColumn(fmt.Sprintf("metric%03d", i), "int32")
	}

	schema := schemaFixture()
	schema.DimensionColumns = dimensions
	schema.MetricColumns = metrics
	schema.TimestampColumn = makeColumn("at", "uint32")
	schema.SegmentSize = 1 << 24

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
		row["dim3"] = float64(i % 2) // We use this for grouping operations.
		row["at"] = float64(0)
		rows[i] = row
	}

	if err := db.Insert(rows); err != nil {
		panic(err)
	}
}
