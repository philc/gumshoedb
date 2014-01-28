// Benchmarks which use the gumshoe core code paths in a representative way. These numbers should be compared
// to the ideal, simplified synthetic benchmarks to find areas for improvement.
package gumshoe_test

import (
	"encoding/json"
	"fmt"
	"os"
	"testing"
	"unsafe"

	"gumshoe"

	. "github.com/cespare/a"
)

const (
	BenchmarkRows    = 100000 // The row count to use in these benchmarks.
	BenchmarkColumns = 42
	tempDir          = "/tmp/gumshoe_benchmark"
)

var factTable *gumshoe.FactTable

func init() {
	factTable = setupFactTable()
}

// TODO(philc): This is duplicated from core_test.go. What's the best way to share it?
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

func checkResult(b *testing.B, actual interface{}, expected interface{}) {
	ok, message := HasEqualJSON(actual, expected)
	if !ok {
		b.Fatalf(message)
	}
}

// A query which only sums aggregates.
func BenchmarkAggregateQuery(b *testing.B) {
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
		[]map[string]gumshoe.Untyped{{"column001": BenchmarkRows,
			"rowCount": BenchmarkRows}})
	setBytes(b)
}

// A query which filters rows by a single, simple filter function.
func BenchmarkFilterQuery(b *testing.B) {
	// column2 cycles between 0 and 1, so this will filter out 1/2 the columns.
	query := createQuery(nil, []gumshoe.QueryFilter{{">", "column002", 0}})
	if err := gumshoe.ValidateQuery(factTable, query); err != nil {
		b.Fatal(err)
	}
	b.ResetTimer()
	var result map[string]gumshoe.Untyped
	for i := 0; i < b.N; i++ {
		result = factTable.InvokeQuery(query)
	}
	checkResult(b, result["results"],
		[]map[string]int{{"column001": BenchmarkRows / 2, "rowCount": BenchmarkRows / 2}})
	setBytes(b)
}

// A query which groups by a column. Each column has 10 possible values, so the result set will contain 10 row
// aggregates.
func BenchmarkGroupByQuery(b *testing.B) {
	setBytes(b)
	// This creates 10 groupings.
	query := createQuery([]gumshoe.QueryGrouping{{"", "column003", "column003"}}, nil)
	if err := gumshoe.ValidateQuery(factTable, query); err != nil {
		panic(err)
	}
	b.ResetTimer()
	// TODO(philc): Check this result.
	for i := 0; i < b.N; i++ {
		factTable.InvokeQuery(query)
	}
}

// A query which groups by a column that is transformed using a time transform function.
func BenchmarkGroupByWithTimeTransformQuery(b *testing.B) {
	query := createQuery([]gumshoe.QueryGrouping{{"hour", "column002", "column002"}}, nil)
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
		Aggregates: createQueryAggregates([]string{"column001"}),
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

func setupFactTable() (table *gumshoe.FactTable) { //, dbTempDir string) {
	columnNames := make([]string, BenchmarkColumns)
	for i := range columnNames {
		// The columns are named columni where i has up to 2 leading zeros.
		columnNames[i] = fmt.Sprintf("column%03d", i)
	}
	os.RemoveAll(tempDir)
	os.MkdirAll(tempDir, 0755)
	schema := *gumshoe.NewSchema()
	for _, column := range columnNames {
		schema.NumericColumns[column] = gumshoe.Int32Type
	}
	table = gumshoe.NewFactTable(tempDir+"/db", BenchmarkRows, schema)
	populateTableWithTestingData(table)
	return table
}

func populateTableWithTestingData(table *gumshoe.FactTable) {
	rows := make([]map[string]gumshoe.Untyped, BenchmarkRows)

	for i := range rows {
		row := make(map[string]gumshoe.Untyped, table.ColumnCount)
		for j := 3; j < table.ColumnCount; j++ {
			row[table.ColumnIndexToName[j]] = float64(i % 10)
		}
		// We use properties of these column values to construct sanity-check assertions in our benchmarks.
		row[table.ColumnIndexToName[0]] = float64(0)
		row[table.ColumnIndexToName[1]] = float64(1)
		row[table.ColumnIndexToName[2]] = float64(i % 2)
		rows[i] = row
	}

	if err := table.InsertRowMaps(rows); err != nil {
		panic(err)
	}
}

func setBytes(b *testing.B) {
	b.SetBytes(int64(BenchmarkRows * BenchmarkColumns * unsafe.Sizeof(float32(0))))
}
