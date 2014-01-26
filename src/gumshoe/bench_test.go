// Benchmarks which use the gumshoe core code paths in a representative way. These numbers should be compared
// to the ideal, simplified synthetic benchmarks to find areas for improvement.
package gumshoe_test

import (
	"fmt"
	"os"
	"testing"
	"unsafe"

	"gumshoe"
)

const (
	BenchmarkRows    = 100000 // The row count to use in these benchmarks.
	BenchmarkColumns = 42
	tempDir          = "/tmp/gumshoe_benchmark"
)

var factTable *gumshoe.FactTable

func init() {
	factTable = setupFactTable()
	populateTableWithTestingData(factTable)
}

// A query which only sums aggregates.
func BenchmarkAggregateQuery(b *testing.B) {
	setBytes(b)
	query := createQuery(nil, nil)
	if err := gumshoe.ValidateQuery(factTable, query); err != nil {
		b.Fatal(err)
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		factTable.InvokeQuery(query)
	}
	setBytes(b)
}

// A query which filters rows by a single, simple filter function.
func BenchmarkFilterQuery(b *testing.B) {
	setBytes(b)
	query := createQuery(nil, []gumshoe.QueryFilter{{">", "column2", 5}})
	if err := gumshoe.ValidateQuery(factTable, query); err != nil {
		b.Fatal(err)
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		factTable.InvokeQuery(query)
	}
}

// A query which groups by a column. Each column has 10 possible values, so the result set will contain 10 row
// aggregates.
func BenchmarkGroupByQuery(b *testing.B) {
	setBytes(b)
	query := createQuery([]gumshoe.QueryGrouping{{"", "column2", "column2"}}, nil)
	if err := gumshoe.ValidateQuery(factTable, query); err != nil {
		panic(err)
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		factTable.InvokeQuery(query)
	}
}

// A query which groups by a column that is transformed using a time transform function.
func BenchmarkGroupByWithTimeTransformQuery(b *testing.B) {
	setBytes(b)
	query := createQuery([]gumshoe.QueryGrouping{{"hour", "column2", "column2"}}, nil)
	if err := gumshoe.ValidateQuery(factTable, query); err != nil {
		panic(err)
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		factTable.InvokeQuery(query)
	}
}

func createQuery(groupings []gumshoe.QueryGrouping, filters []gumshoe.QueryFilter) *gumshoe.Query {
	return &gumshoe.Query{
		TableName:  "tableName",
		Aggregates: createQueryAggregates([]string{"column1"}),
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
		columnNames[i] = fmt.Sprintf("column%d", i)
	}
	os.RemoveAll(tempDir)
	os.MkdirAll(tempDir, 0755)
	table = gumshoe.NewFactTable(tempDir+"/db", BenchmarkRows, columnNames)
	populateTableWithTestingData(table)
	return table
}

func populateTableWithTestingData(table *gumshoe.FactTable) {
	rows := make([]map[string]gumshoe.Untyped, BenchmarkRows)

	for i := range rows {
		row := make(map[string]gumshoe.Untyped, table.ColumnCount)
		for j := 0; j < table.ColumnCount; j++ {
			row[table.ColumnIndexToName[j]] = i % 10
		}
		rows[i] = row
	}

	if err := table.InsertRowMaps(rows); err != nil {
		panic(err)
	}
}

func setBytes(b *testing.B) {
	b.SetBytes(int64(BenchmarkRows * BenchmarkColumns * unsafe.Sizeof(gumshoe.Cell(0))))
}
