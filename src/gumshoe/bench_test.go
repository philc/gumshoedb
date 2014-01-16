// Benchmarks which use the gumshoe core code paths in a representative way. These numbers should be compared
// to the ideal, simplified synthetic benchmarks to find areas for improvement.
package gumshoe_test

import (
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"

	"gumshoe"
)

const BenchmarkRows = 100000 // The row count to use in these benchmarks.

func init() {
	if BenchmarkRows > gumshoe.ROWS {
		panic("BenchmarkRows is larger than gumshoe.ROWS.")
	}
}

// A query which only sums aggregates.
func BenchmarkAggregateQuery(b *testing.B) {
	table, tempDir := setupFactTable()
	defer os.RemoveAll(tempDir)
	query := createQuery(nil, nil)
	if err := gumshoe.ValidateQuery(table, query); err != nil {
		b.Fatal(err)
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		table.InvokeQuery(query)
	}
}

// A query which filters rows by a single, simple filter function.
func BenchmarkFilterQuery(b *testing.B) {
	table, tempDir := setupFactTable()
	defer os.RemoveAll(tempDir)
	query := createQuery(nil, []gumshoe.QueryFilter{{">", "column2", 5}})
	if err := gumshoe.ValidateQuery(table, query); err != nil {
		b.Fatal(err)
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		table.InvokeQuery(query)
	}
}

// A query which groups by a column. Each column has 10 possible values, so the result set will contain 10 row
// aggregates.
func BenchmarkGroupByQuery(b *testing.B) {
	table, tempDir := setupFactTable()
	defer os.RemoveAll(tempDir)
	query := createQuery([]gumshoe.QueryGrouping{{"", "column2", "column2"}}, nil)
	if err := gumshoe.ValidateQuery(table, query); err != nil {
		panic(err)
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		table.InvokeQuery(query)
	}
}

// A query which groups by a column that is transformed using a time transform function.
func BenchmarkGroupByWithTimeTransformQuery(b *testing.B) {
	table, tempDir := setupFactTable()
	defer os.RemoveAll(tempDir)
	query := createQuery([]gumshoe.QueryGrouping{{"hour", "column2", "column2"}}, nil)
	if err := gumshoe.ValidateQuery(table, query); err != nil {
		panic(err)
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		table.InvokeQuery(query)
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

func setupFactTable() (table *gumshoe.FactTable, dbTempDir string) {
	columnNames := make([]string, gumshoe.COLS)
	for i := range columnNames {
		columnNames[i] = fmt.Sprintf("column%d", i)
	}
	// Create a temporary directory for the benchmarking db file.
	tempDir, err := ioutil.TempDir("", "gumshoe-benchmark-")
	if err != nil {
		panic(err)
	}
	table = gumshoe.NewFactTable(filepath.Join(tempDir, "db"), columnNames)
	populateTableWithTestingData(table)
	return table, tempDir
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