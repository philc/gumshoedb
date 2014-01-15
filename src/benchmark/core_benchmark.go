// Benchmarks which use the gumshoe core code paths in a representative way. These numbers should be compared
// to the ideal, simplified synthetic benchmarks to find areas for improvement.
package main

import (
	"fmt"
	"os"
	"runtime/pprof"

	"gumshoe"
)

func setupFactTable() *gumshoe.FactTable {
	columnNames := make([]string, 0, gumshoe.COLS)
	for i := 0; i < gumshoe.COLS; i++ {
		columnNames = append(columnNames, fmt.Sprintf("column%d", i))
	}
	return gumshoe.NewFactTable("./db/benchmark", columnNames)
}

func populateTableWithTestingData(table *gumshoe.FactTable) {
	rows := make([]map[string]gumshoe.Untyped, 0, BENCHMARK_ROWS)

	for i := 0; i < BENCHMARK_ROWS; i++ {
		row := make(map[string]gumshoe.Untyped, table.ColumnCount)
		for j := 0; j < table.ColumnCount; j++ {
			row[table.ColumnIndexToName[j]] = i % 10
		}
		rows = append(rows, row)
	}

	if err := table.InsertRowMaps(rows); err != nil {
		panic(err)
	}
}

// Creates a QueryAggregate structure which represents the sums of the given columns.
func createQueryAggregates(columns []string) []gumshoe.QueryAggregate {
	queryAggregates := make([]gumshoe.QueryAggregate, 0, len(columns))
	for _, column := range columns {
		queryAggregates = append(queryAggregates, gumshoe.QueryAggregate{"sum", column, column})
	}
	return queryAggregates
}

// A query which only sums aggregates.
func runAggregateQuery(table *gumshoe.FactTable) {
	query := &gumshoe.Query{
		"tableName",
		createQueryAggregates([]string{"column1"}),
		nil,
		nil}
	if err := gumshoe.ValidateQuery(table, query); err != nil {
		panic(err)
	}
	table.InvokeQuery(query)
}

// A query which filters rows by a single, simple filter function.
func runFilterQuery(table *gumshoe.FactTable) {
	query := &gumshoe.Query{
		"tableName",
		createQueryAggregates([]string{"column1"}),
		nil,
		[]gumshoe.QueryFilter{{">", "column2", 5}}}
	if err := gumshoe.ValidateQuery(table, query); err != nil {
		panic(err)
	}
	table.InvokeQuery(query)
}

// A query which groups by a column. Each column has 10 possible values, so the result set will contain 10 row
// aggregates.
func runGroupByQuery(table *gumshoe.FactTable) {
	query := &gumshoe.Query{
		"tableName",
		createQueryAggregates([]string{"column1"}),
		[]gumshoe.QueryGrouping{{"", "column2", "column2"}},
		nil}
	if err := gumshoe.ValidateQuery(table, query); err != nil {
		panic(err)
	}
	table.InvokeQuery(query)
}

// A query which groups by a column that is transformed using a time transform function.
func runGroupByWithTimeTransformQuery(table *gumshoe.FactTable) {
	query := &gumshoe.Query{
		"tableName",
		createQueryAggregates([]string{"column1"}),
		[]gumshoe.QueryGrouping{{"hour", "column2", "column2"}},
		nil}
	if err := gumshoe.ValidateQuery(table, query); err != nil {
		panic(err)
	}
	table.InvokeQuery(query)
}

// Run benchmarks which exercise the core query pipeline in a representative way.
func runCoreBenchmarks(flags BenchmarkFlags) {
	table := setupFactTable()
	populateTableWithTestingData(table)

	profileFilename := *flags.cpuprofile
	if profileFilename != "" {
		fmt.Println("Profiling enabled and will be written to ", profileFilename)
		outputFile, _ := os.Create(profileFilename)
		pprof.StartCPUProfile(outputFile)
		defer pprof.StopCPUProfile()
	}

	runBenchmarkFunction("aggregateQuery", func() { runAggregateQuery(table) })
	runBenchmarkFunction("filterQuery", func() { runFilterQuery(table) })
	runBenchmarkFunction("groupByQuery", func() { runGroupByQuery(table) })
	runBenchmarkFunction("groupByWithTimeTransformQuery", func() { runGroupByWithTimeTransformQuery(table) })
}
