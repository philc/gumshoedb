// Benchmarks which use the gumshoe core code paths in a representative way. These numbers should be compared
// to the ideal, simplified synthetic benchmarks to find areas for improvement.
package main

import "fmt"
import core "gumshoe/core"
import "runtime/pprof"
import "os"

func setupFactTable() *core.FactTable {
	columnNames := make([]string, 0, core.COLS)
	for i := 0; i < core.COLS; i++ {
		columnNames = append(columnNames, fmt.Sprintf("column%d", i))
	}
	return core.NewFactTable("./benchmark", columnNames)
}

func populateTableWithTestingData(table *core.FactTable) {
	rows := make([]map[string]core.Untyped, 0, BENCHMARK_ROWS)

	for i := 0; i < BENCHMARK_ROWS; i++ {
		row := make(map[string]core.Untyped, table.ColumnCount)
		for j := 0; j < table.ColumnCount; j++ {
			row[table.ColumnIndexToName[j]] = i % 10
		}
		rows = append(rows, row)
	}

	error := table.InsertRowMaps(rows)
	if error != nil {
		panic(error)
	}
}

// Creates a QueryAggregate structure which represents the sums of the given columns.
func createQueryAggregates(columns []string) []core.QueryAggregate {
	queryAggregates := make([]core.QueryAggregate, 0, len(columns))
	for _, column := range columns {
		queryAggregates = append(queryAggregates, core.QueryAggregate{"sum", column, column})
	}
	return queryAggregates
}

// A query which only sums aggregates.
func runAggregateQuery(table *core.FactTable) {
	query := &core.Query{
		"tableName",
		createQueryAggregates([]string{"column1"}),
		nil,
		nil}
	error := core.ValidateQuery(table, query)
	if error != nil {
		panic(error)
	}
	table.InvokeQuery(query)
}

// A query which filters rows by a single, simple filter function.
func runFilterQuery(table *core.FactTable) {
	query := &core.Query{
		"tableName",
		createQueryAggregates([]string{"column1"}),
		nil,
		[]core.QueryFilter{{">", "column2", 5}}}
	error := core.ValidateQuery(table, query)
	if error != nil {
		panic(error)
	}
	table.InvokeQuery(query)
}

// A query which groups by a column. Each column has 10 possible values, so the result set will contain 10 row
// aggregates.
func runGroupByQuery(table *core.FactTable) {
	query := &core.Query{
		"tableName",
		createQueryAggregates([]string{"column1"}),
		[]core.QueryGrouping{{"", "column2", "column2"}},
		nil}
	error := core.ValidateQuery(table, query)
	if error != nil {
		panic(error)
	}
	table.InvokeQuery(query)
}

// A query which groups by a column that is transformed using a time transform function.
func runGroupByWithTimeTransformQuery(table *core.FactTable) {
	query := &core.Query{
		"tableName",
		createQueryAggregates([]string{"column1"}),
		[]core.QueryGrouping{{"hour", "column2", "column2"}},
		nil}
	error := core.ValidateQuery(table, query)
	if error != nil {
		panic(error)
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
