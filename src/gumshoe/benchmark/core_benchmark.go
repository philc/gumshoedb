// Benchmarks which use the gumshoe core in a representative way.
package main

import "fmt"
import core "gumshoe/core"
import "runtime/pprof"
import "os"

const BENCHMARK_ROWS = 100000 // The row count to use in these benchmarks.

func setupFactTable() *core.FactTable {
	columnNames := make([]string, 0, core.COLS)
	for i := 0; i < core.COLS; i++ {
		columnNames = append(columnNames, fmt.Sprintf("column%d", i))
	}
	return core.NewFactTable(columnNames)
}

// Populates a table with representative test data. The columns are named "column1", "column2".
func populateTableWithTestingData(table *core.FactTable) {
	rows := make([]map[string]core.Untyped, 0, BENCHMARK_ROWS)

	for i := 0; i < BENCHMARK_ROWS; i++ {
		row := make(map[string]core.Untyped, table.ColumnCount)
		for j := 0; j < table.ColumnCount; j++ {
			row[table.ColumnIndexToName[j]] = i % 10
		}
		rows = append(rows, row)
	}

	error := core.InsertRowMaps(table, rows)
	if error != nil {
		panic(error)
	}
}

// Creates a QueryAggregate structure which will define the sums of the given columns.
func createQueryAggregates(columns []string) []core.QueryAggregate {
	queryAggregates := make([]core.QueryAggregate, 0, len(columns))
	for _, column := range columns {
		queryAggregates = append(queryAggregates, core.QueryAggregate{"sum", column, column})
	}
	return queryAggregates
}

// Runs a query which only sums aggregates.
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

	core.InvokeQuery(table, query)
}

// These exercise the main core query pipeline in a representative way.
func runCoreBenchmarks(flags BenchmarkFlags) {
	table := setupFactTable()
	profileFilename := *flags.cpuprofile
	if profileFilename != "" {
		fmt.Println("Profiling enabled and will be written to ", profileFilename)
		outputFile, _ := os.Create(profileFilename)
		pprof.StartCPUProfile(outputFile)
		defer pprof.StopCPUProfile()
	}

	populateTableWithTestingData(table)
	runBenchmarkFunction("aggregateQuery", func() { runAggregateQuery(table) })
}
