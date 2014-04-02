// +build ignore

package gumshoe_test

import (
	"fmt"
	"math"
	"math/rand"
	"testing"

	"gumshoe"
)

const (
	numDimensions = 30
	numMetrics    = 10
	numTableRows  = 1e6
	numInsertRows = 1e4 // This is the size of the pool we pull from to insert (wrapping around if necessary)

	// This many of the dimensions will vary; the others are constant
	varyingDimensions = 2
)

var distinctValuesPerVaryingDimension = int(math.Pow(numInsertRows, (1.0 / varyingDimensions)))

func BenchmarkInsertion(b *testing.B) {
	schema := gumshoe.NewSchema()
	for i := 0; i < numDimensions; i++ {
		schema.DimensionColumns[dimColumn(i)] = gumshoe.TypeUint16
	}
	for i := 0; i < numMetrics; i++ {
		schema.MetricColumns[metricColumn(i)] = gumshoe.TypeUint32
	}
	schema.TimestampColumn = "at"
	table := gumshoe.NewFactTable("", schema)

	// Generate the fixed rows the table will start with
	fixedRows := nRandomRows(numInsertRows)
	normalizedRows := make([][]byte, len(fixedRows))
	for i := range normalizedRows {
		rowBytes, err := table.NormalizeRow(fixedRows[i])
		if err != nil {
			b.Fatal(err)
		}
		normalizedRows[i] = rowBytes
	}

	// Insert the fixed rows into the DB
	for i := 0; i < numTableRows; i++ {
		if err := table.InsertNormalizedRowNoCollapse(normalizedRows[i%len(normalizedRows)], 0); err != nil {
			b.Fatal(err)
		}
	}

	// Generate the test rows to insert during the benchmark
	rows := nRandomRows(numInsertRows)

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		if err := table.InsertRowMaps([]gumshoe.RowMap{rows[i%len(rows)]}); err != nil {
			b.Fatal(err)
		}
	}
}

func dimColumn(i int) string    { return fmt.Sprintf("dim%02d", i) }
func metricColumn(i int) string { return fmt.Sprintf("metric%02d", i) }

func nRandomRows(n int) []gumshoe.RowMap {
	results := make([]gumshoe.RowMap, n)
	for i := 0; i < n; i++ {
		results[i] = randomRow()
	}
	return results
}

func randomRow() gumshoe.RowMap {
	row := gumshoe.RowMap{"at": 0}
	for j := 0; j < varyingDimensions; j++ {
		value := rand.Intn(distinctValuesPerVaryingDimension)
		if value > math.MaxUint16 {
			panic("dimension value too large")
		}
		row[dimColumn(j)] = float64(value)
	}
	for j := varyingDimensions; j < numDimensions; j++ {
		row[dimColumn(j)] = 10.0
	}
	for j := 0; j < numMetrics; j++ {
		row[metricColumn(j)] = float64(rand.Intn(100))
	}
	return row
}
