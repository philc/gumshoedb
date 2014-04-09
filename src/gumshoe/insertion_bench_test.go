package gumshoe_test

import (
	"fmt"
	"math"
	"math/rand"
	"os"
	"testing"
	"time"

	"gumshoe"
)

const (
	numDimensions = 30
	numMetrics    = 10
	numInsertRows = 1e4 // This is the size of the pool we pull from to insert (wrapping around if necessary)

	// Change this to a disk-backed temp dir to test spinning disk performance
	tempDirName = "/tmp/gumshoedb-test"

	// This many of the dimensions will vary; the others are constant
	varyingDimensions = 2
)

var distinctValuesPerVaryingDimension = int(math.Pow(numInsertRows, (1.0 / varyingDimensions)))

func BenchmarkInsertion(b *testing.B) {
	var err error
	dimensions := make([]gumshoe.DimensionColumn, numDimensions)
	for i := range dimensions {
		dimensions[i], err = gumshoe.MakeDimensionColumn(dimColumn(i), "uint16", false)
		if err != nil {
			b.Fatal(err)
		}
	}
	metrics := make([]gumshoe.MetricColumn, numMetrics)
	for i := range metrics {
		metrics[i], err = gumshoe.MakeMetricColumn(metricColumn(i), "uint32")
		if err != nil {
			b.Fatal(err)
		}
	}
	timestampColumn, err := gumshoe.MakeDimensionColumn("at", "uint32", false)
	if err != nil {
		b.Fatal(err)
	}
	_ = os.RemoveAll(tempDirName)
	schema := &gumshoe.Schema{
		TimestampColumn:  timestampColumn.Column,
		DimensionColumns: dimensions,
		MetricColumns:    metrics,
		SegmentSize:      5e5, // 500KB
		IntervalDuration: time.Hour,
		Dir:              tempDirName,
	}
	defer os.RemoveAll(tempDirName)

	db, err := gumshoe.Open(schema)
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()

	// Generate the fixed rows the table will start with
	if err := db.Insert(nRandomRows(numInsertRows)); err != nil {
		b.Fatal(err)
	}

	// Generate the test rows to insert during the benchmark
	rows := nRandomRows(numInsertRows)

	b.SetBytes(int64(db.RowSize))
	b.ResetTimer()

	// NOTE(caleb): Flushing every 50k lines (somewhat arbitrarily) and at the end. Note that this could lead
	// to some quirks (steps) in benchmark results. Pay attention to the number of iterations the benchmark
	// runs.
	for i := 0; i < b.N; i++ {
		if err := db.Insert([]gumshoe.RowMap{rows[i%len(rows)]}); err != nil {
			b.Fatal(err)
		}
		if i%50000 == 0 {
			db.Flush()
		}
	}
	db.Flush()
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
	row := gumshoe.RowMap{"at": 0.0}
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
