package gumshoe

import (
	"strings"
	"testing"

	. "github.com/cespare/a"
)

func tableFixture() *FactTable {
	schema := NewSchema()
	schema.DimensionColumns = map[string]int{"dim1": TypeInt32}
	schema.MetricColumns = map[string]int{"metric1": TypeInt32}
	schema.StringColumns = []string{"dim1"}
	schema.TimestampColumn = "at"
	return NewFactTable("", schema)
}

// The offset in seconds for the given number of hours. This is used to succinctly express rows which should
// fall within different time intervals.
func hour(n int) int {
	return n * 60 * 60
}

func insertRow(table *FactTable, at int, dimensionValue Untyped, metricValue Untyped) {
	table.InsertRowMaps([]RowMap{{"at": at, "dim1": dimensionValue, "metric1": metricValue}})
}

func TestConvertRowMapToRowSliceThrowsErrorForUnrecognizedColumn(t *testing.T) {
	_, err := tableFixture().convertRowMapToRowSlice(
		RowMap{"at": 0, "dim1": "string1", "unknownColumn": 10})
	Assert(t, err, NotNil)
}

func TestNewIntervalsAreAllocatedAsNeeded(t *testing.T) {
	table := tableFixture()
	table.SegmentSizeInBytes = 16
	Assert(t, len(table.Intervals), Equals, 0)
	// This require cause a new Interval and two new segments to be allocated.
	err := table.InsertRowMaps([]RowMap{
		{"at": 0, "dim1": "a", "metric1": 1.0},
		{"at": 0, "dim1": "b", "metric1": 2.0}})
	Assert(t, err, IsNil)
	Assert(t, len(table.Intervals), Equals, 1)
	Assert(t, len(table.Intervals[0].Segments), Equals, 2)
}

func TestNilMetricColumnsAreRejected(t *testing.T) {
	table := tableFixture()
	err := table.InsertRowMaps([]RowMap{{"at": 0, "dim1": "string1", "metric1": nil}})
	Assert(t, strings.Contains(err.Error(), "Metric columns cannot be nil"), IsTrue)
}

func TestRowsGetCollapsedUponInsertion(t *testing.T) {
	t.Skip()
	table := tableFixture()
	// These two rows should be collapsed.
	table.InsertRowMaps([]RowMap{{"at": 0, "dim1": "string1", "metric1": 1.0}})
	table.InsertRowMaps([]RowMap{{"at": 0, "dim1": "string1", "metric1": 3.0}})
	Assert(t, table.GetRowMaps(0, 1)[0]["metric1"], Equals, int32(4))

	// This row should not, because it has a nil column.
	table.InsertRowMaps([]RowMap{{"at": 0, "dim1": nil, "metric1": 5.0}})
	Assert(t, table.GetRowMaps(1, 2)[0]["metric1"], Equals, int32(5))

	// This row should not be collapsed with the others, because it falls in a different time interval.
	table.InsertRowMaps([]RowMap{{"at": 60 * 60, "dim1": "string1", "metric1": 7.0}})
	Assert(t, table.GetRowMaps(2, 3)[0]["metric1"], Equals, int32(7))
}

func TestCompressionFactorWorks(t *testing.T) {
	t.Skip()
	table := tableFixture()
	for i := 0; i < 4; i++ {
		table.InsertRowMaps([]RowMap{{"at": 0, "dim1": "string1", "metric1": 1.0}})
	}
	Assert(t, table.GetRowMaps(0, 1)[0]["metric1"], Equals, int32(4))
	// 4 rows are collapsed into 1 row, so that's a 0.75 compression factor.
	Assert(t, table.CompressionFactor(), Equals, 0.75)
}

func TestInsertAndReadNilValues(t *testing.T) {
	table := tableFixture()
	insertRow(table, hour(0), "a", 0.0)
	insertRow(table, hour(1), nil, 1.0)
	results := table.GetRowMaps(0, table.Count)
	Assert(t, results[0]["dim1"], Equals, "a")
	Assert(t, results[0]["metric1"], Equals, int32(0))
	Assert(t, results[1]["dim1"], Equals, nil)
	Assert(t, results[1]["metric1"].(int32), Equals, int32(1))
}
