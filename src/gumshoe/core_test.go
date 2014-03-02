package gumshoe

import (
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

func TestConvertRowMapToRowArrayThrowsErrorForUnrecognizedColumn(t *testing.T) {
	_, err := tableFixture().convertRowMapToRowArray(
		RowMap{"at": 0, "dim1": "string1", "unknownColumn": 10})
	Assert(t, err, NotNil)
}

func TestNewIntervalsAreAllocatedAsNeeded(t *testing.T) {
	table := tableFixture()
	table.SegmentSizeInBytes = 12
	Assert(t, len(table.Intervals), Equals, 0)
	// This require cause a new Interval and two new segments to be allocated.
	err := table.InsertRowMaps([]RowMap{
		{"at": 0, "dim1": "a", "metric1": 1.0},
		{"at": 0, "dim1": "b", "metric1": 2.0}})
	Assert(t, err, Equals, nil)
	Assert(t, len(table.Intervals), Equals, 1)
	Assert(t, len(table.Intervals[0].Segments), Equals, 2)
}
