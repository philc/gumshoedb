package gumshoe

import (
	"testing"

	. "github.com/cespare/a"
)

func tableFixture() *FactTable {
	schema := NewSchema()
	schema.NumericColumns = map[string]int{"col1": TypeFloat32}
	schema.StringColumns = map[string]int{"col2": TypeFloat32}
	schema.TimestampColumn = "at"
	return NewFactTable("", schema)
}

func TestConvertRowMapToRowArrayThrowsErrorForUnrecognizedColumn(t *testing.T) {
	_, err := tableFixture().convertRowMapToRowArray(
		RowMap{"at": 0, "col1": 5.0, "col2": "a", "unknownColumn": 10})
	Assert(t, err, NotNil)
}

func TestNewIntervalsAreAllocatedAsNeeded(t *testing.T) {
	table := tableFixture()
	table.SegmentSizeInBytes = 12
	Assert(t, len(table.Intervals), Equals, 0)
	// This require cause a new Interval and two new segments to be allocated.
	err := table.InsertRowMaps([]RowMap{
		{"at": 0, "col1": 1.0, "col2": "a"},
		{"at": 0, "col1": 2.0, "col2": "b"}})
	Assert(t, err, Equals, nil)
	Assert(t, len(table.Intervals), Equals, 1)
	Assert(t, len(table.Intervals[0].Segments), Equals, 2)
}
