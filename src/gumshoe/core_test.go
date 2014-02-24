package gumshoe

import (
	"testing"

	. "github.com/cespare/a"
)

func tableFixture() *FactTable {
	schema := NewSchema()
	schema.NumericColumns = map[string]int{"col1": TypeFloat32}
	schema.StringColumns = map[string]int{"col2": TypeFloat32}
	return NewFactTable("", 3, schema)
}

func TestConvertRowMapToRowArrayThrowsErrorForUnrecognizedColumn(t *testing.T) {
	_, err := tableFixture().convertRowMapToRowArray(RowMap{"col1": 5, "unknownColumn": 10})
	Assert(t, err, NotNil)
}
