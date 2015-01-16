package gumshoe

import (
	"testing"

	"util"

	. "github.com/cespare/a"
)

func TestSerializeRowMap(t *testing.T) {
	db := &DB{
		Schema: &Schema{
			TimestampColumn:  Column(makeMetricColumn("at", "uint32")),
			DimensionColumns: []DimensionColumn{makeDimensionColumn("dim1", "uint8", false)},
			MetricColumns:    []MetricColumn{makeMetricColumn("metric1", "uint8")},
		},
	}
	db.Schema.Initialize()

	badRows := []RowMap{
		{"at": "three", "dim1": "string1", "metric1": 1.2},                  // bad type
		{"at": 0.0, "dim1": "string1", "metric2": 1.2},                      // bad column
		{"at": 0.0, "dim1": "string1", "metric1": 1.2, "unknownColumn": 10}, // extra column
	}
	for _, row := range badRows {
		_, err := db.serializeRowMap(row)
		Assert(t, err, NotNil)
	}

	// Nils and 0s should be inserted automatically
	row, err := db.serializeRowMap(RowMap{"at": 0.0})
	Assert(t, err, IsNil)
	Assert(t, row.Dimensions.IsNil(0), IsTrue)
	Assert(t, row.Dimensions[db.Schema.NilBytes:], util.DeepConvertibleEquals, []byte{0})
	Assert(t, row.Metrics, util.DeepConvertibleEquals, []byte{0})

	// Other values should be encoded directly
	row, err = db.serializeRowMap(RowMap{"at": 0.0, "dim1": 1.0, "metric1": 1.0})
	Assert(t, err, IsNil)
	Assert(t, row.Dimensions.IsNil(0), IsFalse)
	Assert(t, row.Dimensions[db.Schema.NilBytes:], util.DeepConvertibleEquals, []byte{1})
	Assert(t, row.Metrics, util.DeepConvertibleEquals, []byte{1})
}
