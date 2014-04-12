package gumshoe

import (
	"testing"

	"utils"

	. "github.com/cespare/a"
)

func TestCompressionFactorWorks(t *testing.T) {
	db := makeTestDB()
	for i := 0; i < 4; i++ {
		insertRow(db, RowMap{"at": 0.0, "dim1": "string1", "metric1": 1.0})
	}
	Assert(t, db.GetDebugRows(), utils.DeepConvertibleEquals, []UnpackedRow{
		{RowMap: RowMap{"at": 0, "dim1": "string1", "metric1": 4}, Count: 4},
	})
	Assert(t, db.GetCompressionRatio(), Equals, 4.0)
}
