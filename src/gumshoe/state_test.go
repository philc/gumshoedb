package gumshoe

import (
	"testing"

	. "github.com/cespare/a"
)

func TestCompressionFactorWorks(t *testing.T) {
	db := testDB()
	for i := 0; i < 4; i++ {
		insertRow(db, RowMap{"at": 0.0, "dim1": "string1", "metric1": 1.0})
	}
	Assert(t, physicalRows(db), Equals, 1)
	Assert(t, db.GetCompressionRatio(), Equals, 4.0)
}
