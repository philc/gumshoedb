package gumshoe

import (
	"testing"

	"github.com/philc/gumshoedb/internal/util"

	. "github.com/philc/gumshoedb/internal/github.com/cespare/a"
)

func TestCompressionFactorWorks(t *testing.T) {
	db := makeTestDB()
	for i := 0; i < 4; i++ {
		insertRow(db, RowMap{"at": 0.0, "dim1": "string1", "metric1": 1.0})
	}
	Assert(t, db.GetDebugRows(), util.DeepConvertibleEquals, []UnpackedRow{
		{RowMap: RowMap{"at": 0, "dim1": "string1", "metric1": 4}, Count: 4},
	})
	stats := db.GetDebugStats()
	Assert(t, stats.CompressionRatio, Equals, 4.0)
}
