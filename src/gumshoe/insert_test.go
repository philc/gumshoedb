package gumshoe

import (
	"testing"

	"utils"

	. "github.com/cespare/a"
)

func TestExpectedNumberOfSegmentsAreAllocated(t *testing.T) {
	db := testDB()
	defer db.Close()
	db.Schema.SegmentSize = 32 // Rows are 13 bytes apiece

	insertRows(db, []RowMap{
		{"at": 0.0, "dim1": "a", "metric1": 1.0},
		{"at": 0.0, "dim1": "b", "metric1": 1.0},
		{"at": 0.0, "dim1": "c", "metric1": 1.0},
	})

	resp := db.makeRequest()
	defer resp.Done()

	Assert(t, len(resp.State.Intervals), Equals, 1)
	numSegments := 0
	for _, interval := range resp.State.Intervals {
		numSegments += interval.NumSegments
	}
	Assert(t, numSegments, Equals, 2)
}

func physicalRows(db *DB) int {
	resp := db.makeRequest()
	defer resp.Done()
	rows := 0
	for _, interval := range resp.State.Intervals {
		rows += interval.NumRows
	}
	return rows
}

func TestRowsGetCollapsedUponInsertion(t *testing.T) {
	db := testDB()
	defer db.Close()

	// These two rows should be collapsed
	insertRows(db, []RowMap{
		{"at": 0.0, "dim1": "string1", "metric1": 1.0},
		{"at": 0.0, "dim1": "string1", "metric1": 3.0},
	})
	Assert(t, physicalRows(db), Equals, 1)

	// This row should not, because it has a nil column.
	insertRow(db, RowMap{"at": 0.0, "dim1": nil, "metric1": 5.0})
	Assert(t, physicalRows(db), Equals, 2)

	// This row should not be collapsed with the others, because it falls in a different time interval.
	insertRow(db, RowMap{"at": hour(2), "dim1": "string1", "metric1": 7.0})
	Assert(t, physicalRows(db), Equals, 3)
}

func TestCompressionFactorWorks(t *testing.T) {
	db := testDB()
	for i := 0; i < 4; i++ {
		insertRow(db, RowMap{"at": 0.0, "dim1": "string1", "metric1": 1.0})
	}
	Assert(t, physicalRows(db), Equals, 1)
	Assert(t, db.GetCompressionRatio(), Equals, 4.0)
}

func TestInsertAndReadNilValues(t *testing.T) {
	db := testDB()
	insertRows(db, []RowMap{
		{"at": hour(0), "dim1": "a", "metric1": 0.0},
		{"at": hour(1), "dim1": nil, "metric1": 1.0},
	})
	results := runWithGroupBy(db, QueryGrouping{TimeTruncationNone, "dim1", "dim1"})
	Assert(t, results, utils.HasEqualJSON, []RowMap{
		{"dim1": "a", "metric1": 0.0, "rowCount": 1.0},
		{"dim1": nil, "metric1": 1.0, "rowCount": 1.0},
	})
}
