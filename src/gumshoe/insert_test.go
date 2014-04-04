package gumshoe

import (
	"testing"

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
