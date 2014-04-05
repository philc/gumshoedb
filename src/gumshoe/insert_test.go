package gumshoe

import (
	"io/ioutil"
	"os"
	"strconv"
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

func TestPersistenceEndToEnd(t *testing.T) {
	tempDir, err := ioutil.TempDir("", "gumshoe-persistence-test")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tempDir)

	schema := schemaFixture()
	schema.Dir = tempDir
	schema.SegmentSize = 100
	db, err := Open(schema)
	if err != nil {
		t.Fatal(err)
	}

	// Insert a bunch of data
	var rows []RowMap
	for i := 0; i < 10000; i++ {
		// Generate 100 unique rows.
		rows = append(rows, RowMap{"at": 0.0, "dim1": strconv.Itoa(i % 100), "metric1": 1.0})
	}
	if err := db.Insert(rows); err != nil {
		t.Fatal(err)
	}
	db.Flush()

	// Query the data
	Assert(t, physicalRows(db), Equals, 100)
	result := runQuery(db, createQuery())
	Assert(t, result[0]["metric1"], Equals, uint32(10000))

	// Reopen the DB and try again
	db.Close()
	db, err = Open(schema)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	Assert(t, physicalRows(db), Equals, 100)
	result = runQuery(db, createQuery())
	Assert(t, result[0]["metric1"], Equals, uint32(10000))
}
