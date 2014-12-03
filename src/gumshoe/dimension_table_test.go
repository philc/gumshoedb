package gumshoe

import (
	"os"
	"path/filepath"
	"testing"

	"util"

	. "github.com/cespare/a"
)

func TestDimensionTablesArePersisted(t *testing.T) {
	db := makeTestPersistentDB()
	defer os.RemoveAll(db.Dir)

	insertRows(db, []RowMap{
		{"at": 0.0, "dim1": "a", "metric1": 3.0},
		{"at": 0.0, "dim1": "b", "metric1": 7.0},
	})
	expected := []RowMap{
		{"dim1": "a", "metric1": 3, "rowCount": 1},
		{"dim1": "b", "metric1": 7, "rowCount": 1},
	}
	result := runWithGroupBy(db, QueryGrouping{TimeTruncationNone, "dim1", "dim1"})
	Assert(t, result, util.DeepEqualsUnordered, expected)

	db = reopenTestDB(db)
	result = runWithGroupBy(db, QueryGrouping{TimeTruncationNone, "dim1", "dim1"})
	Assert(t, result, util.DeepEqualsUnordered, expected)
}

func TestOldDimensionTablesAreDeleted(t *testing.T) {
	db := makeTestPersistentDB()
	defer os.RemoveAll(db.Dir)

	insertRow(db, RowMap{"at": 0.0, "dim1": "a", "metric1": 1.0})
	firstGenDimensionFilename := filepath.Join(db.Dir, "dimension.index0.generation1.gob.gz")
	if _, err := os.Stat(firstGenDimensionFilename); err != nil {
		t.Fatalf("Expected segment file at %s to exist", firstGenDimensionFilename)
	}

	// This doesn't make a new dimension table
	insertRow(db, RowMap{"at": 0.0, "dim1": "a", "metric1": 3.0})
	if _, err := os.Stat(firstGenDimensionFilename); err != nil {
		t.Fatalf("Expected segment file at %s to exist", firstGenDimensionFilename)
	}

	insertRow(db, RowMap{"at": 0.0, "dim1": "b", "metric1": 1.0})
	if _, err := os.Stat(firstGenDimensionFilename); !os.IsNotExist(err) {
		t.Fatalf("Expected segment file at %s to have been deleted", firstGenDimensionFilename)
	}
	secondGenDimensionFilename := filepath.Join(db.Dir, "dimension.index0.generation2.gob.gz")
	if _, err := os.Stat(secondGenDimensionFilename); err != nil {
		t.Fatalf("Expected segment file at %s to exist", secondGenDimensionFilename)
	}
}
