package gumshoe

import (
	"testing"

	. "github.com/cespare/a"
)

func TestSerializeRowMapReturnsErrorForBadRows(t *testing.T) {
	db := testDB()
	defer db.Close()

	badRows := []RowMap{
		{"at": "three", "dim1": "string1", "metric1": 1.2},                  // bad type
		{"at": 0.0, "dim1": "string1"},                                      // missing column
		{"at": 0.0, "dim1": "string1", "metric1": 1.2, "unknownColumn": 10}, // extra column
	}
	for _, row := range badRows {
		_, err := db.serializeRowMap(row)
		Assert(t, err, NotNil)
	}
}
