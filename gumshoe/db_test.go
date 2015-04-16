package gumshoe

import (
	"io/ioutil"
	"os"
	"testing"
)

func TestDoubleOpen(t *testing.T) {
	schema := schemaFixture()
	tempDir, err := ioutil.TempDir("", "gumshoedb-db-test")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tempDir)
	schema.Dir = tempDir
	schema.DiskBacked = true
	db, err := NewDB(schema)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if _, err := OpenDB(schema); err == nil {
		t.Fatal("Expected error double-opening database dir")
	}
}
