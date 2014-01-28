package gumshoe

import (
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"

	. "github.com/cespare/a"
)

func TestPersistenceEndToEnd(t *testing.T) {
	tempDir, err := ioutil.TempDir("", "gumshoe-persistence-test")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tempDir)
	tableFilePath := filepath.Join(tempDir, "test")

	schema := NewSchema()
	schema.NumericColumns = map[string]int{"col1": TypeUint8, "col2": TypeUint8}
	table := NewFactTable(tableFilePath, 1, schema)
	table.SaveToDisk()
	rowMap := map[string]Untyped{"col1": 12.0, "col2": 34.0}
	table.InsertRowMaps([]map[string]Untyped{rowMap})

	table, err = LoadFactTableFromDisk(tableFilePath)
	Assert(t, err, IsNil)
	Assert(t, table.FilePath, Equals, tableFilePath)
	Assert(t, rowMap, HasEqualJSON, table.GetRowMaps(0, 1)[0])
}
