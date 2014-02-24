package gumshoe

import (
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"

	"utils"

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
	schema.TimestampColumn = "at"
	schema.NumericColumns = map[string]int{"col1": TypeUint8}
	table := NewFactTable(tableFilePath, 1, schema)

	rowMap := RowMap{"at": 0, "col1": 12.0}
	if err = table.InsertRowMaps([]RowMap{rowMap}); err != nil {
		t.Fatal(err)
	}
	table.SaveToDisk()
	table, err = LoadFactTableFromDisk(tableFilePath)
	Assert(t, err, IsNil)
	Assert(t, table.FilePath, Equals, tableFilePath)
	Assert(t, table.GetRowMaps(0, 1)[0], utils.HasEqualJSON, rowMap)
}
