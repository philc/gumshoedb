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

	table := NewFactTable(tableFilePath, 1, []string{"col1", "col2"})
	table.SaveToDisk()
	rowMap := map[string]Untyped{"col1": 12, "col2": 34}
	table.InsertRowMaps([]map[string]Untyped{rowMap})

	table = LoadFactTableFromDisk(tableFilePath)
	Assert(t, table.FilePath, Equals, tableFilePath)
	denormalizedRow := table.DenormalizeRow(table.getRowSlice(0))

	Assert(t, int(denormalizedRow["col1"].(Cell)), Equals, int(rowMap["col1"].(int)))
}
