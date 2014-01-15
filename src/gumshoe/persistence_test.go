package gumshoe

import (
	. "github.com/cespare/a"
	"os"
	"testing"
)

const testingFolder = "/tmp/gumshoedb_tests"

func TestPersistenceEndToEnd(t *testing.T) {
	tableFilePath := testingFolder + "/test"
	os.RemoveAll(testingFolder)
	os.Mkdir(testingFolder, os.ModeDir|0700)

	table := NewFactTable(tableFilePath, []string{"col1", "col2"})
	table.SaveToDisk()
	rowMap := map[string]Untyped{"col1": 12, "col2": 34}
	table.InsertRowMaps([]map[string]Untyped{rowMap})

	table = LoadFactTableFromDisk(tableFilePath)
	Assert(t, table.FilePath, Equals, tableFilePath)
	denormalizedRow := table.DenormalizeRow(&table.Rows()[0])

	Assert(t, int(denormalizedRow["col1"].(Cell)), Equals, int(rowMap["col1"].(int)))
}
