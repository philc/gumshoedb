package main

import (
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"

	"gumshoe"
	"utils"

	. "github.com/cespare/a"
)

func createTableWithNumericColumns(tableFilePath string, numericColumns map[string]int) *gumshoe.FactTable {
	schema := gumshoe.NewSchema()
	schema.NumericColumns = numericColumns
	schema.TimestampColumn = "at"
	table := gumshoe.NewFactTable(tableFilePath, schema)
	table.SegmentSizeInBytes = 1000 * 10
	table.SaveToDisk()
	return table
}

func TestMigrationAddColumn(t *testing.T) {
	tempDir, err := ioutil.TempDir("", "gumshoe-migration-test")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tempDir)

	oldTablePath := filepath.Join(tempDir, "old")
	oldTable := createTableWithNumericColumns(oldTablePath, map[string]int{"col1": gumshoe.TypeUint8})
	oldRowMap := gumshoe.RowMap{"at": 0, "col1": 1.0}
	oldTable.InsertRowMaps([]gumshoe.RowMap{oldRowMap})

	newTablePath := filepath.Join(tempDir, "new")
	newTable := createTableWithNumericColumns(newTablePath,
		map[string]int{"col1": gumshoe.TypeUint8, "col2": gumshoe.TypeUint8})

	copyOldDataToNewTable(oldTable, newTable)

	newTable, err = gumshoe.LoadFactTableFromDisk(newTablePath)
	newRowMap := gumshoe.RowMap{"at": 0, "col1": 1.0, "col2": nil}
	Assert(t, err, IsNil)
	Assert(t, newRowMap, utils.HasEqualJSON, newTable.GetRowMaps(0, 1)[0])
}

func TestMigrationDeleteColumn(t *testing.T) {
	tempDir, err := ioutil.TempDir("", "gumshoe-migration-test")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tempDir)

	oldTableFilePath := filepath.Join(tempDir, "old")
	oldTable := createTableWithNumericColumns(oldTableFilePath,
		map[string]int{"col1": gumshoe.TypeUint8, "col2": gumshoe.TypeUint8})
	oldRowMap := gumshoe.RowMap{"at": 0, "col1": 1.0, "col2": 2.0}
	oldTable.InsertRowMaps([]gumshoe.RowMap{oldRowMap})

	newTableFilePath := filepath.Join(tempDir, "new")
	newTable := createTableWithNumericColumns(newTableFilePath, map[string]int{"col2": gumshoe.TypeUint8})

	copyOldDataToNewTable(oldTable, newTable)

	newTable, err = gumshoe.LoadFactTableFromDisk(newTableFilePath)
	newRowMap := gumshoe.RowMap{"at": 0, "col2": 2.0}
	Assert(t, err, IsNil)
	Assert(t, newRowMap, utils.HasEqualJSON, newTable.GetRowMaps(0, 1)[0])
}

func TestInsertAfterMigrateWorks(t *testing.T) {
	tempDir, err := ioutil.TempDir("", "gumshoe-migration-test")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tempDir)

	oldTableFilePath := filepath.Join(tempDir, "old")
	oldTable := createTableWithNumericColumns(oldTableFilePath, map[string]int{"col1": gumshoe.TypeUint8})
	oldRowMaps := []gumshoe.RowMap{
		{"at": 0, "col1": 1.0},
		{"at": 0, "col1": 2.0},
		{"at": 0, "col1": 3.0}}
	oldTable.InsertRowMaps(oldRowMaps)

	newTableFilePath := filepath.Join(tempDir, "new")
	newTable := createTableWithNumericColumns(newTableFilePath, map[string]int{"col1": gumshoe.TypeUint8})

	copyOldDataToNewTable(oldTable, newTable)

	newTable, err = gumshoe.LoadFactTableFromDisk(newTableFilePath)
	newRowMaps := []gumshoe.RowMap{{"at": 0, "col1": 3.0}, {"at": 0, "col1": 4.0}}
	newTable.InsertRowMaps(newRowMaps)
	expected := []gumshoe.RowMap{}
	expected = append(expected, oldRowMaps...)
	expected = append(expected, newRowMaps...)
	Assert(t, newTable.GetRowMaps(0, len(expected)), utils.HasEqualJSON, expected)
}
