package main

import (
	"encoding/json"
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"

	"gumshoe"

	. "github.com/cespare/a"
)

func convertToJSONAndBack(o interface{}) interface{} {
	b, err := json.Marshal(o)
	if err != nil {
		panic(err)
	}
	result := new(interface{})
	json.Unmarshal(b, result)
	return *result
}

func HasEqualJSON(args ...interface{}) (ok bool, message string) {
	o1 := convertToJSONAndBack(args[0])
	o2 := convertToJSONAndBack(args[1])
	return DeepEquals(o1, o2)
}

func createTableWithNumericColumns(tableFilePath string, numericColumns map[string]int) *gumshoe.FactTable {
	schema := gumshoe.NewSchema()
	schema.NumericColumns = numericColumns
	table := gumshoe.NewFactTable(tableFilePath, 1, schema)
	table.SaveToDisk()
	return table
}

func TestMigrationAddColumn(t *testing.T) {
	tempDir, err := ioutil.TempDir("", "gumshoe-migration-test")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tempDir)

	oldTableFilePath := filepath.Join(tempDir, "old")
	oldTable := createTableWithNumericColumns(oldTableFilePath, map[string]int{"col1": gumshoe.TypeUint8})
	oldRowMap := map[string]gumshoe.Untyped{"col1": 1.0}
	oldTable.InsertRowMaps([]map[string]gumshoe.Untyped{oldRowMap})

	newTableFilePath := filepath.Join(tempDir, "new")
	newTable := createTableWithNumericColumns(newTableFilePath,
		map[string]int{"col1": gumshoe.TypeUint8, "col2": gumshoe.TypeUint8})

	copyOldDataToNewTable(oldTable, newTable)

	newTable, err = gumshoe.LoadFactTableFromDisk(newTableFilePath)
	newRowMap := map[string]gumshoe.Untyped{"col1": 1.0, "col2": nil}
	Assert(t, err, IsNil)
	Assert(t, newRowMap, HasEqualJSON, newTable.GetRowMaps(0, 1)[0])
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
	oldRowMap := map[string]gumshoe.Untyped{"col1": 1.0, "col2": 2.0}
	oldTable.InsertRowMaps([]map[string]gumshoe.Untyped{oldRowMap})

	newTableFilePath := filepath.Join(tempDir, "new")
	newTable := createTableWithNumericColumns(newTableFilePath, map[string]int{"col2": gumshoe.TypeUint8})

	copyOldDataToNewTable(oldTable, newTable)

	newTable, err = gumshoe.LoadFactTableFromDisk(newTableFilePath)
	newRowMap := map[string]gumshoe.Untyped{"col2": 2.0}
	Assert(t, err, IsNil)
	Assert(t, newRowMap, HasEqualJSON, newTable.GetRowMaps(0, 1)[0])
}
