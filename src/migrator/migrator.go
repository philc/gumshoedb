package main

import (
	"flag"
	"log"
	"time"

	"config"
	"gumshoe"

	"github.com/BurntSushi/toml"
)

// Migrator currently handles adding new columns and deleting old columns. It will also happily modify
// column sizes (e.g., int16 -> int32), but has no special handling or safety for lossy type changes.

func main() {
	oldTablePath := flag.String("old-table", "db/table", "Old table file to migrate")
	newTablePath := flag.String("new-table", "db/new-table", "New table file to be generated")
	configFile := flag.String("config", "config.toml", "Config file for new table")
	flag.Parse()

	conf := &config.Config{
		TableFilePath: "db/table",
		SaveDuration:  config.Duration{10 * time.Second},
	}

	if _, err := toml.DecodeFile(*configFile, conf); err != nil {
		log.Fatal(err)
	}

	log.Println("Loading table from " + *oldTablePath)
	oldTable, err := gumshoe.LoadFactTableFromDisk(*oldTablePath)
	if err != nil {
		panic(err)
	}
	log.Println("Loaded", oldTable.Count, "rows")

	log.Print("Generating new fact table")
	newTable := gumshoe.NewFactTable(*newTablePath, conf.Rows, conf.ToSchema())
	newTable.SaveToDisk()

	copyOldDataToNewTable(oldTable, newTable)
	log.Println("Migration complete")
}

func copyOldDataToNewTable(oldTable *gumshoe.FactTable, newTable *gumshoe.FactTable) {
	newColumnNames := getNewColumnNames(oldTable, newTable)
	deletedColumnNames := getDeletedColumnNames(oldTable, newTable)
	log.Println("Adding columns:", newColumnNames)
	log.Println("Deleting columns:", deletedColumnNames)
	for _, row := range oldTable.GetRowMaps(0, oldTable.Count) {
		prepareRow(row, newColumnNames, deletedColumnNames)
		err := newTable.InsertRowMaps([]map[string]gumshoe.Untyped{row})
		if err != nil {
			panic(err)
		}
	}
	newTable.SaveToDisk()
}

func getNewColumnNames(oldTable *gumshoe.FactTable, newTable *gumshoe.FactTable) []string {
	newColumnNames := make([]string, 0)
	for columnName, _ := range newTable.ColumnNameToIndex {
		_, ok := oldTable.ColumnNameToIndex[columnName]
		if !ok {
			newColumnNames = append(newColumnNames, columnName)
		}
	}
	return newColumnNames
}

func getDeletedColumnNames(oldTable *gumshoe.FactTable, newTable *gumshoe.FactTable) []string {
	deletedColumnNames := make([]string, 0)
	for columnName, _ := range oldTable.ColumnNameToIndex {
		_, ok := newTable.ColumnNameToIndex[columnName]
		if !ok {
			deletedColumnNames = append(deletedColumnNames, columnName)
		}
	}
	return deletedColumnNames
}

func prepareRow(row map[string]gumshoe.Untyped, newColumnNames []string, deletedColumnNames []string) {
	for _, newColumnName := range newColumnNames {
		row[newColumnName] = nil
	}
	for _, deletedColumnName := range deletedColumnNames {
		delete(row, deletedColumnName)
	}
	// TODO: This switch is necessary because table.InsertRowMaps(table.GetRowMaps(0, 1)) panics on non-string,
	// non-float64 types. Ideally, table.InsertRowMaps would not panic if an unexpected numeric type can be
	// safely coerced.
	for columnName, value := range row {
		switch value.(type) {
		case uint8:
			row[columnName] = float64(value.(uint8))
		case int8:
			row[columnName] = float64(value.(int8))
		case uint16:
			row[columnName] = float64(value.(uint16))
		case int16:
			row[columnName] = float64(value.(int16))
		case uint32:
			row[columnName] = float64(value.(uint32))
		case int32:
			row[columnName] = float64(value.(int32))
		case float32:
			row[columnName] = float64(value.(float32))
		}
	}
}
