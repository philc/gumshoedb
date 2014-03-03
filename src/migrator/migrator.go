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

/*
NOTE(dmac): These are rough guidelines for the future implementor of allowing migrations to shrink columns:

1. For string values, determine the list of values that will fit into the new column. This can be the first N
   from the dimension table, or a hand-picked list if, for example, we want to keep the N highest-traffic apps.

2. If a value can be safely downcast, do so. If it can't, throw an error and abort unless an override flag is
   present.

3. When a downcast is necessary, if the value is out of range, insert null instead of the value.
*/

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
		log.Fatal("Unable to parse config: ", err)
	}

	log.Println("Loading table from", *oldTablePath)
	oldTable, err := gumshoe.LoadFactTableFromDisk(*oldTablePath)
	if err != nil {
		log.Fatal("Unable to load table from disk: ", err)
	}
	log.Println("Loaded", oldTable.Count, "rows")

	log.Print("Generating new fact table")
	newTable := gumshoe.NewFactTable(*newTablePath, conf.ToSchema())
	newTable.SaveToDisk()

	copyOldDataToNewTable(oldTable, newTable)
	log.Println("Migration complete")
}

func copyOldDataToNewTable(oldTable *gumshoe.FactTable, newTable *gumshoe.FactTable) {
	newColumnNames := subtractColumnNames(newTable, oldTable)
	deletedColumnNames := subtractColumnNames(oldTable, newTable)
	log.Println("Adding columns:", newColumnNames)
	log.Println("Deleting columns:", deletedColumnNames)
	chunkSize := 1000000
	for start := 0; start <= oldTable.Count; start += chunkSize {
		end := start + chunkSize
		if end > oldTable.Count {
			end = oldTable.Count
		}
		log.Printf("Migrating data from rows %d to %d", start, end)
		rows := oldTable.GetRowMaps(start, end)
		prepareRows(rows, newColumnNames, deletedColumnNames, newTable.Schema)
		err := newTable.InsertRowMaps(rows)
		if err != nil {
			log.Fatal("Error encountered when inserting rows: ", err)
		}
	}
	newTable.SaveToDisk()
}

// Returns columns in table1 that are not in table2.
func subtractColumnNames(table1 *gumshoe.FactTable, table2 *gumshoe.FactTable) []string {
	columns := make([]string, 0)
	for column := range table1.ColumnNameToIndex {
		if _, ok := table2.ColumnNameToIndex[column]; !ok {
			columns = append(columns, column)
		}
	}
	return columns
}

func prepareRows(rows []gumshoe.RowMap, newColumnNames []string, deletedColumnNames []string,
	newSchema *gumshoe.Schema) {
	for _, row := range rows {
		for _, column := range newColumnNames {
			// The default value for dimension columns is nil. For metric columns (which are not nullable), it's 0.
			_, ok := newSchema.DimensionColumns[column]
			if ok {
				row[column] = nil
			} else {
				row[column] = 0.0
			}
		}
		for _, column := range deletedColumnNames {
			delete(row, column)
		}
		// TODO: This switch is necessary because table.InsertRowMaps(table.GetRowMaps(0, 1)) panics on non-string,
		// non-float64 types. Ideally, table.InsertRowMaps would not panic if an unexpected numeric type can be
		// safely coerced.
		for column, value := range row {
			switch value.(type) {
			case uint8:
				row[column] = float64(value.(uint8))
			case int8:
				row[column] = float64(value.(int8))
			case uint16:
				row[column] = float64(value.(uint16))
			case int16:
				row[column] = float64(value.(int16))
			case uint32:
				row[column] = float64(value.(uint32))
			case int32:
				row[column] = float64(value.(int32))
			case float32:
				row[column] = float64(value.(float32))
			}
		}
	}
}
