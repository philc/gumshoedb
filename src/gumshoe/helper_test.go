package gumshoe

import "time"

func schemaFixture() *Schema {
	return &Schema{
		TimestampColumn:  makeColumn("at", "uint32"),
		DimensionColumns: []DimensionColumn{makeDimensionColumn("dim1", "uint32", true)},
		MetricColumns:    []MetricColumn{makeMetricColumn("metric1", "uint32")},
		SegmentSize:      1 << 10,
		Dir:              "",
		IntervalDuration: time.Hour,
	}
}

func makeTestDB() *DB {
	db, err := Open(schemaFixture())
	if err != nil {
		panic(err)
	}
	return db
}

func closeTestDB(db *DB) {
	if err := db.Close(); err != nil {
		panic(err)
	}
}

func insertRows(db *DB, rows []RowMap) {
	if err := db.Insert(rows); err != nil {
		panic(err)
	}
	if err := db.Flush(); err != nil {
		panic(err)
	}
}

func insertRow(db *DB, row RowMap) { insertRows(db, []RowMap{row}) }

// hour returns the offset in seconds for the given number of hours. This is used to succinctly express rows
// which should fall within different time intervals.
func hour(n int) float64 { return float64(n * 60 * 60) }

func makeColumn(name, typeString string) Column {
	return Column(makeMetricColumn(name, typeString))
}

func makeMetricColumn(name, typeString string) MetricColumn {
	m, err := MakeMetricColumn(name, typeString)
	if err != nil {
		panic(err)
	}
	return m
}

func makeDimensionColumn(name, typeString string, isString bool) DimensionColumn {
	d, err := MakeDimensionColumn(name, typeString, isString)
	if err != nil {
		panic(err)
	}
	return d
}
