package gumshoe

import "time"

func schemaFixture() *Schema {
	return &Schema{
		TimestampColumn:  makeColumn("at", "uint32"),
		DimensionColumns: []DimensionColumn{makeDimensionColumn("dim1", "uint32", true)},
		MetricColumns:    []MetricColumn{makeMetricColumn("metric1", "uint32")},
		SegmentSize:      1 << 10,
		Dir:              "",
		FlushDuration:    time.Minute,
	}
}

func testDB() *DB {
	db, err := Open(schemaFixture())
	if err != nil {
		panic(err)
	}
	return db
}

func insertRow(db *DB, at float64, dimensionValue, metricValue Untyped) {
	row := RowMap{"at": at, "dim1": dimensionValue, "metric1": metricValue}
	if err := db.Insert([]RowMap{row}); err != nil {
		panic(err)
	}
}

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
