package main

import (
	"testing"
	"time"

	"gumshoe"
	"utils"

	"github.com/cespare/a"
)

func TestMigrateSameSchema(t *testing.T) {
	schema := &migrateTestSchema{
		[]migrateTestDimensions{{"dim1", "uint32", false}},
		[]migrateTestMetrics{{"metric1", "uint32"}},
	}

	var rows []gumshoe.RowMap
	// Rows are 13 bytes wide; 100 bytes / segments => 7 rows / segment
	// Make two different intervals with two segments each.
	for _, at := range []float64{0.0, float64(1000 * 60 * 60)} {
		for i := 0; i < 10; i++ {
			rows = append(rows, gumshoe.RowMap{"at": at, "dim1": float64(i), "metric1": 9999.0})
		}
	}

	runMigrateTestCase(t, &migrateTestCase{
		OldSchema:    schema,
		NewSchema:    schema,
		InsertRows:   rows,
		ExpectedRows: makeUnpackedSingleRows(rows),
	})
}

func TestMigrateAddRemoveColumns(t *testing.T) {
	oldSchema := &migrateTestSchema{
		[]migrateTestDimensions{{"dim1", "uint32", true}, {"dim2", "uint32", true}},
		[]migrateTestMetrics{{"metric1", "uint32"}, {"metric2", "uint32"}},
	}
	newSchema := &migrateTestSchema{
		[]migrateTestDimensions{{"dim2", "uint32", true}, {"dim3", "uint32", true}},
		[]migrateTestMetrics{{"metric2", "uint32"}, {"metric3", "uint32"}},
	}

	runMigrateTestCase(t, &migrateTestCase{
		OldSchema:  oldSchema,
		NewSchema:  newSchema,
		InsertRows: []gumshoe.RowMap{{"at": 0.0, "dim1": "d1", "dim2": "d2", "metric1": 1.0, "metric2": 2.0}},
		ExpectedRows: []gumshoe.UnpackedRow{
			{gumshoe.RowMap{"at": 0.0, "dim2": "d2", "dim3": nil, "metric2": 2.0, "metric3": 0.0}, 1},
		},
	})
}

type migrateTestDimensions struct {
	Name   string
	Type   string
	String bool
}

type migrateTestMetrics struct {
	Name string
	Type string
}

type migrateTestSchema struct {
	Dimensions []migrateTestDimensions
	Metrics    []migrateTestMetrics
}

type migrateTestCase struct {
	OldSchema    *migrateTestSchema
	NewSchema    *migrateTestSchema
	InsertRows   []gumshoe.RowMap
	ExpectedRows []gumshoe.UnpackedRow
}

func makeUnpackedSingleRows(rows []gumshoe.RowMap) []gumshoe.UnpackedRow {
	unpackedRows := make([]gumshoe.UnpackedRow, len(rows))
	for i, row := range rows {
		unpackedRows[i] = gumshoe.UnpackedRow{row, 1}
	}
	return unpackedRows
}

func runMigrateTestCase(t *testing.T, testCase *migrateTestCase) {
	oldDB, err := gumshoe.NewDB(schemaFixture(testCase.OldSchema))
	if err != nil {
		t.Fatal(err)
	}
	defer oldDB.Close()
	newDB, err := gumshoe.NewDB(schemaFixture(testCase.NewSchema))
	if err != nil {
		t.Fatal(err)
	}
	defer newDB.Close()

	if err := oldDB.Insert(testCase.InsertRows); err != nil {
		t.Fatal(err)
	}
	if err := oldDB.Flush(); err != nil {
		t.Fatal(err)
	}

	if err := migrateDBs(newDB, oldDB, 4, 10); err != nil {
		t.Fatal(err)
	}

	a.Assert(t, testCase.ExpectedRows, utils.DeepConvertibleEquals, newDB.GetDebugRows())
}

func schemaFixture(testSchema *migrateTestSchema) *gumshoe.Schema {
	atColumn, err := gumshoe.MakeMetricColumn("at", "uint32")
	if err != nil {
		panic(err)
	}
	schema := &gumshoe.Schema{
		TimestampColumn:  gumshoe.Column(atColumn),
		SegmentSize:      100,
		IntervalDuration: time.Hour,
	}
	for _, dimension := range testSchema.Dimensions {
		dimColumn, err := gumshoe.MakeDimensionColumn(dimension.Name, dimension.Type, dimension.String)
		if err != nil {
			panic(err)
		}
		schema.DimensionColumns = append(schema.DimensionColumns, dimColumn)
	}
	for _, metric := range testSchema.Metrics {
		metricColumn, err := gumshoe.MakeMetricColumn(metric.Name, metric.Type)
		if err != nil {
			panic(err)
		}
		schema.MetricColumns = append(schema.MetricColumns, metricColumn)
	}
	return schema
}
