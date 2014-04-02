package gumshoe

import (
	"time"

	"b"
)

type MemInterval struct {
	Start time.Time // Inclusive
	End   time.Time // Exclusive
	*b.Tree
}

type MemTable struct {
	*Schema
	Intervals       map[time.Time]*MemInterval
	DimensionTables []*DimensionTable
}

func NewMemTable(schema *Schema) *MemTable {
	return &MemTable{
		Schema:          schema,
		Intervals:       make(map[time.Time]*MemInterval),
		DimensionTables: NewDimensionTablesForSchema(schema),
	}
}
