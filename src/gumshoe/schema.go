package gumshoe

import (
	"errors"
	"fmt"
	"time"
)

type Column struct {
	Type  Type
	Name  string
	Width int
	String bool
}

type MetricColumn Column

func MakeMetricColumn(name, typeString string) (MetricColumn, error) {
	typ, ok := NameToType[typeString]
	if !ok {
		return MetricColumn{}, fmt.Errorf("bad type: %s", typeString)
	}
	return MetricColumn{Type: typ, Name: name, Width: typeWidths[typ]}, nil
}

type DimensionColumn struct {
	Column
	String bool
}

func MakeDimensionColumn(name, typeString string, isString bool) (DimensionColumn, error) {
	typ, ok := NameToType[typeString]
	if !ok {
		return DimensionColumn{}, fmt.Errorf("bad type: %s", typeString)
	}
	return DimensionColumn{
		Column: Column{Type: typ, Name: name, Width: typeWidths[typ]},
		String: isString,
	}, nil
}

type Schema struct {
	TimestampColumn  Column
	DimensionColumns []DimensionColumn
	MetricColumns    []MetricColumn
	SegmentSize      int // A soft limit (segments are truncated after they exceed this size)

	Dir           string        `json:"-"` // Path to persist a DB
	FlushDuration time.Duration `json:"-"` // How frequently to flush the memtable

	// All other fields are reconstructed from persisted fields
	DimensionNameToIndex map[string]int
	MetricNameToIndex    map[string]int
	// Row is: count | nil bytes | dim1 | dim2 | ... | dimN | metric1 | metric2 | ... | metricN
	//                <----------- DimensionBytes ----------><--------- MetricBytes ---------->
	DimensionStartOffset int   `json:"-"`
	DimensionOffsets     []int `json:"-"` // From DimensionStartOffset
	DimensionWidth       int   `json:"-"`
	MetricStartOffset    int   `json:"-"`
	MetricOffsets        []int `json:"-"` // From MetricStartOffset
	MetricWidth          int   `json:"-"`
	NilBytes             int   `json:"-"`
	RowSize              int   `json:"-"`
}

// initialize fills in the derived fields of s.
func (s *Schema) initialize() {
	s.DimensionNameToIndex = make(map[string]int)
	s.MetricNameToIndex = make(map[string]int)

	// We need enough nil bytes to accomodate one bit per dimension column.
	s.NilBytes = (len(s.DimensionColumns)-1)/8 + 1
	s.DimensionStartOffset = countColumnWidth

	offset := s.NilBytes
	s.DimensionOffsets = make([]int, len(s.DimensionColumns))
	s.DimensionWidth = s.NilBytes
	for i, col := range s.DimensionColumns {
		s.DimensionNameToIndex[col.Name] = i
		s.DimensionOffsets[i] = offset
		s.DimensionWidth += col.Width
		offset += col.Width
	}
	s.MetricStartOffset = s.DimensionStartOffset + offset
	offset = 0
	s.MetricOffsets = make([]int, len(s.MetricColumns))
	for i, col := range s.MetricColumns {
		s.MetricNameToIndex[col.Name] = i
		s.MetricOffsets[i] = offset
		s.MetricWidth += col.Width
		offset += col.Width
	}

	// Total row width includes count byte, nil bytes, dimension columns, and metric columns.
	s.RowSize = countColumnWidth + s.NilBytes
	for _, col := range s.DimensionColumns {
		s.RowSize += col.Width
	}
	for _, col := range s.MetricColumns {
		s.RowSize += col.Width
	}
}

// Equivalent returns an error describing a difference between the json-public fields of s and other or nil if
// they match.
func (s *Schema) Equivalent(other *Schema) (err error) {
	defer func() {
		if err != nil {
			err = fmt.Errorf("Expected schema: %#v; got: %#v", s, other)
		}
	}()

	err = errors.New("")
	if s.TimestampColumn != other.TimestampColumn {
		return err
	}
	if len(s.DimensionColumns) != len(other.DimensionColumns) {
		return err
	}
	for i, col := range s.DimensionColumns {
		if col != other.DimensionColumns[i] {
			return err
		}
	}
	for i, col := range s.MetricColumns {
		if col != other.MetricColumns[i] {
			return err
		}
	}
	if s.SegmentSize != other.SegmentSize {
		return err
	}
	return nil
}