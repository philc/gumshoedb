package gumshoe

import (
	"fmt"
	"time"

	"github.com/philc/gumshoedb/internal/github.com/dustin/go-humanize"
)

type Column struct {
	Type  Type
	Name  string
	Width int
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
	SegmentSize      int
	IntervalDuration time.Duration

	DiskBacked bool   `json:"-"`
	Dir        string `json:"-"` // Path to persist a DB

	// NOTE(caleb) the runtime configuration options are technically not part of the "schema" but we'll keep
	// them here for convenience.
	RunConfig `json:"-"`

	// All other fields are reconstructed from persisted fields
	DimensionNameToIndex map[string]int `json:"-"`
	MetricNameToIndex    map[string]int `json:"-"`
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

type RunConfig struct {
	FixedRetention   bool          // Whether to truncate old data
	Retention        time.Duration // How long to save data if FixedRetention is true
	QueryParallelism int
}

// Initialize fills in the derived fields of s.
func (s *Schema) Initialize() {
	s.RunConfig.fillDefaults()

	s.DimensionNameToIndex = make(map[string]int)
	s.MetricNameToIndex = make(map[string]int)

	// We need enough nil bytes to accomodate one bit per dimension column.
	s.NilBytes = (len(s.DimensionColumns)-1)/8 + 1
	s.DimensionStartOffset = countColumnWidth
	s.DimensionOffsets = make([]int, len(s.DimensionColumns))
	s.DimensionWidth = s.NilBytes
	offset := s.NilBytes
	for i, col := range s.DimensionColumns {
		s.DimensionNameToIndex[col.Name] = i
		s.DimensionOffsets[i] = offset
		s.DimensionWidth += col.Width
		offset += col.Width
	}

	s.MetricStartOffset = s.DimensionStartOffset + offset
	s.MetricOffsets = make([]int, len(s.MetricColumns))
	s.MetricWidth = 0
	offset = 0
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

// fillDefaults sets fields of c to reasonable default values if they are currently set to the zero value for
// the type.
func (c *RunConfig) fillDefaults() {
	if c.Retention <= 0 {
		c.Retention = 7 * 24 * time.Hour
	}
	if c.QueryParallelism == 0 {
		c.QueryParallelism = 2
	}
}

// Equivalent returns an error describing a difference between the json-public fields of s and other or nil if
// they match.
func (s *Schema) Equivalent(other *Schema) (err error) {
	defer func() {
		if err != nil {
			err = fmt.Errorf("Schemas do not match: %s", err)
		}
	}()

	if s.TimestampColumn != other.TimestampColumn {
		return fmt.Errorf("expected timestamp column %v; got %v",
			s.TimestampColumn, other.TimestampColumn)
	}
	if len(s.DimensionColumns) != len(other.DimensionColumns) {
		return fmt.Errorf("expected %d dimension columns; got %d",
			len(s.DimensionColumns), len(other.DimensionColumns))
	}
	for i, col := range s.DimensionColumns {
		if col != other.DimensionColumns[i] {
			return fmt.Errorf("expected dimension column at index %d to be %v; got %v",
				i, col, other.DimensionColumns[i])
		}
	}
	for i, col := range s.MetricColumns {
		if col != other.MetricColumns[i] {
			return fmt.Errorf("expected metric column at index %d to be %v; got %v",
				i, col, other.MetricColumns[i])
		}
	}
	if s.SegmentSize != other.SegmentSize {
		return fmt.Errorf("expected segment size of %s; got %s",
			humanize.Bytes(uint64(s.SegmentSize)), humanize.Bytes(uint64(other.SegmentSize)))
	}
	return nil
}
