// Functions for dealing with rows

package gumshoe

import (
	"encoding/json"
	"fmt"
	"time"
	"unsafe"
)

type Untyped interface{}

func (t Type) MarshalJSON() ([]byte, error) { return []byte(fmt.Sprintf("%q", typeNames[t])), nil }

func (t *Type) UnmarshalJSON(b []byte) error {
	var s string
	if err := json.Unmarshal(b, &s); err != nil {
		return err
	}
	if typ, ok := NameToType[s]; ok {
		*t = typ
		return nil
	}
	return fmt.Errorf("%q is an invalid gumshoedb type", s)
}

const countColumnWidth = int(unsafe.Sizeof(uint32(0)))

// RowBytes is the serialized form of an entire row (count, DimensionBytes, MetricBytes).
type RowBytes []byte

// DimensionBytes is the serialized form of the dimensions of a single GumshoeDB row (nil bytes followed by
// dimension columns).
type DimensionBytes []byte

// MetricBytes is the serialized form of the metrics of a single GumshoeDB row.
type MetricBytes []byte

func (d DimensionBytes) setNil(index int) { d[index/8] |= 1 << byte(index%8) }

// count retrieves a row's count (the number of collapsed logical rows).
func (r RowBytes) count(s *Schema) uint32 { return *(*uint32)(unsafe.Pointer(&r[0])) }

func (db *DB) setDimensionValue(dimensions DimensionBytes, index int, value Untyped) error {
	column := db.DimensionColumns[index]

	if value == nil {
		dimensions.setNil(index)
		return nil
	}

	if column.String {
		stringValue, ok := value.(string)
		if !ok {
			return fmt.Errorf("expected string value for dimension %s", column.Name)
		}
		dimValueIndex, ok := db.State.DimensionTables[index].Get(stringValue)
		if !ok {
			dimValueIndex, _ = db.memTable.DimensionTables[index].GetAndMaybeSet(stringValue)
			// The index in a memtable's dimension table must be offset by the size of the state's dimension table
			// (with which it will be later combined).
			dimValueIndex += uint32(len(db.State.DimensionTables[index].Values))
		}
		if float64(dimValueIndex) > typeMaxes[column.Type] {
			return fmt.Errorf("adding a new value (%v) to dimension %s overflows the dimension table",
				value, column.Name)
		}
		setRowValue(unsafe.Pointer(&dimensions[db.DimensionOffsets[index]]), column.Type, float64(dimValueIndex))
		return nil
	}

	float, ok := value.(float64)
	if !ok {
		return fmt.Errorf("expected numeric value for dimension %s", column.Name)
	}
	if float > typeMaxes[column.Type] {
		return fmt.Errorf("value %v too large for dimension %s", value, column.Name)
	}
	setRowValue(unsafe.Pointer(&dimensions[db.DimensionOffsets[index]]), column.Type, float)
	return nil
}

func (db *DB) setMetricValue(metrics MetricBytes, index int, value Untyped) error {
	column := db.MetricColumns[index]
	float, ok := value.(float64)
	if !ok {
		return fmt.Errorf("expected numeric value for metric %s", column.Name)
	}
	if float > typeMaxes[column.Type] {
		return fmt.Errorf("value %v too large for column %s", value, column.Name)
	}
	setRowValue(unsafe.Pointer(&metrics[db.MetricOffsets[index]]), column.Type, value.(float64))
	return nil
}

// serializeRowMap takes a RowMap (in the form from deserialized JSON -- in particular, with numbers as
// floats) and maps each key to the appropriate column (including adding new entries to the memTable's
// dimension tables). Note that this should only be called from the inserter goroutine.
func (db *DB) serializeRowMap(rowMap RowMap) (*insertionRow, error) {
	timestampColumnName := db.TimestampColumn.Name
	timestamp, ok := rowMap[timestampColumnName]
	if !ok {
		return nil, fmt.Errorf("row must have a value for the timestamp column (%q)", timestampColumnName)
	}
	timestampMillis, ok := timestamp.(float64)
	if !ok {
		return nil, fmt.Errorf("timestamp column (%q) must have an numeric value", timestampColumnName)
	}
	dimensions := make(DimensionBytes, db.DimensionWidth)
	for i, dimCol := range db.DimensionColumns {
		value, ok := rowMap[dimCol.Name]
		if !ok {
			return nil, fmt.Errorf("missing dimension column %s", dimCol.Name)
		}
		if err := db.setDimensionValue(dimensions, i, value); err != nil {
			return nil, err
		}
	}
	metrics := make(MetricBytes, db.MetricWidth)
	for i, metricCol := range db.MetricColumns {
		value, ok := rowMap[metricCol.Name]
		if !ok {
			return nil, fmt.Errorf("missing metric column %s", metricCol.Name)
		}
		if err := db.setMetricValue(metrics, i, value); err != nil {
			return nil, err
		}
	}

	row := &insertionRow{
		Timestamp:  time.Unix(int64(timestampMillis), 0),
		Dimensions: dimensions,
		Metrics:    metrics,
	}
	return row, nil
}
