// Functions for dealing with rows

package gumshoe

import (
	"encoding/json"
	"fmt"
	"math"
	"time"
	"unsafe"
)

type Untyped interface{}

type Type int

// Avoiding 64-bit types for now to lower the probability of overflow.
// TODO(caleb): This is only a concern for (u)int64, and seems pretty minor. MaxInt64 is huge so even adding
// together lots of int64 columns will probably not overflow for typical GumshoeDB usage; seems like just
// documenting the possibility of overflow is enough.
const (
	TypeUint8 Type = iota
	TypeInt8
	TypeUint16
	TypeInt16
	TypeUint32
	TypeInt32
	TypeFloat32
)

var typeWidths = []int{
	TypeUint8:   int(unsafe.Sizeof(uint8(0))),
	TypeInt8:    int(unsafe.Sizeof(int8(0))),
	TypeUint16:  int(unsafe.Sizeof(uint16(0))),
	TypeInt16:   int(unsafe.Sizeof(int16(0))),
	TypeUint32:  int(unsafe.Sizeof(uint32(0))),
	TypeInt32:   int(unsafe.Sizeof(int32(0))),
	TypeFloat32: int(unsafe.Sizeof(float32(0))),
}

var typeMaxes = []float64{
	TypeUint8:   math.MaxUint8,
	TypeInt8:    math.MaxInt8,
	TypeUint16:  math.MaxUint16,
	TypeInt16:   math.MaxInt16,
	TypeUint32:  math.MaxUint32,
	TypeInt32:   math.MaxInt32,
	TypeFloat32: math.MaxFloat32,
}

var typeNames = []string{
	TypeUint8:   "uint8",
	TypeInt8:    "int8",
	TypeUint16:  "uint16",
	TypeInt16:   "int16",
	TypeUint32:  "uint32",
	TypeInt32:   "int32",
	TypeFloat32: "float32",
}

// NameToType is exported for use in config
var NameToType = make(map[string]Type)

func init() {
	for typ, name := range typeNames {
		NameToType[name] = Type(typ)
	}
}

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

// add adds other to m (only m is modified).
func (m MetricBytes) add(s *Schema, other MetricBytes) {
	p1 := uintptr(unsafe.Pointer(&m[0]))
	p2 := uintptr(unsafe.Pointer(&other[0]))
	for i, column := range s.MetricColumns {
		offset := uintptr(s.MetricOffsets[i])
		col1 := unsafe.Pointer(p1 + offset)
		col2 := unsafe.Pointer(p2 + offset)
		switch column.Type {
		case TypeUint8:
			*(*uint8)(col1) = *(*uint8)(col1) + (*(*uint8)(col2))
		case TypeInt8:
			*(*int8)(col1) = *(*int8)(col1) + (*(*int8)(col2))
		case TypeUint16:
			*(*uint16)(col1) = *(*uint16)(col1) + (*(*uint16)(col2))
		case TypeInt16:
			*(*int16)(col1) = *(*int16)(col1) + (*(*int16)(col2))
		case TypeUint32:
			*(*uint32)(col1) = *(*uint32)(col1) + (*(*uint32)(col2))
		case TypeInt32:
			*(*int32)(col1) = *(*int32)(col1) + (*(*int32)(col2))
		case TypeFloat32:
			*(*float32)(col1) = *(*float32)(col1) + (*(*float32)(col2))
		}
	}
}

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
			dimValueIndex += int32(len(db.State.DimensionTables[index].Values))
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

func setRowValue(pos unsafe.Pointer, typ Type, value float64) {
	switch typ {
	case TypeUint8:
		*(*uint8)(pos) = uint8(value)
	case TypeInt8:
		*(*int8)(pos) = int8(value)
	case TypeUint16:
		*(*uint16)(pos) = uint16(value)
	case TypeInt16:
		*(*int16)(pos) = int16(value)
	case TypeUint32:
		*(*uint32)(pos) = uint32(value)
	case TypeInt32:
		*(*int32)(pos) = int32(value)
	case TypeFloat32:
		*(*float32)(pos) = float32(value)
	}
}

// serializeRowMap takes a RowMap (in the form from deserialized JSON -- in particular, with numbers as
// floats) and maps each key to the appropriate column (including adding new entries to the memTable's
// dimension tables). Note that this should only be called from the inserter goroutine.
func (db *DB) serializeRowMap(rowMap RowMap) (*insertionRow, error) {
	timestampColumnName := db.TimestampColumn.Name
	timestamp, ok := rowMap[timestampColumnName]
	if !ok {
		return nil, fmt.Errorf("row must have a value for the timestamp column (%s)", timestampColumnName)
	}
	timestampMillis, ok := timestamp.(float64)
	if !ok {
		return nil, fmt.Errorf("timestamp column must have an integer value (%s)", timestampColumnName)
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

// UntypedToFloat64 converts u to a float, if it is some int or float type. Otherwise, it panics.
func UntypedToFloat64(u Untyped) float64 {
	switch n := u.(type) {
	case uint32:
		return float64(n)
	}
	panic("Unimplemented")
}

// UntypedToInt converts u to an int, if it is some integer type. Otherwise, it panics.
func UntypedToInt(u Untyped) int {
	switch n := u.(type) {
	case uint32:
		return int(n)
	}
	panic("Unimplemented")
}
