// The table creation and insertion functions.
package gumshoe

import (
	"bytes"
	"fmt"
	"math"
	"reflect"
	"sort"
	"strings"
	"sync"
	"time"
	"unsafe"
)

// Note that the 64 bit types are not supported in schemas at the moment. This is because we serialize column
// values to JSON as float64 and we accumulate values in the scan loop as float64s; if columns themselves are
// > 32 bit, we could corrupt the value when casting it to a float64.
const (
	TypeUint8 = iota
	TypeInt8
	TypeUint16
	TypeInt16
	TypeUint32
	TypeInt32
	TypeFloat32
)

var typeSizes = map[int]int{
	TypeUint8:   int(unsafe.Sizeof(uint8(0))),
	TypeInt8:    int(unsafe.Sizeof(int8(0))),
	TypeUint16:  int(unsafe.Sizeof(uint16(0))),
	TypeInt16:   int(unsafe.Sizeof(int16(0))),
	TypeUint32:  int(unsafe.Sizeof(uint32(0))),
	TypeInt32:   int(unsafe.Sizeof(int32(0))),
	TypeFloat32: int(unsafe.Sizeof(float32(0))),
}

const countColumnSize = 1 // This is a uint8.

// How large to make each segment. Segments may not be precisely this size, because they will be aligned
// to the table's row size.
const defaultSegmentSize = 100e6 // 100MB

type Schema struct {
	DimensionColumns   map[string]int // name => size
	MetricColumns      map[string]int // name => size
	SegmentSizeInBytes int            // How large to make segments. This setting is used mainly by our tests.
	TimestampColumn    string
	StringColumns      []string
}

// TODO(philc): In the future this interval duration will be configurable, and variable (e.g. older data has
// larger granularities, graphite-style).
const intervalDuration = time.Duration(1) * time.Hour

type Interval struct {
	Start            time.Time
	Duration         time.Duration
	NextInsertOffset int
	Segments         [][]byte `json:"-"` // Segments of rows. Each segment can be backed by a memory-mapped file.
	// We maintain this count separately from len(Segments) so that we can serialize it to JSON and use it to
	// rebuild the Segments slice.
	SegmentCount int
}

func NewSchema() *Schema {
	s := new(Schema)
	s.DimensionColumns = make(map[string]int)
	s.MetricColumns = make(map[string]int)
	s.StringColumns = make([]string, 0)
	s.SegmentSizeInBytes = defaultSegmentSize
	return s
}

// A fixed sized table of rows.
// When we insert more rows than the table's capacity, we wrap around and begin inserting rows at index 0.
type FactTable struct {
	// NOTE(philc): map[int]Interval would be more convenient, but it's not JSON serializable.
	Intervals []*Interval
	FilePath  string `json:"-"` // Path to this table on disk, where we will periodically save it.
	// TODO(caleb): This is not enough. Reads still race with writes. We need to fix this, possibly by removing
	// the circular writes and instead persisting historic chunks to disk (or deleting them) and allocating
	// fresh tables.
	InsertLock *sync.Mutex `json:"-"`
	// TODO(philc): Eliminate rows, as the storage is now contained within Intervals.
	rows        []byte
	Count       int // The number of used rows currently in the table.
	ColumnCount int // The number of columns in use in the table.
	RowSize     int // In bytes
	Schema      *Schema
	// A mapping from column index => column's DimensionTable. Dimension tables exist for string columns only.
	DimensionTables       map[string]*DimensionTable
	ColumnNameToIndex     map[string]int
	ColumnIndexToName     []string
	ColumnIndexToOffset   []uintptr // The byte offset of each column from the beggining byte of the row
	DimensionColumnsWidth int       // The combined width of all dimension columns
	ColumnIndexToType     []int     // Index => one of the type constants (e.g. TypeUint8).
	stringColumnsMap      map[string]bool
	TimestampColumnName   string // Name of the column used for grouping rows into time buckets.
	SegmentSizeInBytes    int
}

func (table *FactTable) Rows() []byte {
	return table.rows
}

// A DimensionTable is a mapping of string column values to int IDs, so that the FactTable can store rows of
// integer IDs rather than string values.
type DimensionTable struct {
	Name      string
	Rows      []string
	ValueToId map[string]int32
}

func NewDimensionTable(name string) *DimensionTable {
	return &DimensionTable{
		Name:      name,
		ValueToId: make(map[string]int32),
	}
}

type RowMap map[string]Untyped

type RowAggregate struct {
	GroupByValue float64
	Sums         []float64
	Count        int
}

type FactTableFilterFunc func(uintptr) bool

// Returns the keys from a map in sorted order.
func getSortedKeys(m map[string]int) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

// Allocates a new FactTable. If a non-empty filePath is specified, this table's rows are immediately
// persisted to disk in the form of a memory-mapped file.
// String columns appear first, and then numeric columns, for no particular reason other than
// implementation convenience in a few places.
func NewFactTable(filePath string, schema *Schema) *FactTable {
	dimensionColumnNames := getSortedKeys(schema.DimensionColumns)
	metricColumnNames := getSortedKeys(schema.MetricColumns)
	allColumnNames := append(dimensionColumnNames, metricColumnNames...)
	table := &FactTable{
		ColumnCount: len(allColumnNames),
		FilePath:    filePath,
		InsertLock:  new(sync.Mutex),
	}
	table.TimestampColumnName = schema.TimestampColumn
	table.SegmentSizeInBytes = schema.SegmentSizeInBytes
	table.Schema = schema

	columnToType := make(map[string]int)
	for k, v := range schema.DimensionColumns {
		columnToType[k] = v
	}
	for k, v := range schema.MetricColumns {
		columnToType[k] = v
	}

	// Compute the byte offset from the beginning of the row for each column
	table.ColumnIndexToOffset = make([]uintptr, table.ColumnCount)
	table.ColumnIndexToType = make([]int, table.ColumnCount)
	// Skip space for the nil bytes header and the count header.
	columnOffset := countColumnSize + table.numNilBytes()
	for i, name := range allColumnNames {
		table.ColumnIndexToOffset[i] = uintptr(columnOffset)
		table.ColumnIndexToType[i] = columnToType[name]
		columnOffset += typeSizes[columnToType[name]]
	}
	table.RowSize = columnOffset

	dimensionColumnsWidth := 0
	for _, v := range schema.DimensionColumns {
		dimensionColumnsWidth += typeSizes[v]
	}
	table.DimensionColumnsWidth = dimensionColumnsWidth

	table.ColumnIndexToName = make([]string, len(allColumnNames))
	table.ColumnNameToIndex = make(map[string]int, len(allColumnNames))
	for i, name := range allColumnNames {
		table.ColumnIndexToName[i] = name
		table.ColumnNameToIndex[name] = i
	}

	table.stringColumnsMap = make(map[string]bool, len(schema.StringColumns))
	table.DimensionTables = make(map[string]*DimensionTable, len(schema.StringColumns))
	for _, column := range schema.StringColumns {
		table.stringColumnsMap[column] = true
		table.DimensionTables[column] = NewDimensionTable(column)
	}

	return table
}

// Returns the number of bytes a row requires in order to have one nil bit per column.
func (table *FactTable) numNilBytes() int {
	return int(math.Ceil(float64(table.ColumnCount) / 8.0))
}

// Return a set of row maps. Useful for debugging the contents of the table.
func (table *FactTable) GetRowMaps(start, end int) []RowMap {
	results := make([]RowMap, 0, end-start)
	if start > end {
		panic("Invalid row indices passed to GetRowMaps")
	}
	for i := start; i < end; i++ {
		rowSlice, interval := table.getRowSlice(i)
		row := table.DenormalizeRow(rowSlice)
		// We don't store the timestamp column in the row itself, so add it in, since it's helpful to have it in
		// the row map.
		row[table.TimestampColumnName] = int(interval.Start.Unix()) // Convert to seconds since epoch
		results = append(results, row)
	}
	return results
}

// Given a column value from a row vector, return either the column value if it's a numeric column, or the
// corresponding string if it's a normalized string column.
// E.g. denormalizeColumnValue(213, 1) => "Japan"
func (table *FactTable) denormalizeColumnValue(value Untyped, columnName string) Untyped {
	if value == nil {
		return value
	} else if table.stringColumnsMap[columnName] {
		dimensionTable := table.DimensionTables[columnName]
		return dimensionTable.Rows[int(convertUntypedToFloat64(value))]
	} else {
		return value
	}
}

// Takes a normalized row vector and returns a map consisting of column names and values pulled from the
// dimension tables.
// e.g. [0, 1, 17] => {"country": "Japan", "browser": "Chrome", "age": 17}
func (table *FactTable) DenormalizeRow(row []byte) RowMap {
	result := make(RowMap)
	for i := 0; i < table.ColumnCount; i++ {
		name := table.ColumnIndexToName[i]
		value := table.getColumnValue(row, i)
		result[name] = table.denormalizeColumnValue(value, name)
	}
	return result
}

// Takes a map of column names => values, and returns a vector with the map's values in the correct column
// position according to the table's schema, e.g.:
// e.g. {"country": "Japan", "browser": "Chrome", "age": 17} => ["Chrome", 17, "Japan"]
// Returns an error if there are unrecognized columns, or if a column is missing.
func (table *FactTable) convertRowMapToRowArray(rowMap RowMap) ([]Untyped, error) {
	result := make([]Untyped, table.ColumnCount)
	expectedColumns := len(table.ColumnNameToIndex) + 1 // +1 to account for the timestamp column.
	if len(rowMap) != expectedColumns {
		return nil, fmt.Errorf("This row has %d columns; expected %d: %s", len(rowMap), expectedColumns, rowMap)
	}
	for columnName, value := range rowMap {
		if columnName == table.TimestampColumnName {
			continue
		}
		columnIndex, ok := table.ColumnNameToIndex[columnName]
		if !ok {
			return nil, fmt.Errorf("Unrecognized column name: %s", columnName)
		}
		result[columnIndex] = value
	}
	return result, nil
}

func (table *FactTable) getRowOffset(row int) int {
	return row * table.RowSize
}

func (table *FactTable) getRowSlice(rowIndex int) ([]byte, *Interval) {
	if rowIndex >= table.Count {
		panic(fmt.Sprintf("Row index %d is out of bounds (table size is %d).", rowIndex, table.Count))
	}
	// We search for the Interval and segment which contains the given rowIndex.
	var interval *Interval
	var segment []byte
	offset := 0
	rowLocation := rowIndex * table.RowSize
	for _, interval = range table.Intervals {
		if offset+interval.NextInsertOffset > rowLocation {
			break
		}
		offset += interval.NextInsertOffset
	}

	for _, segment = range interval.Segments {
		if offset+len(segment) > rowLocation {
			break
		}
		offset += len(segment)
	}
	segmentOffset := rowLocation - offset
	slice := segment[segmentOffset : segmentOffset+table.RowSize]
	return slice, interval
}

// Takes a row of mixed types and replaces all string columns with the ID of the matching string in the
// corresponding dimension table (creating a new row in the dimension table if the dimension table doesn't
// already contain the string).
// Note that all numeric values are assumed to be float64 (this is what Go's JSON unmarshaller produces).
// e.g. {"country": "Japan", "browser": "Chrome", "age": 17} => [0, 1, 17]
func (table *FactTable) normalizeRow(rowMap RowMap) ([]byte, error) {
	rowAsArray, err := table.convertRowMapToRowArray(rowMap)
	if err != nil {
		return nil, err
	}
	rowSlice := make([]byte, table.RowSize)
	for columnIndex, value := range rowAsArray {
		columnName := table.ColumnIndexToName[columnIndex]
		if value == nil {
			_, ok := table.Schema.MetricColumns[columnName]
			if ok {
				return nil, fmt.Errorf("Metric columns cannot be nil. %s is nil.", columnName)
			}
			table.setColumnValue(rowSlice, columnIndex, value)
			continue
		}
		var valueAsFloat64 float64
		if table.stringColumnsMap[columnName] {
			if !isString(value) {
				return nil, fmt.Errorf("Cannot insert a non-string value into column %s.", columnName)
			}
			stringValue := value.(string)
			dimensionTable := table.DimensionTables[columnName]
			dimensionRowId, ok := dimensionTable.ValueToId[stringValue]
			if !ok {
				dimensionRowId = dimensionTable.addRow(stringValue)
			}
			valueAsFloat64 = float64(dimensionRowId)
		} else {
			switch value.(type) {
			case float64:
				valueAsFloat64 = value.(float64)
			default:
				return nil, fmt.Errorf("Expected float64 as the type for column %s, but received %s",
					columnName, reflect.TypeOf(value))
			}
		}
		table.setColumnValue(rowSlice, columnIndex, valueAsFloat64)
	}
	return rowSlice, nil
}

func (table *FactTable) columnIsNil(rowPtr uintptr, columnIndex int) bool {
	nilBytePtr := unsafe.Pointer(rowPtr + uintptr(countColumnSize+columnIndex/8))
	nilBitIndex := uint(columnIndex) % 8
	isNil := *(*uint)(nilBytePtr) & (1 << (7 - nilBitIndex))
	return isNil > 0
}

func (table *FactTable) getColumnValue(row []byte, column int) Untyped {
	rowPtr := uintptr(unsafe.Pointer(&row[0]))
	columnPtr := unsafe.Pointer(rowPtr + table.ColumnIndexToOffset[column])

	if table.columnIsNil(rowPtr, column) {
		return nil
	}

	switch table.ColumnIndexToType[column] {
	case TypeUint8:
		return *(*uint8)(columnPtr)
	case TypeInt8:
		return *(*int8)(columnPtr)
	case TypeUint16:
		return *(*uint16)(columnPtr)
	case TypeInt16:
		return *(*int16)(columnPtr)
	case TypeUint32:
		return *(*uint32)(columnPtr)
	case TypeInt32:
		return *(*int32)(columnPtr)
	case TypeFloat32:
		return *(*float32)(columnPtr)
	}
	return nil
}

func (table *FactTable) setColumnValue(row []byte, column int, value Untyped) {
	rowPtr := uintptr(unsafe.Pointer(&row[0]))
	if value == nil {
		nilBytePtr := unsafe.Pointer(rowPtr + uintptr(countColumnSize+column/8))
		nilBitIndex := uint(column) % 8
		*(*uint8)(nilBytePtr) = *(*uint8)(nilBytePtr) | (1 << (7 - nilBitIndex))
	} else {
		value := value.(float64)
		columnPtr := unsafe.Pointer(rowPtr + table.ColumnIndexToOffset[column])
		switch table.ColumnIndexToType[column] {
		case TypeUint8:
			*(*uint8)(columnPtr) = uint8(value)
		case TypeInt8:
			*(*int8)(columnPtr) = int8(value)
		case TypeUint16:
			*(*uint16)(columnPtr) = uint16(value)
		case TypeInt16:
			*(*int16)(columnPtr) = int16(value)
		case TypeUint32:
			*(*uint32)(columnPtr) = uint32(value)
		case TypeInt32:
			*(*int32)(columnPtr) = int32(value)
		case TypeFloat32:
			*(*float32)(columnPtr) = float32(value)
		}
	}
}

// Useful for inspecting the nil bits of a row when debugging.
func (table *FactTable) nilBitsToString(row int) string {
	var parts []string
	rowSlice, _ := table.getRowSlice(row)
	for _, b := range rowSlice[countColumnSize : countColumnSize+table.numNilBytes()] {
		parts = append(parts, fmt.Sprintf("%08b", b))
	}
	return strings.Join(parts, " ")
}

// Finds a row in the interval with the same dimensions and which doesn't have its count at the max value.
func (table *FactTable) findCollapsibleRowInInterval(row []byte, interval *Interval) ([]byte, bool) {
	// TODO(philc): I'm disabling row collapsing because the n^2 insertion time is too slow (who'd have
	// thought!?). We'll need to implement something faster.
	return nil, false
	// To determine whether rows are collapsible, we need to compare the bytes representing nil values, and the
	// value of all dimension columns.
	start := countColumnSize
	sliceLength := table.numNilBytes() + table.DimensionColumnsWidth
	rowDimensions := row[start : start+sliceLength]
	for si, segment := range interval.Segments {
		segmentLength := len(segment)
		if si == len(interval.Segments)-1 {
			segmentLength = interval.NextInsertOffset
		}
		end := segmentLength + start
		for i := start; i < end; i += table.RowSize {
			segmentRowDimensions := segment[i : i+sliceLength]
			if bytes.Equal(rowDimensions, segmentRowDimensions) {
				rowOffset := i - start
				rowCount := segment[rowOffset]
				if rowCount < 255 {
					return segment[rowOffset : rowOffset+table.RowSize], true
				}
			}
		}
	}
	return nil, false
}

// Adds all of b's metrics columns to a's metrics columns, and increments a's row count by one.
func (table *FactTable) collapseRow(a []byte, b []byte) {
	aPtr := uintptr(unsafe.Pointer(&a[0]))
	bPtr := uintptr(unsafe.Pointer(&b[0]))
	*(*uint8)(unsafe.Pointer(aPtr)) += 1 // Increment the count of rows.
	// Iterate over all metric columns and add from b to a.
	firstMetricColumn := len(table.Schema.DimensionColumns)
	for i := firstMetricColumn; i < table.ColumnCount; i++ {
		columnOffset := table.ColumnIndexToOffset[i]
		aColumnPtr := unsafe.Pointer(aPtr + columnOffset)
		bColumnPtr := unsafe.Pointer(bPtr + columnOffset)
		columnType := table.ColumnIndexToType[i]
		// TODO(philc): We should check for overflow here, or perhaps require that metric columns be something
		// large like float64.
		switch columnType {
		case TypeUint8:
			*(*uint8)(aColumnPtr) = *(*uint8)(aColumnPtr) + (*(*uint8)(bColumnPtr))
		case TypeInt8:
			*(*int8)(aColumnPtr) = *(*int8)(aColumnPtr) + (*(*int8)(bColumnPtr))
		case TypeUint16:
			*(*uint16)(aColumnPtr) = *(*uint16)(aColumnPtr) + (*(*uint16)(bColumnPtr))
		case TypeInt16:
			*(*int16)(aColumnPtr) = *(*int16)(aColumnPtr) + (*(*int16)(bColumnPtr))
		case TypeUint32:
			*(*uint32)(aColumnPtr) = *(*uint32)(aColumnPtr) + (*(*uint32)(bColumnPtr))
		case TypeInt32:
			*(*int32)(aColumnPtr) = *(*int32)(aColumnPtr) + (*(*int32)(bColumnPtr))
		case TypeFloat32:
			*(*float32)(aColumnPtr) = *(*float32)(aColumnPtr) + (*(*float32)(bColumnPtr))
		}
	}
}

func (table *FactTable) insertNormalizedRow(timestamp time.Time, row []byte) {
	interval := getIntervalForTimestamp(table, timestamp)
	if interval == nil {
		interval = table.createInterval(timestamp)
		table.Intervals = append(table.Intervals, interval)
	}
	if existingRow, ok := table.findCollapsibleRowInInterval(row, interval); ok {
		table.collapseRow(existingRow, row)
		return
	}
	if interval.SegmentsAreFull() {
		interval.AddSegment(table)
	}
	segment := interval.Segments[len(interval.Segments)-1]
	rowSlice := segment[interval.NextInsertOffset : interval.NextInsertOffset+table.RowSize]
	copy(rowSlice, row)
	rowSlice[0] = 1 // Set the count to 1.
	interval.NextInsertOffset += table.RowSize
	table.Count++
}

func getIntervalForTimestamp(table *FactTable, timestamp time.Time) *Interval {
	for _, interval := range table.Intervals {
		if interval.Start == timestamp {
			return interval
		}
	}
	return nil
}

func (table *FactTable) createInterval(timestamp time.Time) *Interval {
	return &Interval{
		Start:            timestamp,
		Duration:         intervalDuration,
		NextInsertOffset: 0, // A byte offset of the last segment.
		Segments:         [][]byte{},
	}
}

// Appends a new segment to the end of interval.Segments. Note that this should only be called when there is
// no more remaining space in the existing segments.
func (interval *Interval) AddSegment(table *FactTable) {
	if !interval.SegmentsAreFull() {
		panic("Adding a new segment when the existing segments aren't full. That's invalid.")
	}
	segmentSize := int(table.SegmentSizeInBytes/table.RowSize) * table.RowSize
	var segment []byte
	if table.FilePath == "" {
		// Use in-memory storage; don't persist anything to disk.
		segment = make([]byte, segmentSize)
	} else {
		segmentIndex := 0
		if len(interval.Segments) > 0 {
			segmentIndex = len(interval.Segments) + 1
		}
		segment = createMemoryMappedSegment(table.FilePath, interval.Start, segmentIndex, segmentSize)
	}
	interval.Segments = append(interval.Segments, segment)
	interval.SegmentCount++
	interval.NextInsertOffset = 0
}

// True if there is no room remaining in the Interval's segments, and a new one needs to be allocated.
func (interval *Interval) SegmentsAreFull() bool {
	return len(interval.Segments) == 0 ||
		interval.NextInsertOffset >= len(interval.Segments[len(interval.Segments)-1])
}

// Inserts the given rows into the table. Returns an error if any row contains an unrecognized column.
func (table *FactTable) InsertRowMaps(rows []RowMap) error {
	table.InsertLock.Lock()
	defer table.InsertLock.Unlock()

	for _, rowMap := range rows {
		normalizedRow, err := table.normalizeRow(rowMap)
		if err != nil {
			return err
		}
		timestamp := rowMap[table.TimestampColumnName].(int)
		// Truncate the timestamp to the interval's duration.
		timestamp = timestamp - (timestamp % int(intervalDuration/time.Second))
		table.insertNormalizedRow(time.Unix(int64(timestamp), 0), normalizedRow)
	}
	return nil
}

func (table *DimensionTable) addRow(rowValue string) int32 {
	nextId := int32(len(table.Rows))
	table.Rows = append(table.Rows, rowValue)
	table.ValueToId[rowValue] = nextId
	return nextId
}

// The fraction of the table's rows are compressed together. Returns a number between 0 and 1. For example, if
// 4 rows are stored in the table but only 1 row of space is used, then the compression factor is 0.75. Note
// that performing this computation requires scanning the entire table.
func (table *FactTable) GetCompressionFactor() float64 {
	rows := 0
	rowsStored := 0
	for _, interval := range table.Intervals {
		for i, segment := range interval.Segments {
			segmentSize := len(segment)
			if i == len(interval.Segments)-1 {
				segmentSize = interval.NextInsertOffset
			}
			for j := 0; j < segmentSize; j += table.RowSize {
				rows += int(segment[j]) // Sum the row's count byte
				rowsStored++
			}
		}
	}
	return float64(rows-rowsStored) / float64(rows)
}

func isString(value interface{}) bool {
	result := false
	switch value.(type) {
	case string:
		result = true
	}
	return result
}

func convertUntypedToFloat64(v Untyped) float64 {
	var result float64
	switch v.(type) {
	case float32:
		result = float64(v.(float32))
	case float64:
		result = v.(float64)
	case uint8:
		result = float64(v.(uint8))
	case uint16:
		result = float64(v.(uint16))
	case uint32:
		result = float64(v.(uint32))
	case uint64:
		result = float64(v.(uint64))
	case int:
		result = float64(v.(int))
	case int8:
		result = float64(v.(int8))
	case int32:
		result = float64(v.(int32))
	case int64:
		result = float64(v.(int64))
	default:
		panic(fmt.Sprintf("Unrecognized type: %s", reflect.TypeOf(v)))
	}
	return result
}
