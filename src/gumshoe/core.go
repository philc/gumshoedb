// The table creation and insertion functions.
package gumshoe

import (
	"fmt"
	"math"
	"reflect"
	"sort"
	"strings"
	"sync"
	"unsafe"

	mmap "github.com/edsrzf/mmap-go"
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

type Schema struct {
	NumericColumns map[string]int // name => size
	StringColumns  map[string]int // name => size
}

func NewSchema() *Schema {
	s := new(Schema)
	s.NumericColumns = make(map[string]int)
	s.StringColumns = make(map[string]int)
	return s
}

// A fixed sized table of rows.
// When we insert more rows than the table's capacity, we wrap around and begin inserting rows at index 0.
type FactTable struct {
	// We serialize this struct using JSON. The unexported fields are fields we don't want to serialize.
	rows     []byte
	FilePath string `json:"-"` // Path to this table on disk, where we will periodically snapshot it to.
	// TODO(caleb): This is not enough. Reads still race with writes. We need to fix this, possibly by removing
	// the circular writes and instead persisting historic chunks to disk (or deleting them) and allocating
	// fresh tables.
	InsertLock *sync.Mutex `json:"-"`
	// The mmap bookkeeping object which contains the file descriptor we are mapping the table rows to.
	memoryMap           mmap.MMap
	NextInsertPosition  int
	Count               int               // The number of used rows currently in the table. This is <= ROWS.
	ColumnCount         int               // The number of columns in use in the table. This is <= COLS.
	Capacity            int               // For now, this is an alias for the ROWS constant.
	RowSize             int               // In bytes
	DimensionTables     []*DimensionTable // A mapping from column index => column's DimensionTable.
	ColumnNameToIndex   map[string]int
	ColumnIndexToName   []string
	ColumnIndexToOffset []uintptr // The byte offset of each column from the beggining byte of the row
	ColumnIndexToType   []int     // Index => one of the type constants (e.g. TypeUint8).
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

// NewFactTable allocates a FactTable. If a non-empty filePath is specified, this table's rows are immediately
// persisted to disk in the form of a memory-mapped file. The parent directory of filePath is recursively
// created, if necessary. String columns appear first, and then numeric columns, for no particular reason
// other than implementation convenience in a few places.
func NewFactTable(filePath string, rowCount int, schema *Schema) (*FactTable, error) {
	stringColumnNames := getSortedKeys(schema.StringColumns)
	numericColumnNames := getSortedKeys(schema.NumericColumns)
	allColumnNames := append(stringColumnNames, numericColumnNames...)
	table := &FactTable{
		ColumnCount: len(allColumnNames),
		FilePath:    filePath,
		InsertLock:  new(sync.Mutex),
		Capacity:    rowCount,
	}

	columnToType := make(map[string]int)
	for k, v := range schema.StringColumns {
		columnToType[k] = v
	}
	for k, v := range schema.NumericColumns {
		columnToType[k] = v
	}

	// Compute the byte offset from the beginning of the row for each column
	table.ColumnIndexToOffset = make([]uintptr, table.ColumnCount)
	table.ColumnIndexToType = make([]int, table.ColumnCount)
	columnOffset := table.numNilBytes() // Skip space for the nil bytes header
	for i, name := range allColumnNames {
		table.ColumnIndexToOffset[i] = uintptr(columnOffset)
		table.ColumnIndexToType[i] = columnToType[name]
		columnOffset += typeSizes[columnToType[name]]
	}

	table.DimensionTables = make([]*DimensionTable, len(stringColumnNames))
	for i, column := range stringColumnNames {
		table.DimensionTables[i] = NewDimensionTable(column)
	}

	table.RowSize = columnOffset
	tableSize := table.Capacity * table.RowSize
	if filePath == "" {
		// Create an in-memory database only, without a file backing.
		slice := make([]byte, tableSize)
		table.rows = slice
	} else {
		memoryMap, err := CreateMemoryMappedFactTableStorage(table.FilePath, tableSize)
		if err != nil {
			return nil, err
		}
		table.memoryMap = memoryMap
		table.rows = []byte(memoryMap)
	}

	table.ColumnIndexToName = make([]string, len(allColumnNames))
	table.ColumnNameToIndex = make(map[string]int, len(allColumnNames))
	for i, name := range allColumnNames {
		table.ColumnIndexToName[i] = name
		table.ColumnNameToIndex[name] = i
	}

	return table, nil
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
		results = append(results, table.DenormalizeRow(table.getRowSlice(i)))
	}
	return results
}

// Given a column value from a row vector, return either the column value if it's a numeric column, or the
// corresponding string if it's a normalized string column.
// E.g. denormalizeColumnValue(213, 1) => "Japan"
func (table *FactTable) denormalizeColumnValue(value Untyped, columnIndex int) Untyped {
	if value == nil {
		return value
	} else if table.columnUsesDimensionTable(columnIndex) {
		dimensionTable := table.DimensionTables[columnIndex]
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
		result[name] = table.denormalizeColumnValue(value, i)
	}
	return result
}

// Takes a map of column names => values, and returns a vector with the map's values in the correct column
// position according to the table's schema, e.g.:
// e.g. {"country": "Japan", "browser": "Chrome", "age": 17} => ["Chrome", 17, "Japan"]
// Returns an error if there are unrecognized columns, or if a column is missing.
func (table *FactTable) convertRowMapToRowArray(rowMap RowMap) ([]Untyped, error) {
	result := make([]Untyped, table.ColumnCount)
	for columnName, value := range rowMap {
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

func (table *FactTable) getRowSlice(row int) []byte {
	rowOffset := table.getRowOffset(row)
	newSlice := table.Rows()[rowOffset : rowOffset+table.RowSize]
	return newSlice
}

func (table *FactTable) columnUsesDimensionTable(columnIndex int) bool {
	// This expression is valid because we put the string columns at the beginning of the row.
	return columnIndex < len(table.DimensionTables)
}

// Takes a row of mixed types and replaces all string columns with the ID of the matching string in the
// corresponding dimension table (creating a new row in the dimension table if the dimension table doesn't
// already contain the string).
// Note that all numeric values are assumed to be float64 (this is what Go's JSON unmarshaller produces).
// e.g. {"country": "Japan", "browser": "Chrome", "age": 17} => [0, 1, 17]
// TODO(philc): Make this return []byte
func (table *FactTable) normalizeRow(rowMap RowMap) (*[]byte, error) {
	rowAsArray, err := table.convertRowMapToRowArray(rowMap)
	if err != nil {
		return nil, err
	}
	rowSlice := make([]byte, table.RowSize)
	for columnIndex, value := range rowAsArray {
		if value == nil {
			table.setColumnValue(rowSlice, columnIndex, value)
			continue
		}
		var valueAsFloat64 float64
		if table.columnUsesDimensionTable(columnIndex) {
			if !isString(value) {
				return nil, fmt.Errorf("Cannot insert a non-string value into column %s.",
					table.ColumnIndexToName[columnIndex])
			}
			stringValue := value.(string)
			dimensionTable := table.DimensionTables[columnIndex]
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
					table.ColumnIndexToName[columnIndex], reflect.TypeOf(value))
			}
		}
		table.setColumnValue(rowSlice, columnIndex, valueAsFloat64)
	}
	return &rowSlice, nil
}

func (table *FactTable) columnIsNil(rowPtr uintptr, columnIndex int) bool {
	nilBytePtr := unsafe.Pointer(rowPtr + uintptr(columnIndex/8))
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
		nilBytePtr := unsafe.Pointer(rowPtr + uintptr(column/8))
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
	for _, b := range table.getRowSlice(row)[0:table.numNilBytes()] {
		parts = append(parts, fmt.Sprintf("%08b", b))
	}
	return strings.Join(parts, " ")
}

func (table *FactTable) insertNormalizedRow(row *[]byte) {
	copy(table.getRowSlice(table.NextInsertPosition), *row)
	table.NextInsertPosition = (table.NextInsertPosition + 1) % table.Capacity
	if table.Count < table.Capacity {
		table.Count++
	}
}

// Inserts the given rows into the table. Returns an error if one of the rows contains an unrecognized column.
func (table *FactTable) InsertRowMaps(rows []RowMap) error {
	table.InsertLock.Lock()
	defer table.InsertLock.Unlock()

	for _, rowMap := range rows {
		normalizedRow, err := table.normalizeRow(rowMap)
		if err != nil {
			return err
		}
		table.insertNormalizedRow(normalizedRow)
	}
	return nil
}

func (table *DimensionTable) addRow(rowValue string) int32 {
	nextId := int32(len(table.Rows))
	table.Rows = append(table.Rows, rowValue)
	table.ValueToId[rowValue] = nextId
	return nextId
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
