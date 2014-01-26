// The core table creation and query execution functions.
package gumshoe

import (
	"fmt"
	"reflect"
	"sync"
	"unsafe"

	mmap "github.com/edsrzf/mmap-go"
)

// The size of the fact table is currently a compile time constant, so we can use native arrays instead of
// ranges. In the future we'll use byte arrays so we that rows can be composites of many column types.
type Cell float32

// A fixed sized table of rows.
// When we insert more rows than the table's capacity, we wrap around and begin inserting rows at index 0.
type FactTable struct {
	// We serialize this struct using JSON. The unexported fields are fields we don't want to serialize.
	rows     []byte
	FilePath string // Path to this table on disk, where we will periodically snapshot it to.
	// The mmap bookkeeping object which contains the file descriptor we are mapping the table rows to.
	memoryMap mmap.MMap
	// This mutex must be held by each writer to prevent concurrent writes.
	// TODO(caleb): This is not enough. Reads still race with writes. We need to fix this, possibly by removing
	// the circular writes and instead persisting historic chunks to disk (or deleting them) and allocating
	// fresh tables.
	insertLock         *sync.Mutex
	NextInsertPosition int
	Count              int               // The number of used rows currently in the table. This is <= ROWS.
	ColumnCount        int               // The number of columns in use in the table. This is <= COLS.
	Capacity           int               // For now, this is an alias for the ROWS constant.
	RowSize            int               // In bytes
	ColumnSize         int               // TODO(philc): Remove this once we stop using fixed-type columns
	DimensionTables    []*DimensionTable // A mapping from column index => column's DimensionTable.
	ColumnNameToIndex  map[string]int
	ColumnIndexToName  []string
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

type RowAggregate struct {
	GroupByValue Cell
	Sums         []float64
	Count        int
}

type FactTableFilterFunc func(uintptr) bool

// Allocates a new FactTable. If a non-empty filePath is specified, this table's rows are immediately
// persisted to disk in the form of a memory-mapped file.
func NewFactTable(filePath string, rowCount int, columnNames []string) *FactTable {
	table := &FactTable{
		ColumnCount: len(columnNames),
		FilePath:    filePath,
		insertLock:  new(sync.Mutex),
		ColumnSize:  4,
		Capacity:    rowCount,
	}
	table.DimensionTables = make([]*DimensionTable, table.ColumnCount)
	for i, name := range columnNames {
		table.DimensionTables[i] = NewDimensionTable(name)
	}
	table.RowSize = table.ColumnCount * table.ColumnSize
	tableSize := table.Capacity * table.RowSize
	if filePath == "" {
		// Create an in-memory database only, without a file backing.
		slice := make([]byte, tableSize)
		table.rows = slice
	} else {
		table.memoryMap, table.rows = CreateMemoryMappedFactTableStorage(table.FilePath, tableSize)
	}
	table.ColumnIndexToName = make([]string, len(columnNames))
	table.ColumnNameToIndex = make(map[string]int, len(columnNames))
	for i, name := range columnNames {
		table.ColumnIndexToName[i] = name
		table.ColumnNameToIndex[name] = i
	}
	return table
}

// Return a set of row maps. Useful for debugging the contents of the table.
func (table *FactTable) GetRowMaps(start, end int) []map[string]Untyped {
	results := make([]map[string]Untyped, 0, table.Count)
	if start > end {
		panic("Invalid indices passed to GetRowMaps")
	}
	for i := start; i < end; i++ {
		results = append(results, table.DenormalizeRow(table.getRowSlice(i)))
	}
	return results
}

// Given a cell from a row vector, returns either the cell if this column isn't already normalized,
// or the denormalized value. E.g. denormalizeColumnValue(213, 1) => "Japan"
func (table *FactTable) denormalizeColumnValue(columnValue Cell, columnIndex int) Untyped {
	// TODO(philc): I'm using the implicit condition that if a dimension table is empty, this column isn't a
	// normalized dimension. This should be represented explicitly by a user-defined schema.
	dimensionTable := table.DimensionTables[columnIndex]
	if len(dimensionTable.Rows) > 0 {
		return dimensionTable.Rows[int(columnValue)]
	} else {
		return columnValue
	}
}

// Takes a normalized row vector and returns a map consisting of column names and values pulled from the
// dimension tables.
// e.g. [0, 1, 17] => {"country": "Japan", "browser": "Chrome", "age": 17}
func (table *FactTable) DenormalizeRow(row *[]byte) map[string]Untyped {
	result := make(map[string]Untyped)
	rowDataPtr := (*reflect.SliceHeader)(unsafe.Pointer(row)).Data
	for i := 0; i < table.ColumnCount; i++ {
		value := *(*Cell)(unsafe.Pointer(rowDataPtr))
		result[table.ColumnIndexToName[i]] = table.denormalizeColumnValue(value, i)
		rowDataPtr += uintptr(table.ColumnSize)
	}
	return result
}

// Takes a map of column names => values, and returns a vector with the map's values in the correct column
// position according to the table's schema, e.g.:
// e.g. {"country": "Japan", "browser": "Chrome", "age": 17} => ["Chrome", 17, "Japan"]
// Returns an error if there are unrecognized columns, or if a column is missing.
func (table *FactTable) convertRowMapToRowArray(rowMap map[string]Untyped) ([]Untyped, error) {
	result := make([]Untyped, table.ColumnCount)
	for columnName, value := range rowMap {
		columnIndex, found := table.ColumnNameToIndex[columnName]
		if !found {
			return nil, fmt.Errorf("Unrecognized column name: %s", columnName)
		}
		result[columnIndex] = value
	}
	return result, nil
}

func (table *FactTable) getRowOffset(row int) int {
	return row * table.RowSize
}

func (table *FactTable) getRowSlice(row int) *[]byte {
	rowOffset := table.getRowOffset(row)
	newSlice := table.Rows()[rowOffset : rowOffset+table.RowSize]
	return &newSlice
}

// Takes a row of mixed types, like strings and ints, and converts it to a fact row (a vector of integer
// types). For every string column, replaces its value with the matching ID from the dimension table,
// inserting a row into the dimension table if one doesn't already exist.
// e.g. {"country": "Japan", "browser": "Chrome", "age": 17} => [0, 1, 17]
func (table *FactTable) normalizeRow(rowMap map[string]Untyped) (*[]byte, error) {
	rowAsArray, err := table.convertRowMapToRowArray(rowMap)
	if err != nil {
		return nil, err
	}
	rowSlice := make([]byte, table.RowSize)
	rowDataPtr := (*reflect.SliceHeader)(unsafe.Pointer(&rowSlice)).Data
	for columnIndex, value := range rowAsArray {
		usesDimensionTable := isString(value)
		columnOffset := uintptr(columnIndex * table.ColumnSize)
		if usesDimensionTable {
			stringValue := value.(string)
			dimensionTable := table.DimensionTables[columnIndex]
			dimensionRowId, ok := dimensionTable.ValueToId[stringValue]
			if !ok {
				dimensionRowId = dimensionTable.addRow(stringValue)
			}
			*(*Cell)(unsafe.Pointer(rowDataPtr + columnOffset)) = Cell(dimensionRowId)
		} else {
			*(*Cell)(unsafe.Pointer(rowDataPtr + columnOffset)) = Cell(convertUntypedToFloat64(value))
		}
	}
	return &rowSlice, nil
}

func (table *FactTable) insertNormalizedRow(row *[]byte) {
	copy(*table.getRowSlice(table.NextInsertPosition), *row)
	table.NextInsertPosition = (table.NextInsertPosition + 1) % table.Capacity
	if table.Count < table.Capacity {
		table.Count++
	}
}

// Inserts the given rows into the table. Returns an error if one of the rows contains an unrecognized column.
func (table *FactTable) InsertRowMaps(rows []map[string]Untyped) error {
	table.insertLock.Lock()
	defer table.insertLock.Unlock()

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

// Scans all rows in the table, aggregating columns, filtering and grouping rows.
// This logic is performance critical.
// TODO(philc): make the groupByColumnName parameter be an integer, for consistency
func (table *FactTable) scan(filters []FactTableFilterFunc, columnIndices []int,
	groupByColumnName string, groupByColumnTransformFn func(Cell) Cell) []RowAggregate {
	columnIndexToGroupBy, useGrouping := table.ColumnNameToIndex[groupByColumnName]
	// This maps the values of the group-by column => RowAggregate.
	// Due to laziness, only one level of grouping is supported. TODO(philc): Support multiple levels of
	// grouping.
	rowAggregatesMap := make(map[Cell]*RowAggregate)
	// When the query has no group-by clause, we accumulate results into a single RowAggregate.
	rowAggregate := new(RowAggregate)
	rowAggregate.Sums = make([]float64, table.ColumnCount)
	rowCount := table.Count
	columnCountInQuery := len(columnIndices)
	filterCount := len(filters)
	rowPtr := (*reflect.SliceHeader)(unsafe.Pointer(&table.rows)).Data
	rowSize := uintptr(table.RowSize)
	groupByColumnOffset := uintptr(columnIndexToGroupBy * 4)

outerLoop:
	for i := 0; i < rowCount; i++ {
		for filterIndex := 0; filterIndex < filterCount; filterIndex++ {
			if !filters[filterIndex](rowPtr) {
				rowPtr += rowSize
				continue outerLoop
			}
		}

		if useGrouping {
			groupByValue := *(*Cell)(unsafe.Pointer(rowPtr + groupByColumnOffset))
			if groupByColumnTransformFn != nil {
				groupByValue = groupByColumnTransformFn(groupByValue)
			}
			var found bool
			rowAggregate, found = rowAggregatesMap[groupByValue]
			if !found {
				rowAggregate = new(RowAggregate)
				rowAggregate.Sums = make([]float64, table.ColumnCount)
				(*rowAggregate).GroupByValue = groupByValue
				rowAggregatesMap[groupByValue] = rowAggregate
			}
		}

		for j := 0; j < columnCountInQuery; j++ {
			columnIndex := columnIndices[j]
			// TODO(philc): Pre-compute these offsets.
			columnOffset := uintptr(4 * columnIndex)
			columnValue := *(*Cell)(unsafe.Pointer(rowPtr + columnOffset))
			(*rowAggregate).Sums[columnIndex] += float64(columnValue)
		}
		(*rowAggregate).Count++
		rowPtr += rowSize
	}

	results := make([]RowAggregate, 0)
	if useGrouping {
		for _, value := range rowAggregatesMap {
			results = append(results, *value)
		}
	} else {
		results = append(results, *rowAggregate)
	}
	return results
}

// TODO(philc): This function probably be inlined.
func (table *FactTable) getColumnIndicesFromQuery(query *Query) []int {
	columnIndices := make([]int, 0)
	for _, queryAggregate := range query.Aggregates {
		columnIndices = append(columnIndices, table.ColumnNameToIndex[queryAggregate.Column])
	}
	return columnIndices
}

func (table *FactTable) mapRowAggregatesToJSONResultsFormat(query *Query,
	rowAggregates []RowAggregate) [](map[string]Untyped) {
	jsonRows := make([](map[string]Untyped), 0)
	for _, rowAggregate := range rowAggregates {
		jsonRow := make(map[string]Untyped)
		for _, queryAggregate := range query.Aggregates {
			columnIndex := table.ColumnNameToIndex[queryAggregate.Column]
			// TODO(philc): Change this to an enum
			sums := rowAggregate.Sums[columnIndex]
			if queryAggregate.Type == "sum" {
				jsonRow[queryAggregate.Name] = sums
			} else if queryAggregate.Type == "average" {
				jsonRow[queryAggregate.Name] = sums / float64(rowAggregate.Count)
			}
		}
		// TODO(philc): This code does not handle multi-level groupings.
		for _, grouping := range query.Groupings {
			columnIndex := table.ColumnNameToIndex[grouping.Column]
			jsonRow[grouping.Name] = table.denormalizeColumnValue(rowAggregate.GroupByValue, columnIndex)
		}
		jsonRow["rowCount"] = rowAggregate.Count
		jsonRows = append(jsonRows, jsonRow)
	}
	return jsonRows
}

// Given a list of values, looks up the corresponding row IDs for those values. If those values don't
// exist in the dimension table, they're omitted.
func (table *DimensionTable) getDimensionRowIdsForValues(values []string) []Cell {
	rowIds := make([]Cell, 0)
	for _, value := range values {
		if id, ok := table.ValueToId[value]; ok {
			rowIds = append(rowIds, Cell(id))
		}
	}
	return rowIds
}

// Given a QueryFilter, return a filter function that can be tested against a row.
func convertQueryFilterToFilterFunc(queryFilter QueryFilter, table *FactTable) FactTableFilterFunc {
	columnIndex := table.ColumnNameToIndex[queryFilter.Column]
	var f FactTableFilterFunc

	// The query value can either be a single value (in the case of =, >, < queries) or an array of values (in
	// the case of "in", "not in" queries.
	var valueAsCell Cell
	var valueAsCells []Cell

	queryValueIsList := queryFilter.Type == "in"

	if queryValueIsList {
		untypedQueryValues := queryFilter.Value.([]interface{})
		shouldTranslateToDimensionColumnIds := len(untypedQueryValues) > 0 && isString(untypedQueryValues[0])
		if shouldTranslateToDimensionColumnIds {
			// Convert this slice of untyped objects to []string. We encounter a panic if we try to cast straight
			// to []string; I'm not sure why.
			queryValuesAstrings := make([]string, 0, len(untypedQueryValues))
			for _, value := range untypedQueryValues {
				queryValuesAstrings = append(queryValuesAstrings, value.(string))
			}
			dimensionTable := table.DimensionTables[columnIndex]
			valueAsCells = dimensionTable.getDimensionRowIdsForValues(queryValuesAstrings)
		} else {
			valueAsCells = make([]Cell, 0, len(untypedQueryValues))
			for _, value := range untypedQueryValues {
				valueAsCells = append(valueAsCells, (Cell(convertUntypedToFloat64(value))))
			}
		}
	} else {
		if isString(queryFilter.Value) {
			dimensionTable := table.DimensionTables[columnIndex]
			matchingRowIds := dimensionTable.getDimensionRowIdsForValues([]string{queryFilter.Value.(string)})
			if len(matchingRowIds) == 0 {
				return func(row uintptr) bool { return false }
			} else {
				valueAsCell = matchingRowIds[0]
			}
		} else {
			valueAsCell = Cell(convertUntypedToFloat64(queryFilter.Value))
		}
	}

	columnOffset := uintptr(columnIndex * table.ColumnSize)

	switch queryFilter.Type {
	case "greaterThan", ">":
		f = func(row uintptr) bool {
			columnValue := *(*Cell)(unsafe.Pointer(row + columnOffset))
			return columnValue > valueAsCell
		}
	case "greaterThanOrEqualTo", ">=":
		f = func(row uintptr) bool {
			columnValue := *(*Cell)(unsafe.Pointer(row + columnOffset))
			return columnValue >= valueAsCell
		}
	case "lessThan", "<":
		f = func(row uintptr) bool {
			columnValue := *(*Cell)(unsafe.Pointer(row + columnOffset))
			return columnValue < valueAsCell
		}
	case "lessThanOrEqualTo", "<=":
		f = func(row uintptr) bool {
			columnValue := *(*Cell)(unsafe.Pointer(row + columnOffset))
			return columnValue <= valueAsCell
		}
	case "notequal", "!=":
		f = func(row uintptr) bool {
			columnValue := *(*Cell)(unsafe.Pointer(row + columnOffset))
			return columnValue != valueAsCell
		}
	case "equal", "=":
		f = func(row uintptr) bool {
			columnValue := *(*Cell)(unsafe.Pointer(row + columnOffset))
			return columnValue == valueAsCell
		}
	case "in":
		count := len(valueAsCells)
		// TODO(philc): A hash table may be more efficient for longer lists. We should determine what that list
		// size is and use a hash table in that case.
		f = func(row uintptr) bool {
			columnValue := *(*Cell)(unsafe.Pointer(row + columnOffset))
			for i := 0; i < count; i++ {
				if columnValue == valueAsCells[i] {
					return true
				}
			}
			return false
		}
	}
	return f
}

// Returns a function which, given a cell, performs a date-truncation transformation.
// - transformFunctionName: one of [minute, hour, day].
func convertTimeTransformToFunc(transformFunctionName string) func(Cell) Cell {
	var divisor int
	switch transformFunctionName {
	case "minute":
		divisor = 60
	case "hour":
		divisor = 60 * 60
	case "day":
		divisor = 60 * 60 * 24
	}
	return func(cell Cell) Cell {
		cellInt := int(cell)
		remainder := cellInt % divisor
		return Cell(cellInt - remainder)
	}
}

func (table *FactTable) InvokeQuery(query *Query) map[string]Untyped {
	columnIndices := table.getColumnIndicesFromQuery(query)
	var groupByColumn string
	var groupByTransformFunc func(Cell) Cell
	// NOTE(philc): For now, only support one level of grouping. We intend to support multiple levels.
	if len(query.Groupings) > 0 {
		grouping := query.Groupings[0]
		groupByColumn = grouping.Column
		if grouping.TimeTransform != "" {
			groupByTransformFunc = convertTimeTransformToFunc(grouping.TimeTransform)
		}
	}

	filterFuncs := make([]FactTableFilterFunc, 0, len(query.Filters))
	for _, queryFilter := range query.Filters {
		filterFuncs = append(filterFuncs, convertQueryFilterToFilterFunc(queryFilter, table))
	}

	results := table.scan(filterFuncs, columnIndices, groupByColumn, groupByTransformFunc)
	jsonResultRows := table.mapRowAggregatesToJSONResultsFormat(query, results)
	return map[string]Untyped{
		"results": jsonResultRows,
	}
}

func isString(value interface{}) bool {
	// TODO(philc): There must be a built-in for this.
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
	case int:
		result = float64(v.(int))
	case int8:
		result = float64(v.(int8))
	case int32:
		result = float64(v.(int32))
	case int64:
		result = float64(v.(int64))
	}
	return result
}
