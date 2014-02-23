// Query execution functions.
package gumshoe

import (
	"math"
	"reflect"
	"unsafe"
)

func (table *FactTable) InvokeQuery(query *Query) map[string]Untyped {
	columnIndices := table.getColumnIndicesFromQuery(query)
	var groupingParams GroupingParams
	// NOTE(philc): For now, only support one level of grouping. We intend to support multiple levels.
	if len(query.Groupings) > 0 {
		grouping := query.Groupings[0]
		groupingParams = GroupingParams{
			UseGrouping: true,
			ColumnIndex: table.ColumnNameToIndex[grouping.Column],
		}

		// Only support computing the max value of 8 and 16 bit unsigned types, since that set of values
		// can efficiently be mapped to array indices for the purposes of grouping.
		switch table.ColumnIndexToType[groupingParams.ColumnIndex] {
		case TypeUint8:
			groupingParams.KnownCardinality = true
			groupingParams.MaxValue = int(math.Pow(2, 8))
		case TypeUint16:
			groupingParams.KnownCardinality = true
			groupingParams.MaxValue = int(math.Pow(2, 16))
		}
		if grouping.TimeTransform != "" {
			groupingParams.TransformFn = convertTimeTransformToFunc(grouping.TimeTransform)
		}
	}
	filterFuncs := make([]FactTableFilterFunc, 0, len(query.Filters))
	for _, queryFilter := range query.Filters {
		filterFuncs = append(filterFuncs, convertQueryFilterToFilterFunc(queryFilter, table))
	}

	results := table.scan(filterFuncs, columnIndices, groupingParams)
	jsonResultRows := table.mapRowAggregatesToJSONResultsFormat(query, results)
	return map[string]Untyped{
		"results": jsonResultRows,
	}
}

// Contains all configuration needed to perform the user's group by query.
type GroupingParams struct {
	UseGrouping      bool
	ColumnIndex      int
	TransformFn      func(float64) float64
	KnownCardinality bool
	MaxValue         int
	// We assume the minValue is zero.
}

// Scans all rows in the table, aggregates columns, filters and groups rows. This code is performance
// critical.
func (table *FactTable) scan(filters []FactTableFilterFunc, columnIndices []int,
	groupingParams GroupingParams) []RowAggregate {
	var useGrouping bool
	var groupedAggregatesMap map[float64]*RowAggregate
	var groupedAggregatesSlice []RowAggregate
	var groupByColumnOffset uintptr
	var groupByColumnType int
	var groupByColumnIndex int
	var groupByColumnTransformFn func(float64) float64
	var useSliceForGrouping bool

	if useGrouping = groupingParams.UseGrouping; useGrouping {
		if groupingParams.KnownCardinality {
			useSliceForGrouping = true
			groupedAggregatesSlice = make([]RowAggregate, groupingParams.MaxValue)
		} else {
			groupedAggregatesMap = make(map[float64]*RowAggregate)
		}
		groupByColumnIndex = groupingParams.ColumnIndex
		groupByColumnOffset = table.ColumnIndexToOffset[groupingParams.ColumnIndex]
		groupByColumnType = table.ColumnIndexToType[groupingParams.ColumnIndex]
		groupByColumnTransformFn = groupingParams.TransformFn
	}

	// This maps the values of the group-by column => RowAggregate.
	// Due to laziness, only one level of grouping is currently supported.
	// When the query has no group-by clause, we accumulate results into a single RowAggregate.
	rowAggregate := new(RowAggregate)
	rowAggregate.Sums = make([]float64, table.ColumnCount)
	rowCount := table.Count
	columnCountInQuery := len(columnIndices)
	filterCount := len(filters)
	rowPtr := (*reflect.SliceHeader)(unsafe.Pointer(&table.rows)).Data
	rowSize := uintptr(table.RowSize)
	columnIndexToOffset := table.ColumnIndexToOffset
	columnIndexToType := table.ColumnIndexToType

outerLoop:
	for i := 0; i < rowCount; i++ {
		for filterIndex := 0; filterIndex < filterCount; filterIndex++ {
			if !filters[filterIndex](rowPtr) {
				rowPtr += rowSize
				continue outerLoop
			}
		}

		if useGrouping {
			// NOTE(dmac): For now, nil values aren't included when grouping on a column.
			if table.columnIsNil(rowPtr, groupByColumnIndex) {
				rowPtr += rowSize
				continue
			}
			// TODO(philc): Use a type switch here.
			groupByValue := getColumnValueAsFloat64(rowPtr, groupByColumnOffset, groupByColumnType)
			if groupByColumnTransformFn != nil {
				groupByValue = groupByColumnTransformFn(groupByValue)
			}
			if useSliceForGrouping {
				rowAggregate = &groupedAggregatesSlice[int(groupByValue)]
				// If the RowAggregate has never been initialized, initialize it.
				if len(rowAggregate.Sums) == 0 {
					*(&rowAggregate.Sums) = make([]float64, table.ColumnCount)
					rowAggregate.GroupByValue = groupByValue
				}
			} else {
				var ok bool
				rowAggregate, ok = groupedAggregatesMap[groupByValue]
				if !ok {
					rowAggregate = new(RowAggregate)
					rowAggregate.Sums = make([]float64, table.ColumnCount)
					(*rowAggregate).GroupByValue = groupByValue
					groupedAggregatesMap[groupByValue] = rowAggregate
				}
			}
		}

		for j := 0; j < columnCountInQuery; j++ {
			columnIndex := columnIndices[j]
			columnOffset := columnIndexToOffset[columnIndex]
			columnPtr := unsafe.Pointer(rowPtr + columnOffset)

			if table.columnIsNil(rowPtr, columnIndex) {
				continue
			}

			var columnValue float64
			columnType := columnIndexToType[columnIndex]
			switch columnType {
			case TypeUint8:
				columnValue = float64(*(*uint8)(columnPtr))
			case TypeInt8:
				columnValue = float64(*(*int8)(columnPtr))
			case TypeUint16:
				columnValue = float64(*(*uint16)(columnPtr))
			case TypeInt16:
				columnValue = float64(*(*int16)(columnPtr))
			case TypeUint32:
				columnValue = float64(*(*uint32)(columnPtr))
			case TypeInt32:
				columnValue = float64(*(*int32)(columnPtr))
			case TypeFloat32:
				columnValue = float64(*(*float32)(columnPtr))
			}
			(*rowAggregate).Sums[columnIndex] += columnValue
		}
		(*rowAggregate).Count++
		rowPtr += rowSize
	}

	results := []RowAggregate{}
	if useGrouping {
		if useSliceForGrouping {
			// Remove empty, unused rows from the grouping vector.
			for _, value := range groupedAggregatesSlice {
				if value.Count > 0 {
					results = append(results, value)
				}
			}
		} else {
			for _, value := range groupedAggregatesMap {
				results = append(results, *value)
			}
		}
	} else {
		results = append(results, *rowAggregate)
	}
	return results
}

func (table *FactTable) mapRowAggregatesToJSONResultsFormat(query *Query,
	rowAggregates []RowAggregate) [](map[string]Untyped) {
	jsonRows := [](map[string]Untyped){}
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

// TODO(philc): This function probably be inlined.
func (table *FactTable) getColumnIndicesFromQuery(query *Query) []int {
	columnIndices := []int{}
	for _, queryAggregate := range query.Aggregates {
		columnIndices = append(columnIndices, table.ColumnNameToIndex[queryAggregate.Column])
	}
	return columnIndices
}

// Given a list of values, looks up the corresponding row IDs for those values. If those values don't
// exist in the dimension table, they're omitted.
func (table *DimensionTable) getDimensionRowIdsForValues(values []string) []float64 {
	rowIds := []float64{}
	for _, value := range values {
		if id, ok := table.ValueToId[value]; ok {
			rowIds = append(rowIds, float64(id))
		}
	}
	return rowIds
}

// Returns a function which, given a cell, performs a date-truncation transformation.
// - transformFunctionName: one of [minute, hour, day].
func convertTimeTransformToFunc(transformFunctionName string) func(float64) float64 {
	var divisor int
	switch transformFunctionName {
	case "minute":
		divisor = 60
	case "hour":
		divisor = 60 * 60
	case "day":
		divisor = 60 * 60 * 24
	}
	return func(cell float64) float64 {
		cellInt := int(cell)
		remainder := cellInt % divisor
		return float64(cellInt - remainder)
	}
}

// Given a QueryFilter, return a filter function that can be tested against a row.
func convertQueryFilterToFilterFunc(queryFilter QueryFilter, table *FactTable) FactTableFilterFunc {
	columnIndex := table.ColumnNameToIndex[queryFilter.Column]
	var f FactTableFilterFunc

	// The query value can either be a single value (in the case of =, >, < queries) or an array of values (in
	// the case of "in", "not in" queries.
	var value float64
	var values []float64

	queryValueIsList := queryFilter.Type == "in"

	if queryValueIsList {
		untypedQueryValues := queryFilter.Value.([]interface{})
		shouldTranslateToDimensionColumnIds := len(untypedQueryValues) > 0 && isString(untypedQueryValues[0])
		if shouldTranslateToDimensionColumnIds {
			// Convert this slice of untyped objects to []string. We encounter a panic if we try to cast straight
			// to []string; I'm not sure why.
			valuesAsStrings := make([]string, 0, len(untypedQueryValues))
			for _, value := range untypedQueryValues {
				valuesAsStrings = append(valuesAsStrings, value.(string))
			}
			dimensionTable := table.DimensionTables[columnIndex]
			values = dimensionTable.getDimensionRowIdsForValues(valuesAsStrings)
		} else {
			values = make([]float64, 0, len(untypedQueryValues))
			for _, value := range untypedQueryValues {
				values = append(values, float64(convertUntypedToFloat64(value)))
			}
		}
	} else {
		if isString(queryFilter.Value) {
			dimensionTable := table.DimensionTables[columnIndex]
			matchingRowIds := dimensionTable.getDimensionRowIdsForValues([]string{queryFilter.Value.(string)})
			if len(matchingRowIds) == 0 {
				return func(row uintptr) bool { return false }
			} else {
				value = matchingRowIds[0]
			}
		} else {
			value = convertUntypedToFloat64(queryFilter.Value)
		}
	}

	columnOffset := table.ColumnIndexToOffset[columnIndex]
	columnType := table.ColumnIndexToType[columnIndex]

	switch queryFilter.Type {
	case "greaterThan", ">":
		f = func(row uintptr) bool {
			return !table.columnIsNil(row, columnIndex) &&
				getColumnValueAsFloat64(row, columnOffset, columnType) > value
		}
	case "greaterThanOrEqualTo", ">=":
		f = func(row uintptr) bool {
			return !table.columnIsNil(row, columnIndex) &&
				getColumnValueAsFloat64(row, columnOffset, columnType) >= value
		}
	case "lessThan", "<":
		f = func(row uintptr) bool {
			return !table.columnIsNil(row, columnIndex) &&
				getColumnValueAsFloat64(row, columnOffset, columnType) < value
		}
	case "lessThanOrEqualTo", "<=":
		f = func(row uintptr) bool {
			return !table.columnIsNil(row, columnIndex) &&
				getColumnValueAsFloat64(row, columnOffset, columnType) <= value
		}
	case "notEqual", "!=":
		f = func(row uintptr) bool {
			return !table.columnIsNil(row, columnIndex) &&
				getColumnValueAsFloat64(row, columnOffset, columnType) != value
		}
	case "equal", "=":
		f = func(row uintptr) bool {
			return !table.columnIsNil(row, columnIndex) &&
				getColumnValueAsFloat64(row, columnOffset, columnType) == value
		}
	case "in":
		count := len(values)
		// TODO(philc): A hash table may be more efficient for longer lists. We should determine what that list
		// size is and use a hash table in that case.
		f = func(row uintptr) bool {
			if table.columnIsNil(row, columnIndex) {
				return false
			}
			columnValue := getColumnValueAsFloat64(row, columnOffset, columnType)
			for i := 0; i < count; i++ {
				if columnValue == values[i] {
					return true
				}
			}
			return false
		}
	}
	return f
}

// A helper method used by the grouping and filtering functions in the scan method.
// TODO(philc): Consider inlining this for better performance.
func getColumnValueAsFloat64(row uintptr, columnOffset uintptr, columnType int) float64 {
	columnPtr := unsafe.Pointer(row + columnOffset)
	switch columnType {
	case TypeUint8:
		return float64(*(*uint8)(columnPtr))
	case TypeInt8:
		return float64(*(*int8)(columnPtr))
	case TypeUint16:
		return float64(*(*uint16)(columnPtr))
	case TypeInt16:
		return float64(*(*int16)(columnPtr))
	case TypeUint32:
		return float64(*(*uint32)(columnPtr))
	case TypeInt32:
		return float64(*(*int32)(columnPtr))
	case TypeFloat32:
		return float64(*(*float32)(columnPtr))
	}
	panic("Unrecognized column type.")
}