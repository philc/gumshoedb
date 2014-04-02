// Query execution functions.

package gumshoe

import (
	"errors"
	"fmt"
	"unsafe"
)

// UntypedBytes is some numeric type which is set and modified unsafely. We know its type because we know the
// corresponding column in context.
type UntypedBytes []byte

func (u UntypedBytes) Pointer() unsafe.Pointer { return unsafe.Pointer(&u[0]) }

type rowAggregate struct {
	GroupByValue UntypedBytes
	Sums         []UntypedBytes // Corresponds to query.Aggregates
	Count        uint32
}

type scanParams struct {
	TimestampFilterFuncs []timestampFilterFunc
	FilterFuncs          []filterFunc
	SumColumns           []MetricColumn
	SumFuncs             []sumFunc
	Grouping             *groupingParams
}

// groupingParams contains all configuration needed to perform the user's group by query.
type groupingParams struct {
	OnTimestampColumn bool
	ColumnIndex       int
	TransformFunc     transformFunc
	KnownCardinality  bool
	Cardinality       int
}

type (
	transformFunc       func(cell unsafe.Pointer, isNil bool) Untyped
	filterFunc          func(row RowBytes) bool
	timestampFilterFunc func(timestamp uint32) bool
	sumFunc             func(sum UntypedBytes, metrics MetricBytes)
)

// InvokeQuery runs query on a State. It returns a slice of aggregated row results.
func (s *State) InvokeQuery(query *Query) ([]RowMap, error) {
	sumColumns := make([]MetricColumn, len(query.Aggregates))
	sumFuncs := make([]sumFunc, len(query.Aggregates))
	for i, aggregate := range query.Aggregates {
		index, ok := s.MetricNameToIndex[aggregate.Column]
		if !ok {
			return nil, fmt.Errorf("%s (selected for aggregation) is not a valid metric column name",
				aggregate.Column)
		}
		sumFuncs[i] = s.makeSumFunc(aggregate, index)
		sumColumns[i] = s.MetricColumns[index]
	}

	// NOTE(philc): For now, only support one level of grouping. We intend to support multiple levels.
	// TODO(caleb): Remove this check once we actually support > 1 grouping.
	if len(query.Groupings) > 1 {
		return nil, fmt.Errorf("more than 1 grouping is not supported at the moment")
	}
	var grouping *groupingParams
	if len(query.Groupings) > 0 {
		grouping = new(groupingParams)
		groupingOptions := query.Groupings[0]

		var groupingColumn Column
		if groupingOptions.Column == s.TimestampColumn.Name {
			grouping.OnTimestampColumn = true
			groupingColumn = s.TimestampColumn
		} else {
			index, ok := s.DimensionNameToIndex[groupingOptions.Column]
			if !ok {
				return nil, fmt.Errorf("%s (used for grouping) is not a valid dimension column name",
					groupingOptions.Column)
			}
			grouping.ColumnIndex = index
			groupingColumn = s.DimensionColumns[index].Column
		}

		// Only support computing the max value of 8 and 16 bit unsigned types, since that set of values
		// can efficiently be mapped to slice indices for the purposes of groupingOptions.
		switch width := groupingColumn.Width; width {
		case 1, 2:
			grouping.KnownCardinality = true
			grouping.Cardinality = 1 << uint(8*width)
		}

		if groupingOptions.TimeTransform != TimeTruncationNone {
			var err error
			grouping.TransformFunc, err = s.makeTimeTruncationFunc(groupingOptions.TimeTransform, groupingColumn)
			if err != nil {
				return nil, err
			}
		}
	}

	var timestampFilterFuncs []timestampFilterFunc
	var filterFuncs []filterFunc
	for _, queryFilter := range query.Filters {
		if queryFilter.Column == s.TimestampColumn.Name {
			filter, err := s.makeTimestampFilterFunc(queryFilter)
			if err != nil {
				return nil, err
			}
			timestampFilterFuncs = append(timestampFilterFuncs, filter)
			continue
		}

		var err error
		var filter filterFunc
		if index, ok := s.DimensionNameToIndex[queryFilter.Column]; ok {
			filter, err = s.makeDimensionFilterFunc(queryFilter, index)
		} else if index, ok := s.MetricNameToIndex[queryFilter.Column]; ok {
			filter, err = s.makeMetricFilterFunc(queryFilter, index)
		} else {
			return nil, fmt.Errorf("%q (in a filter) is not a recognized column", queryFilter.Column)
		}
		if err != nil {
			return nil, err
		}
		filterFuncs = append(filterFuncs, filter)
	}

	params := &scanParams{
		TimestampFilterFuncs: timestampFilterFuncs,
		FilterFuncs:          filterFuncs,
		SumColumns:           sumColumns,
		SumFuncs:             sumFuncs,
		Grouping:             grouping,
	}

	var rows []*rowAggregate
	if grouping == nil {
		row := s.scan(params)
		rows = []*rowAggregate{row}
	} else {
		rows = s.scanWithGrouping(params)
	}

	return s.postProcessScanRows(rows, query, grouping), nil
}

func (s *State) scan(params *scanParams) *rowAggregate {
	result := new(rowAggregate)
	result.Sums = make([]UntypedBytes, len(params.SumColumns))
	for i, col := range params.SumColumns {
		result.Sums[i] = make(UntypedBytes, col.Width)
	}

intervalLoop:
	for timestamp, interval := range s.Intervals {

		// We can apply the timestamp filters at this point.
		for _, filter := range params.TimestampFilterFuncs {
			if !filter(uint32(timestamp.Unix())) {
				continue intervalLoop
			}
		}

		for _, segment := range interval.Segments {
		rowLoop:
			for i := 0; i < len(segment.Bytes); i += s.RowSize {
				row := RowBytes(segment.Bytes[i : i+s.RowSize])

				// Run each filter to see if we should skip this row.
				for _, filter := range params.FilterFuncs {
					if !filter(row) {
						continue rowLoop
					}
				}

				// Sum each aggregate metric.
				metrics := MetricBytes(row[s.MetricStartOffset:])
				for i, sumFn := range params.SumFuncs {
					sumFn(result.Sums[i], metrics)
				}

				result.Count += row.count(s.Schema)
			}
		}
	}
	return result
}

func (s *State) scanWithGrouping(params *scanParams) []*rowAggregate {
	panic("unimplemented")
}

// Scans all rows in the table, aggregates columns, filters and groups rows. This code is performance
// critical.
//func (table *FactTable) scan(filters []FactTableFilterFunc, columnIndices []int,
//groupingParams GroupingParams) []RowAggregate {
//var (
//useGrouping                bool
//groupedAggregatesMap       map[float64]*RowAggregate
//groupedAggregatesSlice     []RowAggregate
//groupByColumnOffset        uintptr
//groupByColumnType          int
//groupByColumnIndex         int
//groupByColumnTransformFunc func(float64) float64
//useSliceForGrouping        bool
//)

//if useGrouping = groupingParams.UseGrouping; useGrouping {
//if groupingParams.KnownCardinality {
//useSliceForGrouping = true
//groupedAggregatesSlice = make([]RowAggregate, groupingParams.Cardinality)
//} else {
//groupedAggregatesMap = make(map[float64]*RowAggregate)
//}
//groupByColumnIndex = groupingParams.ColumnIndex
//groupByColumnOffset = table.ColumnIndexToOffset[groupingParams.ColumnIndex]
//groupByColumnType = table.ColumnIndexToType[groupingParams.ColumnIndex]
//groupByColumnTransformFunc = groupingParams.TransformFunc
//}

//// This maps the values of the group-by column => RowAggregate.
//// Due to laziness, only one level of grouping is currently supported.
//// When the query has no group-by clause, we accumulate results into a single RowAggregate.
//rowAggregate := new(RowAggregate)
//rowAggregate.Sums = make([]float64, table.ColumnCount)
//columnCountInQuery := len(columnIndices)
//filterCount := len(filters)
//rowSize := uintptr(table.RowSize)
//columnIndexToOffset := table.ColumnIndexToOffset
//columnIndexToType := table.ColumnIndexToType

//for _, interval := range table.Intervals {
//for si, segment := range interval.Segments {
//rowPtr := uintptr(unsafe.Pointer(&segment[0]))
//var rowCount int
//if si == len(interval.Segments)-1 {
//rowCount = interval.NextInsertOffset / table.RowSize
//} else {
//rowCount = len(segment) / table.RowSize
//}

//outerLoop:
//for i := 0; i < rowCount; i++ {
//for filterIndex := 0; filterIndex < filterCount; filterIndex++ {
//if !filters[filterIndex](rowPtr) {
//rowPtr += rowSize
//continue outerLoop
//}
//}

//if useGrouping {
//// NOTE(dmac): For now, nil values aren't included when grouping on a column.
//if table.columnIsNil(rowPtr, groupByColumnIndex) {
//rowPtr += rowSize
//continue
//}
//// TODO(philc): Use a type switch here.
//groupByValue := getColumnValueAsFloat64(rowPtr, groupByColumnOffset, groupByColumnType)
//if groupByColumnTransformFunc != nil {
//groupByValue = groupByColumnTransformFunc(groupByValue)
//}
//if useSliceForGrouping {
//rowAggregate = &groupedAggregatesSlice[int(groupByValue)]
//// If the RowAggregate has never been initialized, initialize it.
//if len(rowAggregate.Sums) == 0 {
//*(&rowAggregate.Sums) = make([]float64, table.ColumnCount)
//rowAggregate.GroupByValue = groupByValue
//}
//} else {
//var ok bool
//rowAggregate, ok = groupedAggregatesMap[groupByValue]
//if !ok {
//rowAggregate = new(RowAggregate)
//rowAggregate.Sums = make([]float64, table.ColumnCount)
//(*rowAggregate).GroupByValue = groupByValue
//groupedAggregatesMap[groupByValue] = rowAggregate
//}
//}
//}

//for j := 0; j < columnCountInQuery; j++ {
//columnIndex := columnIndices[j]
//columnOffset := columnIndexToOffset[columnIndex]
//columnPtr := unsafe.Pointer(rowPtr + columnOffset)

//if table.columnIsNil(rowPtr, columnIndex) {
//continue
//}

//var columnValue float64
//columnType := columnIndexToType[columnIndex]
//switch columnType {
//case TypeUint8:
//columnValue = float64(*(*uint8)(columnPtr))
//case TypeInt8:
//columnValue = float64(*(*int8)(columnPtr))
//case TypeUint16:
//columnValue = float64(*(*uint16)(columnPtr))
//case TypeInt16:
//columnValue = float64(*(*int16)(columnPtr))
//case TypeUint32:
//columnValue = float64(*(*uint32)(columnPtr))
//case TypeInt32:
//columnValue = float64(*(*int32)(columnPtr))
//case TypeFloat32:
//columnValue = float64(*(*float32)(columnPtr))
//}
//(*rowAggregate).Sums[columnIndex] += columnValue
//}

//// The first byte is the count of how many rows have been collapsible into this one row.
//rowCount := *((*uint8)(unsafe.Pointer(rowPtr)))
//(*rowAggregate).Count += int(rowCount)
//rowPtr += rowSize
//}
//}
//}

//results := []RowAggregate{}
//if useGrouping {
//if useSliceForGrouping {
//// Remove empty, unused rows from the grouping vector.
//for _, value := range groupedAggregatesSlice {
//if value.Count > 0 {
//results = append(results, value)
//}
//}
//} else {
//for _, value := range groupedAggregatesMap {
//results = append(results, *value)
//}
//}
//} else {
//results = append(results, *rowAggregate)
//}
//return results
//}

func (s *State) postProcessScanRows(aggregates []*rowAggregate, query *Query,
	grouping *groupingParams) []RowMap {
	rows := make([]RowMap, len(aggregates))
	for i, aggregate := range aggregates {
		row := make(RowMap)
		for _, queryAggregate := range query.Aggregates {
			index := s.MetricNameToIndex[queryAggregate.Column]
			column := s.MetricColumns[index]
			sum := s.numericCellValue(aggregate.Sums[index].Pointer(), column.Type)
			switch queryAggregate.Type {
			case AggregateSum:
				row[queryAggregate.Name] = sum
			case AggregateAvg:
				row[queryAggregate.Name] = UntypedToFloat64(sum) / float64(aggregate.Count)
			}
		}
		if grouping != nil {
			var value Untyped
			switch {
			case grouping.OnTimestampColumn:
				value = s.numericCellValue(aggregate.GroupByValue.Pointer(), s.TimestampColumn.Type)
			case aggregate.GroupByValue == nil:
				value = nil
			default:
				column := s.DimensionColumns[grouping.ColumnIndex]
				value = s.numericCellValue(aggregate.GroupByValue.Pointer(), column.Type)
				if column.String {
					value = s.DimensionTables[grouping.ColumnIndex].Values[UntypedToInt(value)]
				}
			}
			row[query.Groupings[0].Name] = value
		}
		row["rowCount"] = aggregate.Count
		rows[i] = row
	}
	return rows
}

func (s *State) makeSumFunc(aggregate QueryAggregate, index int) sumFunc {
	col := s.MetricColumns[index]
	offset := s.MetricOffsets[index]
	return typeToSumFunc[col.Type](offset)
}

// makeTimeTruncationFunc returns a function which, given a cell, performs a date truncation transformation.
// intervalName should be one of "minute", "hour", or "day".
func (s *State) makeTimeTruncationFunc(truncationType TimeTruncationType,
	column Column) (transformFunc, error) {
	if column.Type != TypeUint32 {
		return nil, errors.New("cannot apply timestamp truncation to non-uint32 column")
	}
	var divisor int
	switch truncationType {
	case TimeTruncationMinute:
		divisor = 60
	case TimeTruncationHour:
		divisor = 60 * 60
	case TimeTruncationDay:
		divisor = 60 * 60 * 24
	}
	return func(cell unsafe.Pointer, isNil bool) Untyped {
		if isNil {
			return nil
		}
		value := int(*(*uint32)(cell))
		return value - (value % divisor)
	}, nil
}

func (s *State) makeTimestampFilterFunc(filter QueryFilter) (timestampFilterFunc, error) {
	panic("unimplemented")
}

func (s *State) makeDimensionFilterFunc(filter QueryFilter, index int) (filterFunc, error) {
	panic("unimplemented")
}

func (s *State) makeMetricFilterFunc(filter QueryFilter, index int) (filterFunc, error) {
	if filter.Type == FilterIn {
		panic("unimplemented")
	}

	value, ok := filter.Value.(float64)
	if !ok {
		return nil, fmt.Errorf("need a numeric value for filter comparisons; got %v", filter.Value)
	}
	col := s.MetricColumns[index]
	offset := s.MetricStartOffset + s.MetricOffsets[index]
	filterGenFunc := typeAndFilterToMetricFilterFuncSimple[typeAndFilter{col.Type, filter.Type}]
	return filterGenFunc(value, offset), nil
}

// Given a QueryFilter, return a filter function that can be tested against a row.
//func convertQueryFilterToFilterFunc(queryFilter QueryFilter, table *FactTable) FactTableFilterFunc {
//columnIndex := table.ColumnNameToIndex[queryFilter.Column]
//var f FactTableFilterFunc

//// The query value can either be a single value (in the case of =, >, < queries) or an array of values (in
//// the case of "in", "not in" queries.
//var value float64
//var values []float64

//queryValueIsList := queryFilter.Type == "in"

//// TODO(philc): Enforce that the argument is a string when the column itself is a string type.
//if queryValueIsList {
//untypedQueryValues := queryFilter.Value.([]interface{})
//shouldTranslateToDimensionColumnIds := len(untypedQueryValues) > 0 && isString(untypedQueryValues[0])
//if shouldTranslateToDimensionColumnIds {
//// Convert this slice of untyped objects to []string. We encounter a panic if we try to cast straight
//// to []string; I'm not sure why.
//valuesAsStrings := make([]string, 0, len(untypedQueryValues))
//for _, value := range untypedQueryValues {
//valuesAsStrings = append(valuesAsStrings, value.(string))
//}
//dimensionTable := table.DimensionTables[queryFilter.Column]
//values = dimensionTable.getDimensionRowIdsForValues(valuesAsStrings)
//} else {
//values = make([]float64, 0, len(untypedQueryValues))
//for _, value := range untypedQueryValues {
//values = append(values, convertUntypedToFloat64(value))
//}
//}
//} else {
//if isString(queryFilter.Value) {
//dimensionTable := table.DimensionTables[queryFilter.Column]
//matchingRowIds := dimensionTable.getDimensionRowIdsForValues([]string{queryFilter.Value.(string)})
//if len(matchingRowIds) == 0 {
//return func(row uintptr) bool { return false }
//} else {
//value = matchingRowIds[0]
//}
//} else {
//value = convertUntypedToFloat64(queryFilter.Value)
//}
//}

//columnOffset := table.ColumnIndexToOffset[columnIndex]
//columnType := table.ColumnIndexToType[columnIndex]

//// NOTE(philc): This list of filters is duplicated in query_parser.go.
//switch queryFilter.Type {
//case "=":
//f = func(row uintptr) bool {
//return !table.columnIsNil(row, columnIndex) &&
//getColumnValueAsFloat64(row, columnOffset, columnType) == value
//}
//case "!=":
//f = func(row uintptr) bool {
//return !table.columnIsNil(row, columnIndex) &&
//getColumnValueAsFloat64(row, columnOffset, columnType) != value
//}
//case ">":
//f = func(row uintptr) bool {
//return !table.columnIsNil(row, columnIndex) &&
//getColumnValueAsFloat64(row, columnOffset, columnType) > value
//}
//case ">=":
//f = func(row uintptr) bool {
//return !table.columnIsNil(row, columnIndex) &&
//getColumnValueAsFloat64(row, columnOffset, columnType) >= value
//}
//case "<":
//f = func(row uintptr) bool {
//return !table.columnIsNil(row, columnIndex) &&
//getColumnValueAsFloat64(row, columnOffset, columnType) < value
//}
//case "<=":
//f = func(row uintptr) bool {
//return !table.columnIsNil(row, columnIndex) &&
//getColumnValueAsFloat64(row, columnOffset, columnType) <= value
//}
//case "in":
//count := len(values)
//// TODO(philc): A hash table may be more efficient for longer lists. We should determine what that list
//// size is and use a hash table in that case.
//f = func(row uintptr) bool {
//if table.columnIsNil(row, columnIndex) {
//return false
//}
//columnValue := getColumnValueAsFloat64(row, columnOffset, columnType)
//for i := 0; i < count; i++ {
//if columnValue == values[i] {
//return true
//}
//}
//return false
//}
//}
//return f
//}
