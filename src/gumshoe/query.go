// Query execution functions.

package gumshoe

import (
	"errors"
	"fmt"
	"log"
	"time"
	"unsafe"
)

// UntypedBytes is some numeric type which is set and modified unsafely. We know its type because we know the
// corresponding column in context.
type UntypedBytes []byte

func (u UntypedBytes) Pointer() unsafe.Pointer { return unsafe.Pointer(&u[0]) }

type rowAggregate struct {
	GroupByValue Untyped
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
}

type (
	transformFunc       func(cell unsafe.Pointer) Untyped
	filterFunc          func(row RowBytes) bool
	timestampFilterFunc func(timestamp uint32) bool
	sumFunc             func(sum UntypedBytes, metrics MetricBytes)
)

// TODO(caleb): Wherever we use falseFilterFunc, we can optimize by immediately returning an empty result.
var falseFilterFunc = func(RowBytes) bool { return false }

func (p *scanParams) AllTimestampFilterFuncsMatch(intervalTimestamp time.Time) bool {
	timestamp := uint32(intervalTimestamp.Unix())
	for _, f := range p.TimestampFilterFuncs {
		if !f(timestamp) {
			return false
		}
	}
	return true
}

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

	Log.Printf("Query: grouping=%t, %d timestamp filter funcs, %d sum columns, %d filter funcs",
		grouping != nil, len(sumColumns), len(filterFuncs), len(timestampFilterFuncs))

	start := time.Now()
	var rows []*rowAggregate
	var stats *scanStats
	if grouping == nil {
		rows, stats = s.scan(params)
	} else {
		rows, stats = s.scanWithGrouping(params)
	}
	log.Printf("Query: scan completed in %s; %d intervals skipped; %d intervals scanned; %d rows scanned",
		time.Since(start), stats.IntervalsSkipped, stats.IntervalsScanned, stats.RowsScanned)

	return s.postProcessScanRows(rows, query, grouping), nil
}

type scanStats struct {
	IntervalsSkipped int
	IntervalsScanned int
	RowsScanned      int
}

func (s *State) scan(params *scanParams) ([]*rowAggregate, *scanStats) {
	result := new(rowAggregate)
	stats := new(scanStats)
	result.Sums = make([]UntypedBytes, len(params.SumColumns))
	for i, col := range params.SumColumns {
		result.Sums[i] = make(UntypedBytes, typeWidths[typeToBigType[col.Type]])
	}

intervalLoop:
	for timestamp, interval := range s.Intervals {
		if !params.AllTimestampFilterFuncsMatch(timestamp) {
			stats.IntervalsSkipped++
			continue intervalLoop
		}
		stats.IntervalsScanned++

		for _, segment := range interval.Segments {
			stats.RowsScanned += len(segment.Bytes) / s.RowSize
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
	return []*rowAggregate{result}, stats
}

func (s *State) scanWithGrouping(params *scanParams) ([]*rowAggregate, *scanStats) {
	// Only support computing the max value of 8 and 16 bit unsigned types, since that set of values can
	// efficiently be mapped to slice indices for the purposes of groupingOptions.
	var sliceGroups []*rowAggregate
	var nilGroup *rowAggregate // used with sliceGroups
	var mapGroups map[Untyped]*rowAggregate
	stats := new(scanStats)

	var groupingColumn Column
	if params.Grouping.OnTimestampColumn {
		groupingColumn = s.TimestampColumn
	} else {
		groupingColumn = s.DimensionColumns[params.Grouping.ColumnIndex].Column
	}

	var nilOffset, valueOffset int
	var nilMask byte
	groupOnTimestampColumn := params.Grouping.OnTimestampColumn
	transformFunc := params.Grouping.TransformFunc
	if !groupOnTimestampColumn {
		i := params.Grouping.ColumnIndex
		nilOffset = s.DimensionStartOffset + i/8
		nilMask = 1 << byte(i%8)
		valueOffset = s.DimensionStartOffset + s.DimensionOffsets[i]
	}
	getDimensionValueFunc := makeGetDimensionValueFuncGen(groupingColumn.Type)
	getDimensionValueAsIntFunc := makeGetDimensionValueAsIntFuncGen(groupingColumn.Type)

	useSlice := false
	width := groupingColumn.Width
	if width <= 2 && transformFunc == nil {
		useSlice = true
		sliceGroups = make([]*rowAggregate, 1<<uint(8*width))
	} else {
		mapGroups = make(map[Untyped]*rowAggregate)
	}

	// A quick sanity check -- this should never be the case as long as timestamps are uint32.
	if groupOnTimestampColumn && useSlice {
		panic("using slices for timestamp column group is unhandled")
	}

intervalLoop:
	for timestamp, interval := range s.Intervals {
		if !params.AllTimestampFilterFuncsMatch(timestamp) {
			stats.IntervalsSkipped++
			continue intervalLoop
		}
		stats.IntervalsScanned++
		var groupMapKey Untyped
		var aggregate *rowAggregate // Corresponding to groupValue
		if groupOnTimestampColumn {
			groupTimestamp := uint32(timestamp.Unix())
			if transformFunc == nil {
				groupMapKey = groupTimestamp
			} else {
				groupMapKey = transformFunc(unsafe.Pointer(&groupTimestamp))
			}
		}
		// TODO(caleb): We can hoist the computation of the group row aggregate for timestamp groupings out to
		// this level as well, but it causes code duplication without a huge amount of gain so I'm leaving it
		// alone for now.

		for _, segment := range interval.Segments {
			stats.RowsScanned += len(segment.Bytes) / s.RowSize
		rowLoop:
			for i := 0; i < len(segment.Bytes); i += s.RowSize {
				row := RowBytes(segment.Bytes[i : i+s.RowSize])

				// Run each filter to see if we should skip this row.
				for _, filter := range params.FilterFuncs {
					if !filter(row) {
						continue rowLoop
					}
				}

				// Perform grouping.
				if useSlice {
					if row[nilOffset]&nilMask > 0 {
						aggregate = nilGroup
						if aggregate == nil {
							aggregate = makeRowAggregate(nil, params)
							nilGroup = aggregate
						}
					} else {
						cell := unsafe.Pointer(&row[valueOffset])
						index := getDimensionValueAsIntFunc(cell)
						aggregate = sliceGroups[index]
						if aggregate == nil {
							aggregate = makeRowAggregate(index, params)
							sliceGroups[index] = aggregate
						}
					}
				} else {
					if !groupOnTimestampColumn {
						if row[nilOffset]&nilMask > 0 {
							groupMapKey = nil // just to be explicit about things
						} else {
							cell := unsafe.Pointer(&row[valueOffset])
							if transformFunc != nil {
								groupMapKey = transformFunc(cell)
							} else {
								groupMapKey = getDimensionValueFunc(cell)
							}
						}
					}
					// Now we've set groupMapKey in both cases (whether or not we're grouping on the timestamp column).
					aggregate = mapGroups[groupMapKey]
					if aggregate == nil {
						aggregate = makeRowAggregate(groupMapKey, params)
						mapGroups[groupMapKey] = aggregate
					}
				}

				// Sum each aggregate metric.
				metrics := MetricBytes(row[s.MetricStartOffset:])
				for i, sumFn := range params.SumFuncs {
					sumFn(aggregate.Sums[i], metrics)
				}

				aggregate.Count += row.count(s.Schema)
			}
		}
	}

	var results []*rowAggregate
	if useSlice {
		if nilGroup != nil {
			results = append(results, nilGroup)
		}
		// Prune the grouping slice
		for _, aggregate := range sliceGroups {
			if aggregate != nil {
				results = append(results, aggregate)
			}
		}
	} else {
		for _, aggregate := range mapGroups {
			results = append(results, aggregate)
		}
	}
	return results, stats
}

func makeRowAggregate(groupByValue Untyped, params *scanParams) *rowAggregate {
	aggregate := new(rowAggregate)
	aggregate.GroupByValue = groupByValue
	aggregate.Sums = make([]UntypedBytes, len(params.SumColumns))
	for i, col := range params.SumColumns {
		aggregate.Sums[i] = make(UntypedBytes, typeWidths[typeToBigType[col.Type]])
	}
	return aggregate
}

func (s *State) postProcessScanRows(aggregates []*rowAggregate, query *Query,
	grouping *groupingParams) []RowMap {
	rows := make([]RowMap, len(aggregates))
	for i, aggregate := range aggregates {
		row := make(RowMap)
		for i, queryAggregate := range query.Aggregates {
			index := s.MetricNameToIndex[queryAggregate.Column]
			column := s.MetricColumns[index]
			sum := numericCellValue(aggregate.Sums[i].Pointer(), typeToBigType[column.Type])
			switch queryAggregate.Type {
			case AggregateSum:
				row[queryAggregate.Name] = sum
			case AggregateAvg:
				row[queryAggregate.Name] = UntypedToFloat64(sum) / float64(aggregate.Count)
			}
		}
		if grouping != nil {
			var value Untyped = aggregate.GroupByValue
			if aggregate.GroupByValue != nil && !grouping.OnTimestampColumn {
				col := s.DimensionColumns[grouping.ColumnIndex]
				if col.String {
					dimensionIndex := UntypedToInt(aggregate.GroupByValue)
					value = s.DimensionTables[grouping.ColumnIndex].Values[dimensionIndex]
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
	return makeSumFuncGen(col.Type)(offset)
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
	return func(cell unsafe.Pointer) Untyped {
		value := int(*(*uint32)(cell))
		return value - (value % divisor)
	}, nil
}

func (s *State) makeTimestampFilterFunc(filter QueryFilter) (timestampFilterFunc, error) {
	if filter.Type == FilterIn {
		return s.makeTimestampFilterFuncIn(filter)
	}

	value, ok := filter.Value.(float64)
	if !ok {
		return nil, fmt.Errorf("timestamp column filters must be numeric; got %v", filter.Value)
	}
	timestamp := uint32(value)
	return makeTimestampFilterFuncSimpleGen(filter.Type)(timestamp), nil
}

func (s *State) makeTimestampFilterFuncIn(filter QueryFilter) (timestampFilterFunc, error) {
	values, ok := filter.Value.([]interface{})
	if !ok {
		return nil, fmt.Errorf("timestamp column 'in' filter must be given an array; got %v", filter.Value)
	}
	timestamps := make([]uint32, len(values))
	for i, v := range values {
		float, ok := v.(float64)
		if !ok {
			return nil, fmt.Errorf("timestamp column 'in' filter list must include numeric values; got %v", v)
		}
		timestamps[i] = uint32(float)
	}
	return func(timestamp uint32) bool {
		for _, t := range timestamps {
			if t == timestamp {
				return true
			}
		}
		return false
	}, nil
}

func (s *State) makeDimensionFilterFunc(filter QueryFilter, index int) (filterFunc, error) {
	if filter.Type == FilterIn {
		return s.makeDimensionFilterFuncIn(filter, index)
	}

	col := s.DimensionColumns[index]
	mask := byte(1) << byte(index%8)
	nilOffset := s.DimensionStartOffset + index/8
	valueOffset := s.DimensionStartOffset + s.DimensionOffsets[index]

	// Comparison table: (x is some not-nil value, OP is some operator that is not '=' or '!=')
	// nil	=		x		false
	// nil	!=	x		true
	// nil	OP	x		false
	// nil	=		nil	true
	// nil	!=	nil	false
	// nil	OP	nil	false

	if filter.Value == nil {
		return makeNilFilterFuncSimpleGen(col.Type, filter.Type)(nilOffset, mask), nil
	}

	// For string columns, value will be a precise uint32 dimension table index; otherwise it will be a float as
	// usual for numeric types we get from JSON.
	var value interface{}
	isString := false

	if col.String {
		str, ok := filter.Value.(string)
		if !ok {
			return nil, fmt.Errorf("need a string value to filter column %q; got %v", col.Name, filter.Value)
		}
		dimIndex, ok := s.DimensionTables[index].Get(str)
		if !ok {
			return falseFilterFunc, nil
		}
		value = dimIndex
		isString = true
	} else {
		float, ok := filter.Value.(float64)
		if !ok {
			return nil, fmt.Errorf("need a numeric value to filter column %q; got %v", col.Name, filter.Value)
		}
		value = float
	}
	filterGenFunc := makeDimensionFilterFuncSimpleGen(col.Type, filter.Type, isString)
	return filterGenFunc(value, nilOffset, mask, valueOffset), nil
}

func (s *State) makeDimensionFilterFuncIn(filter QueryFilter, index int) (filterFunc, error) {
	valueSlice, ok := filter.Value.([]interface{})
	if !ok {
		return nil, fmt.Errorf("'in' queries require a list for comparison; got %v", filter.Value)
	}
	if len(valueSlice) == 0 {
		return falseFilterFunc, nil
	}

	col := s.DimensionColumns[index]
	mask := byte(1) << byte(index%8)
	nilOffset := s.DimensionStartOffset + index/8
	valueOffset := s.DimensionStartOffset + s.DimensionOffsets[index]
	acceptNil := false
	isString := false
	// For string columns, values is []uint32, otherwise it's []float64.
	var values interface{}

	if col.String {
		var dimIndices []uint32
		for _, v := range valueSlice {
			if v == nil {
				acceptNil = true
				continue
			}
			str, ok := v.(string)
			if !ok {
				return nil, fmt.Errorf("'in' queries on dimension %q take string or null values; got %v", col.Name, v)
			}
			if dimIndex, ok := s.DimensionTables[index].Get(str); ok {
				dimIndices = append(dimIndices, dimIndex)
			}
		}
		if len(dimIndices) == 0 && !acceptNil {
			return falseFilterFunc, nil
		}
		values = dimIndices
		isString = true
	} else {
		var floats []float64
		for _, v := range valueSlice {
			if v == nil {
				acceptNil = true
				continue
			}
			float, ok := v.(float64)
			if !ok {
				err := fmt.Errorf("'in' queries on dimension %q take numeric or null values; got %v", col.Name, v)
				return nil, err
			}
			floats = append(floats, float)
		}
		values = floats
	}

	filterGenFunc := makeDimensionFilterFuncInGen(col.Type, isString)
	return filterGenFunc(values, acceptNil, nilOffset, mask, valueOffset), nil
}

func (s *State) makeMetricFilterFunc(filter QueryFilter, index int) (filterFunc, error) {
	if filter.Type == FilterIn {
		return s.makeMetricFilterFuncIn(filter, index)
	}

	float, ok := filter.Value.(float64)
	if !ok {
		return nil, fmt.Errorf("need a numeric value for metric filter comparisons; got %v", filter.Value)
	}
	col := s.MetricColumns[index]
	offset := s.MetricStartOffset + s.MetricOffsets[index]
	return makeMetricFilterFuncSimpleGen(col.Type, filter.Type)(float, offset), nil
}

func (s *State) makeMetricFilterFuncIn(filter QueryFilter, index int) (filterFunc, error) {
	values, ok := filter.Value.([]interface{})
	if !ok {
		return nil, fmt.Errorf("'in' queries require a list for comparison; got %v", filter.Value)
	}
	if len(values) == 0 {
		return falseFilterFunc, nil
	}
	floats := make([]float64, len(values))
	for i, v := range values {
		float, ok := v.(float64)
		if !ok {
			return nil, fmt.Errorf("'in' queries on metric columns take numeric values only; got %v", v)
		}
		floats[i] = float
	}
	col := s.MetricColumns[index]
	offset := s.MetricStartOffset + s.MetricOffsets[index]
	// TODO(philc): A hash table may be more efficient for longer lists. We should determine what that list
	// size is and use a hash table in that case.
	return makeMetricFilterFuncInGen(col.Type)(floats, offset), nil
}
