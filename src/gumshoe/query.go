// Query execution functions.

package gumshoe

import (
	"errors"
	"fmt"
	"sync"
	"time"
	"unsafe"
)

// UntypedBytes is some numeric type which is set and modified unsafely. We know its type because we know the
// corresponding column in context.
type UntypedBytes []byte

func (u UntypedBytes) Pointer() unsafe.Pointer { return unsafe.Pointer(&u[0]) }

type rowAggregate struct {
	GroupByValue Untyped
	Sums         []Untyped // Corresponds to query.Aggregates
	Count        uint32
}

func (db *DB) runQuerySegmentWorker() {
	for {
		select {
		case fn := <-db.querySegmentJobs:
			fn()
		case <-db.shutdown:
			return
		}
	}
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

// InvokeQuery runs query on a StaticTable. It returns a slice of aggregated row results.
func (s *StaticTable) InvokeQuery(query *Query) ([]RowMap, error) {
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
		grouping != nil, len(timestampFilterFuncs), len(sumColumns), len(filterFuncs))

	start := time.Now()
	var rows []*rowAggregate
	var stats *scanStats
	if grouping == nil {
		rows, stats = s.scan(params)
	} else {
		rows, stats = s.scanWithGrouping(params)
	}
	Log.Printf("Query: scan completed in %s; %d intervals skipped; %d intervals scanned; %d rows scanned",
		time.Since(start), stats.IntervalsSkipped, stats.IntervalsScanned, stats.RowsScanned)

	return s.postProcessScanRows(rows, query, grouping), nil
}

type scanStats struct {
	IntervalsSkipped int
	IntervalsScanned int
	RowsScanned      int
}

type scanPartial struct {
	Sums  []UntypedBytes
	Count uint32
}

func makeScanPartial(params *scanParams) *scanPartial {
	partial := &scanPartial{Sums: make([]UntypedBytes, len(params.SumColumns))}
	for i, col := range params.SumColumns {
		partial.Sums[i] = make(UntypedBytes, typeWidths[typeToBigType[col.Type]])
	}
	return partial
}

func combineScanPartials(results []*scanPartial, params *scanParams, groupByValue Untyped) *rowAggregate {
	result := &rowAggregate{
		GroupByValue: groupByValue,
		Sums:         make([]Untyped, len(params.SumColumns)),
	}
	for i, col := range params.SumColumns {
		result.Sums[i] = untypedZero(typeToBigType[col.Type])
	}
	for _, partial := range results {
		for i, col := range params.SumColumns {
			typ := typeToBigType[col.Type]
			partialSum := numericCellValue(partial.Sums[i].Pointer(), typ)
			result.Sums[i] = sumUntyped(result.Sums[i], partialSum, typ)
		}
		result.Count += partial.Count
	}
	return result
}

func (s *StaticTable) scan(params *scanParams) ([]*rowAggregate, *scanStats) {
	stats := new(scanStats)
	var partials []*scanPartial
	wg := new(sync.WaitGroup)

intervalLoop:
	for timestamp, interval := range s.Intervals {
		if !params.AllTimestampFilterFuncsMatch(timestamp) {
			stats.IntervalsSkipped++
			continue intervalLoop
		}
		stats.IntervalsScanned++

		for _, segment := range interval.Segments {
			segment := segment // Create a fresh loop variable so the closure below captures the correct segment
			stats.RowsScanned += len(segment.Bytes) / s.RowSize

			partial := makeScanPartial(params)
			partials = append(partials, partial)
			wg.Add(1)

			s.querySegmentJobs <- func() {
				defer wg.Done()
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
						sumFn(partial.Sums[i], metrics)
					}

					partial.Count += row.count(s.Schema)
				}
			}
		}
	}

	wg.Wait()
	return []*rowAggregate{combineScanPartials(partials, params, nil)}, stats
}

func (s *StaticTable) scanWithGrouping(params *scanParams) ([]*rowAggregate, *scanStats) {
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

	// Only support computing the max value of 8 and 16 bit unsigned types, since that set of values can
	// efficiently be mapped to slice indices for the purposes of groupingOptions.
	var sliceGroupPartials [][]*scanPartial
	var nilGroupPartials []*scanPartial // used with sliceGroupPartials
	var mapGroupPartials []map[Untyped]*scanPartial
	stats := new(scanStats)

	width := groupingColumn.Width
	sliceGroupSize := 1 << uint(8*width)
	useSlice := (width <= 2 && transformFunc == nil)

	wg := new(sync.WaitGroup)

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
		var timestampGroupMapKey Untyped
		if groupOnTimestampColumn {
			groupTimestamp := uint32(timestamp.Unix())
			if transformFunc == nil {
				timestampGroupMapKey = groupTimestamp
			} else {
				timestampGroupMapKey = transformFunc(unsafe.Pointer(&groupTimestamp))
			}
		}
		// TODO(caleb): We can hoist the computation of the group row aggregate for timestamp groupings out to
		// this level as well, but it causes code duplication without a huge amount of gain so I'm leaving it
		// alone for now.

		for _, segment := range interval.Segments {
			segment := segment
			stats.RowsScanned += len(segment.Bytes) / s.RowSize

			var (
				sliceGroupPartial    []*scanPartial
				nilGroupPartial      *scanPartial
				nilGroupPartialIndex int
				mapGroupPartial      map[Untyped]*scanPartial
			)
			if useSlice {
				sliceGroupPartial = make([]*scanPartial, sliceGroupSize)
				sliceGroupPartials = append(sliceGroupPartials, sliceGroupPartial)
				nilGroupPartialIndex = len(nilGroupPartials)
				nilGroupPartials = append(nilGroupPartials, nilGroupPartial)
			} else {
				mapGroupPartial = make(map[Untyped]*scanPartial)
				mapGroupPartials = append(mapGroupPartials, mapGroupPartial)
			}
			wg.Add(1)

			var partial *scanPartial
			var groupMapKey Untyped

			s.querySegmentJobs <- func() {
				defer wg.Done()
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
					switch {
					case groupOnTimestampColumn:
						partial = mapGroupPartial[timestampGroupMapKey]
						if partial == nil {
							partial = makeScanPartial(params)
							mapGroupPartial[timestampGroupMapKey] = partial
						}
					case useSlice:
						if row[nilOffset]&nilMask > 0 {
							partial = nilGroupPartial
							if partial == nil {
								partial = makeScanPartial(params)
								nilGroupPartial = partial
								nilGroupPartials[nilGroupPartialIndex] = partial
							}
						} else {
							cell := unsafe.Pointer(&row[valueOffset])
							index := getDimensionValueAsIntFunc(cell)
							partial = sliceGroupPartial[index]
							if partial == nil {
								partial = makeScanPartial(params)
								sliceGroupPartial[index] = partial
							}
						}
					default: // map grouping, not on timestamp column
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
						partial = mapGroupPartial[groupMapKey]
						if partial == nil {
							partial = makeScanPartial(params)
							mapGroupPartial[groupMapKey] = partial
						}
					}

					// Sum each aggregate metric.
					metrics := MetricBytes(row[s.MetricStartOffset:])
					for i, sumFn := range params.SumFuncs {
						sumFn(partial.Sums[i], metrics)
					}

					partial.Count += row.count(s.Schema)
				}
			}
		}
	}

	wg.Wait()
	if useSlice {
		return mergeSliceGroupPartials(sliceGroupPartials, nilGroupPartials, params), stats
	}
	return mergeMapGroupPartials(mapGroupPartials, params), stats
}

func mergeSliceGroupPartials(sliceGroupPartials [][]*scanPartial,
	nilGroupPartials []*scanPartial, params *scanParams) []*rowAggregate {

	var results []*rowAggregate

	var validNilGroupPartials []*scanPartial
	for _, partial := range nilGroupPartials {
		if partial != nil {
			validNilGroupPartials = append(validNilGroupPartials, partial)
		}
	}
	if len(validNilGroupPartials) > 0 {
		results = append(results, combineScanPartials(validNilGroupPartials, params, nil))
	}

	if len(sliceGroupPartials) > 0 {
		sliceGroupSize := len(sliceGroupPartials[0])
		for i := 0; i < sliceGroupSize; i++ {
			var singleIndexPartials []*scanPartial
			for _, partials := range sliceGroupPartials {
				if partial := partials[i]; partial != nil {
					singleIndexPartials = append(singleIndexPartials, partial)
				}
			}
			if len(singleIndexPartials) > 0 {
				results = append(results, combineScanPartials(singleIndexPartials, params, i))
			}
		}
	}
	return results
}

func mergeMapGroupPartials(mapGroupPartials []map[Untyped]*scanPartial, params *scanParams) []*rowAggregate {
	allKeys := make(map[Untyped]struct{})
	for _, partialMap := range mapGroupPartials {
		for k := range partialMap {
			allKeys[k] = struct{}{}
		}
	}
	var results []*rowAggregate
	for k := range allKeys {
		var partials []*scanPartial
		for _, partialMap := range mapGroupPartials {
			if partial := partialMap[k]; partial != nil {
				partials = append(partials, partial)
			}
		}
		if len(partials) > 0 {
			results = append(results, combineScanPartials(partials, params, k))
		}
	}
	return results
}

func (s *StaticTable) postProcessScanRows(aggregates []*rowAggregate, query *Query,
	grouping *groupingParams) []RowMap {
	rows := make([]RowMap, len(aggregates))
	for i, aggregate := range aggregates {
		row := make(RowMap)
		for i, queryAggregate := range query.Aggregates {
			switch queryAggregate.Type {
			case AggregateSum:
				row[queryAggregate.Name] = aggregate.Sums[i]
			case AggregateAvg:
				row[queryAggregate.Name] = UntypedToFloat64(aggregate.Sums[i]) / float64(aggregate.Count)
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

func (s *StaticTable) makeSumFunc(aggregate QueryAggregate, index int) sumFunc {
	col := s.MetricColumns[index]
	offset := s.MetricOffsets[index]
	return makeSumFuncGen(col.Type)(offset)
}

// makeTimeTruncationFunc returns a function which, given a cell, performs a date truncation transformation.
// intervalName should be one of "minute", "hour", or "day".
func (s *StaticTable) makeTimeTruncationFunc(truncationType TimeTruncationType,
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

func (s *StaticTable) makeTimestampFilterFunc(filter QueryFilter) (timestampFilterFunc, error) {
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

func (s *StaticTable) makeTimestampFilterFuncIn(filter QueryFilter) (timestampFilterFunc, error) {
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

func (s *StaticTable) makeDimensionFilterFunc(filter QueryFilter, index int) (filterFunc, error) {
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

func (s *StaticTable) makeDimensionFilterFuncIn(filter QueryFilter, index int) (filterFunc, error) {
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

func (s *StaticTable) makeMetricFilterFunc(filter QueryFilter, index int) (filterFunc, error) {
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

func (s *StaticTable) makeMetricFilterFuncIn(filter QueryFilter, index int) (filterFunc, error) {
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
