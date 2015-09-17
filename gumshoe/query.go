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
	Log.Println("Running query:", query)
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
	rows, stats := s.scan(params)
	Log.Printf("Query: scan completed in %s; %d intervals skipped; %d intervals scanned; %d rows scanned",
		time.Since(start), stats.Get(statIntervalsSkipped), stats.Get(statIntervalsScanned),
		stats.Get(statRowsScanned))

	return s.postProcessScanRows(rows, query, grouping), nil
}

type scanPartial struct {
	Sums  []UntypedBytes
	Count uint32
}

func makeScanPartial(params *scanParams) *scanPartial {
	partial := &scanPartial{Sums: make([]UntypedBytes, len(params.SumColumns))}
	for i, col := range params.SumColumns {
		partial.Sums[i] = make(UntypedBytes, typeWidths[TypeToBigType[col.Type]])
	}
	return partial
}

func combineScanPartials(results []*scanPartial, params *scanParams, groupByValue Untyped) *rowAggregate {
	result := &rowAggregate{
		GroupByValue: groupByValue,
		Sums:         make([]Untyped, len(params.SumColumns)),
	}
	for i, col := range params.SumColumns {
		result.Sums[i] = untypedZero(TypeToBigType[col.Type])
	}
	for _, partial := range results {
		for i, col := range params.SumColumns {
			typ := TypeToBigType[col.Type]
			partialSum := NumericCellValue(partial.Sums[i].Pointer(), typ)
			result.Sums[i] = sumUntyped(result.Sums[i], partialSum, typ)
		}
		result.Count += partial.Count
	}
	return result
}

type intervalScanJob struct {
	timestamp time.Time
	interval  *Interval
}

type scanState struct {
	stats     *scanStats
	params    *scanParams
	jobs      chan *intervalScanJob
	partialCh chan interface{}
	wg        *sync.WaitGroup
}

func (s *StaticTable) scan(params *scanParams) ([]*rowAggregate, *scanStats) {
	state := &scanState{
		stats:     newScanStats(),
		params:    params,
		jobs:      make(chan *intervalScanJob),
		partialCh: make(chan interface{}),
		wg:        new(sync.WaitGroup),
	}

	var scanFunc func(*scanState)
	var combineFunc func(partials []interface{}, params *scanParams) []*rowAggregate

	switch {
	case params.Grouping == nil:
		scanFunc = s.scanSimple
		combineFunc = combineSimple
	case s.useSliceGrouping(params):
		scanFunc = s.scanSliceGrouping
		combineFunc = combineSliceGrouping
	default:
		scanFunc = s.scanMapGrouping
		combineFunc = combineMapGrouping
	}

	for i := 0; i < s.QueryParallelism; i++ {
		state.wg.Add(1)
		go scanFunc(state)
	}

	var partials []interface{}
	done := make(chan struct{})
	go func() {
		for partial := range state.partialCh {
			partials = append(partials, partial)
		}
		done <- struct{}{}
	}()

	for timestamp, interval := range s.Intervals {
		if !params.AllTimestampFilterFuncsMatch(timestamp) {
			state.stats.Inc(statIntervalsSkipped)
			continue
		}
		state.stats.Inc(statIntervalsScanned)
		state.jobs <- &intervalScanJob{timestamp, interval}
	}

	close(state.jobs)
	state.wg.Wait()
	close(state.partialCh)
	<-done

	return combineFunc(partials, params), state.stats
}

// sliceGroupingSizeLimit is the cardinality of string dimension
// beyond which we use a map, rather than a slice, for grouping.
// It's a var rather than a const so tests can adjust it.
// This value was chosen as:
//   500k * 8 bytes / pointer = 4MB max slice allocation per partial.
var sliceGroupingSizeLimit int = 500e3

func (s *StaticTable) useSliceGrouping(params *scanParams) bool {
	// TODO(caleb): We should be able to use slice groupings here.
	// It requires a two-phase grouping:
	// - Scan using a slice to group on the un-transformed dimension value
	// - Collapse into the final result by grouping on the transformed values
	if params.Grouping.TransformFunc != nil {
		return false
	}
	// TODO(caleb): We should definitely be able to use a slice for timestamp grouping.
	if params.Grouping.OnTimestampColumn {
		return false
	}
	groupingColumn := s.DimensionColumns[params.Grouping.ColumnIndex]
	if groupingColumn.Width <= 2 {
		return true
	}
	return groupingColumn.String &&
		s.DimensionTables[params.Grouping.ColumnIndex].Size <= sliceGroupingSizeLimit
}

func (s *StaticTable) scanSimple(state *scanState) {
	defer state.wg.Done()

	filterFuncs := state.params.FilterFuncs
	sumFuncs := state.params.SumFuncs
	for job := range state.jobs {
		partial := makeScanPartial(state.params)
		for _, segment := range job.interval.Segments {
			state.stats.Add(statRowsScanned, len(segment.Bytes)/s.RowSize)

		rowLoop:
			for i := 0; i < len(segment.Bytes); i += s.RowSize {
				row := RowBytes(segment.Bytes[i : i+s.RowSize])

				// Run each filter to see if we should skip this row.
				for _, filter := range filterFuncs {
					if !filter(row) {
						continue rowLoop
					}
				}

				// Sum each aggregate metric.
				metrics := MetricBytes(row[s.MetricStartOffset:])
				for i, sumFn := range sumFuncs {
					sumFn(partial.Sums[i], metrics)
				}

				partial.Count += row.count(s.Schema)
			}
		}
		state.partialCh <- partial
	}
}

func combineSimple(partials []interface{}, params *scanParams) []*rowAggregate {
	ps := make([]*scanPartial, len(partials))
	for i, p := range partials {
		ps[i] = p.(*scanPartial)
	}
	return []*rowAggregate{combineScanPartials(ps, params, nil)}
}

type sliceGroupPartials struct {
	slicePartials []*scanPartial
	nilPartial    *scanPartial
}

func (s *StaticTable) scanSliceGrouping(state *scanState) {
	defer state.wg.Done()

	groupingColumn := s.DimensionColumns[state.params.Grouping.ColumnIndex]
	width := groupingColumn.Width
	var sliceGroupSize int
	switch {
	case groupingColumn.String:
		sliceGroupSize = s.DimensionTables[state.params.Grouping.ColumnIndex].Size
	case width <= 2:
		sliceGroupSize = 1 << uint(8*width)
	default:
		panic("trying to use slice grouping for wide (>2 byte), non-string column")
	}

	// Sanity checks.
	if state.params.Grouping.OnTimestampColumn {
		panic("using slices for timestamp column group is unhandled")
	}
	if state.params.Grouping.TransformFunc != nil {
		panic("using slices for grouping with transform func")
	}

	i := state.params.Grouping.ColumnIndex
	nilOffset := s.DimensionStartOffset + i/8
	nilMask := byte(1) << byte(i%8)
	valueOffset := s.DimensionStartOffset + s.DimensionOffsets[i]
	getDimensionValueAsIntFunc := makeGetDimensionValueAsIntFuncGen(groupingColumn.Type)
	filterFuncs := state.params.FilterFuncs
	sumFuncs := state.params.SumFuncs

	for job := range state.jobs {

		slicePartials := make([]*scanPartial, sliceGroupSize)
		var nilGroupPartial *scanPartial
		var partial *scanPartial // The current partial at each iteration

		for _, segment := range job.interval.Segments {
			state.stats.Add(statRowsScanned, len(segment.Bytes)/s.RowSize)

		rowLoop:
			for i := 0; i < len(segment.Bytes); i += s.RowSize {
				row := RowBytes(segment.Bytes[i : i+s.RowSize])

				// Run each filter to see if we should skip this row.
				for _, filter := range filterFuncs {
					if !filter(row) {
						continue rowLoop
					}
				}

				// Grouping
				if row[nilOffset]&nilMask > 0 {
					partial = nilGroupPartial
					if partial == nil {
						partial = makeScanPartial(state.params)
						nilGroupPartial = partial
					}
				} else {
					cell := unsafe.Pointer(&row[valueOffset])
					index := getDimensionValueAsIntFunc(cell)
					partial = slicePartials[index]
					if partial == nil {
						partial = makeScanPartial(state.params)
						slicePartials[index] = partial
					}
				}

				// Sum each aggregate metric.
				metrics := MetricBytes(row[s.MetricStartOffset:])
				for i, sumFn := range sumFuncs {
					sumFn(partial.Sums[i], metrics)
				}

				partial.Count += row.count(s.Schema)
			}
		}

		state.partialCh <- &sliceGroupPartials{slicePartials, nilGroupPartial}
	}
}

func combineSliceGrouping(boxedPartials []interface{}, params *scanParams) []*rowAggregate {
	partials := make([]*sliceGroupPartials, len(boxedPartials))
	for i, p := range boxedPartials {
		partials[i] = p.(*sliceGroupPartials)
	}

	var results []*rowAggregate

	var nilGroupPartials []*scanPartial
	for _, slicePartials := range partials {
		if slicePartials.nilPartial != nil {
			nilGroupPartials = append(nilGroupPartials, slicePartials.nilPartial)
		}
	}
	if len(nilGroupPartials) > 0 {
		results = append(results, combineScanPartials(nilGroupPartials, params, nil))
	}

	if len(partials) > 0 {
		sliceGroupSize := len(partials[0].slicePartials)
		for i := 0; i < sliceGroupSize; i++ {
			var singleIndexPartials []*scanPartial
			for _, slicePartials := range partials {
				if partial := slicePartials.slicePartials[i]; partial != nil {
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

func (s *StaticTable) scanMapGrouping(state *scanState) {
	defer state.wg.Done()

	var groupingColumn Column
	if state.params.Grouping.OnTimestampColumn {
		groupingColumn = s.TimestampColumn
	} else {
		groupingColumn = s.DimensionColumns[state.params.Grouping.ColumnIndex].Column
	}

	var nilOffset, valueOffset int
	var nilMask byte
	groupOnTimestampColumn := state.params.Grouping.OnTimestampColumn
	transformFunc := state.params.Grouping.TransformFunc
	if !groupOnTimestampColumn {
		i := state.params.Grouping.ColumnIndex
		nilOffset = s.DimensionStartOffset + i/8
		nilMask = 1 << byte(i%8)
		valueOffset = s.DimensionStartOffset + s.DimensionOffsets[i]
	}
	getDimensionValueFunc := makeGetDimensionValueFuncGen(groupingColumn.Type)
	filterFuncs := state.params.FilterFuncs
	sumFuncs := state.params.SumFuncs

	for job := range state.jobs {

		mapPartials := make(map[Untyped]*scanPartial)
		var partial *scanPartial
		var key Untyped

		// If we're grouping on the timestamp column, do that work out here.
		if groupOnTimestampColumn {
			groupTimestamp := uint32(job.timestamp.Unix())
			if transformFunc == nil {
				key = groupTimestamp
			} else {
				key = transformFunc(unsafe.Pointer(&groupTimestamp))
			}
			partial = makeScanPartial(state.params)
			mapPartials[key] = partial
		}

		for _, segment := range job.interval.Segments {
			state.stats.Add(statRowsScanned, len(segment.Bytes)/s.RowSize)

		rowLoop:
			for i := 0; i < len(segment.Bytes); i += s.RowSize {
				row := RowBytes(segment.Bytes[i : i+s.RowSize])

				// Run each filter to see if we should skip this row.
				for _, filter := range filterFuncs {
					if !filter(row) {
						continue rowLoop
					}
				}

				// Perform grouping.
				if !groupOnTimestampColumn {
					if row[nilOffset]&nilMask > 0 {
						key = nil // just to be explicit about things
					} else {
						cell := unsafe.Pointer(&row[valueOffset])
						if transformFunc != nil {
							key = transformFunc(cell)
						} else {
							key = getDimensionValueFunc(cell)
						}
					}
					partial = mapPartials[key]
					if partial == nil {
						partial = makeScanPartial(state.params)
						mapPartials[key] = partial
					}
				}

				// Sum each aggregate metric.
				metrics := MetricBytes(row[s.MetricStartOffset:])
				for i, sumFn := range sumFuncs {
					sumFn(partial.Sums[i], metrics)
				}

				partial.Count += row.count(s.Schema)
			}
		}

		state.partialCh <- mapPartials
	}
}

func combineMapGrouping(boxedPartials []interface{}, params *scanParams) []*rowAggregate {
	mapPartials := make([]map[Untyped]*scanPartial, len(boxedPartials))
	for i, p := range boxedPartials {
		mapPartials[i] = p.(map[Untyped]*scanPartial)
	}

	allKeys := make(map[Untyped]struct{})
	for _, mapPartial := range mapPartials {
		for k := range mapPartial {
			allKeys[k] = struct{}{}
		}
	}

	var results []*rowAggregate
	for k := range allKeys {
		var partials []*scanPartial
		for _, mapPartial := range mapPartials {
			if partial := mapPartial[k]; partial != nil {
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

type scanStat int

const (
	statIntervalsSkipped scanStat = iota
	statIntervalsScanned
	statRowsScanned
)

type scanStats struct {
	*sync.Mutex
	m map[scanStat]int
}

func newScanStats() *scanStats {
	return &scanStats{
		Mutex: new(sync.Mutex),
		m:     make(map[scanStat]int),
	}
}

func (s *scanStats) Add(key scanStat, delta int) {
	s.Lock()
	s.m[key]++
	s.Unlock()
}

func (s *scanStats) Inc(key scanStat) { s.Add(key, 1) }

func (s *scanStats) Get(key scanStat) int {
	s.Lock()
	defer s.Unlock()
	return s.m[key]
}
