// Implementations of the core scan function hard-coded to fulfill the quries from bench_test.go.
// This is supposed to demonstrate the performance gain of generating near-optimal scan code at runtime per
// query.
package gumshoe

import (
	"reflect"
	"unsafe"
)

// This assumes a filter on column002 > 0.
func (table *FactTable) scanInlineFilter(filters []FactTableFilterFunc, columnIndices []int,
	groupByColumnName string, groupByColumnTransformFn func(float64) float64) []RowAggregate {
	rowAggregate := *(new(RowAggregate))
	rowAggregate.Sums = make([]float64, table.ColumnCount)
	rowCount := table.Count
	rowPtr := (*reflect.SliceHeader)(unsafe.Pointer(&table.rows)).Data
	rowSize := uintptr(table.RowSize)
	column1 := table.ColumnNameToIndex["column001"]
	column1Offset := table.ColumnIndexToOffset[column1]
	column2 := table.ColumnNameToIndex["column002"]
	column2Offset := table.ColumnIndexToOffset[column2]
	var int64Sum int64

outerLoop:
	for i := 0; i < rowCount; i++ {
		// Nil check on column1
		nilBytePtr := unsafe.Pointer(rowPtr + uintptr(0))
		nilBitIndex := uint(1)
		isNil := *(*uint)(nilBytePtr) & (1 << (7 - nilBitIndex))
		if isNil > 0 {
			continue
		}

		// Apply filters
		column2Ptr := unsafe.Pointer(rowPtr + column2Offset)
		column2Value := *(*int32)(column2Ptr)
		if column2Value <= 0 {
			rowPtr += rowSize
			continue outerLoop
		}

		column1Ptr := unsafe.Pointer(rowPtr + column1Offset)
		column1Value := *(*int32)(column1Ptr)
		int64Sum += int64(column1Value)

		rowAggregate.Count++
		rowPtr += rowSize
	}
	rowAggregate.Sums[column1] = float64(int64Sum)

	results := []RowAggregate{}
	results = append(results, rowAggregate)
	return results
}

// This assumes a sum aggregate on column001.
func (table *FactTable) scanInlineAggregate(filters []FactTableFilterFunc, columnIndices []int,
	groupByColumnName string, groupByColumnTransformFn func(float64) float64) []RowAggregate {
	rowAggregate := *(new(RowAggregate))
	rowAggregate.Sums = make([]float64, table.ColumnCount)
	rowCount := table.Count
	rowPtr := (*reflect.SliceHeader)(unsafe.Pointer(&table.rows)).Data
	rowSize := uintptr(table.RowSize)
	// NOTE(philc): We achieve a 2% gain if we inline these integer values into the expression itself.
	column1 := table.ColumnNameToIndex["column001"]
	column1Offset := table.ColumnIndexToOffset[column1]
	var int64Sum int64

	for i := 0; i < rowCount; i++ {
		// Nil check on column1
		nilBytePtr := unsafe.Pointer(rowPtr + uintptr(0))
		nilBitIndex := uint(1)
		isNil := *(*uint)(nilBytePtr) & (1 << (7 - nilBitIndex))
		if isNil > 0 {
			continue
		}
		column1Ptr := unsafe.Pointer(rowPtr + column1Offset)
		column1Value := *(*int32)(column1Ptr)
		int64Sum += int64(column1Value)

		rowAggregate.Count++
		rowPtr += rowSize
	}
	rowAggregate.Sums[column1] = float64(int64Sum)

	results := []RowAggregate{}
	results = append(results, rowAggregate)
	return results
}

// This implements a sum over column001, and a group by over column003.
func (table *FactTable) scanInlineGroupBy(filters []FactTableFilterFunc, columnIndices []int,
	groupByColumnName string, groupByColumnTransformFn func(float64) float64) []RowAggregate {
	rowAggregatesMap := make(map[float64]*RowAggregate)
	var rowAggregate *RowAggregate
	rowCount := table.Count
	rowPtr := (*reflect.SliceHeader)(unsafe.Pointer(&table.rows)).Data
	rowSize := uintptr(table.RowSize)
	column1 := table.ColumnNameToIndex["column001"]
	column1Offset := table.ColumnIndexToOffset[column1]
	groupByColumnIndex := table.ColumnNameToIndex["column003"]
	groupByColumnOffset := table.ColumnIndexToOffset[groupByColumnIndex]

	rowAggregate = new(RowAggregate)
	rowAggregate.Sums = make([]float64, table.ColumnCount)
	for i := 0; i < rowCount; i++ {
		// Nil check on the group by column
		nilBytePtr := unsafe.Pointer(rowPtr + uintptr(0))
		nilBitIndex := uint(groupByColumnIndex)
		isNil := *(*uint)(nilBytePtr) & (1 << (7 - nilBitIndex))
		if isNil > 0 {
			rowPtr += rowSize
			continue
		}

		column1Ptr := unsafe.Pointer(rowPtr + column1Offset)
		column1Value := *(*int32)(column1Ptr)

		groupByValue := float64(*(*int32)(unsafe.Pointer(rowPtr + groupByColumnOffset)))
		if groupByColumnTransformFn != nil {
			groupByValue = groupByColumnTransformFn(groupByValue)
		}
		rowAggregatePtr, found := rowAggregatesMap[groupByValue]
		if !found {
			rowAggregate = new(RowAggregate)
			rowAggregate.Sums = make([]float64, table.ColumnCount)
			rowAggregate.GroupByValue = groupByValue
			rowAggregatesMap[groupByValue] = rowAggregate
		} else {
			rowAggregate = rowAggregatePtr
		}

		rowAggregate.Sums[column1] += float64(column1Value)
		rowAggregate.Count++
		rowPtr += rowSize
	}

	results := []RowAggregate{}
	for _, value := range rowAggregatesMap {
		results = append(results, *value)
	}
	return results
}
