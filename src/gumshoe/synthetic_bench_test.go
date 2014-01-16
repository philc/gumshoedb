// Synthetic benchmarks which test narrow, isolated conditions. These represent the ideal performance for a
// given technique. These benchmarks are helpful in providing an upper-bound for table scan speed, a
// simplified view on what affects performance, and for sanity checking the performance of the main query
// pipeline.
package gumshoe_test

import (
	"testing"
	"unsafe"
)

const (
	Rows = 100000
	Cols = 50
)

type Cell int32
type SumType int32

// The "Cell" type and ColSize are compile time constants, but they can be changed by hand to observe the
// the effect column size and thus row size has on scan speed.
var (
	ColSize = typeSizes["int32"]
	RowSize = ColSize * Cols

	RowIndexSum SumType
)

var typeSizes = map[string]uintptr{
	"int":     unsafe.Sizeof(*new(int)),
	"int8":    unsafe.Sizeof(*new(int8)),
	"int16":   unsafe.Sizeof(*new(int16)),
	"int32":   unsafe.Sizeof(*new(int32)),
	"int64":   unsafe.Sizeof(*new(int64)),
	"uint32":  unsafe.Sizeof(*new(uint32)),
	"float32": unsafe.Sizeof(*new(float32)),
	"float64": unsafe.Sizeof(*new(float64)),
}

func init() {
	for i := 0; i < Rows; i++ {
		RowIndexSum += SumType(i)
	}
}

func checkExpectedSum(b *testing.B, got SumType) {
	if got != RowIndexSum {
		b.Fatalf("Expected %v, but got %v", RowIndexSum, got)
	}
}

// Sum columns over a native array.
func BenchmarkSumArrayMatrix(b *testing.B) {
	matrix := createArrayMatrix()
	var sum SumType
	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		sum = 0
		for i := range matrix {
			sum += SumType(matrix[i][0])
		}
	}
	b.StopTimer()
	checkExpectedSum(b, sum)
}

// Sum columns over a slice of arrays.
func BenchmarkSumOneRolledLoop(b *testing.B) {
	matrix := createSliceMatrix()
	var sum SumType
	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		sum = 0
		for i := range matrix {
			sum += SumType(matrix[i][0])
		}
	}
	b.StopTimer()
	checkExpectedSum(b, sum)
}

// Sum columns over a slice of arrays.
func BenchmarkSumOneUnrolledLoop(b *testing.B) {
	matrix := createSliceMatrix()
	var sum SumType
	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		sum = 0
		for i := 0; i < len(matrix); i += 5 {
			sum += SumType(matrix[i][0])
			sum += SumType(matrix[i+1][0])
			sum += SumType(matrix[i+2][0])
			sum += SumType(matrix[i+3][0])
			sum += SumType(matrix[i+4][0])
		}
	}
	b.StopTimer()
	checkExpectedSum(b, sum)
}

// Sum columns over a slice of slices.
func BenchmarkSumSliceOfSliceMatrix(b *testing.B) {
	matrix := createSliceOfSliceMatrix()
	var sum SumType
	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		sum = 0
		for i := range matrix {
			sum += SumType(matrix[i][0])
		}
	}
	b.StopTimer()
	checkExpectedSum(b, sum)
}

// Sum columns over a byte matrix.
func BenchmarkSumByteMatrix(b *testing.B) {
	matrix := createByteMatrix()
	var sum SumType
	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		sum = 0
		p := uintptr(matrix)
		for i := 0; i < Rows; i++ {
			a := *(*Cell)(unsafe.Pointer(p))
			sum += SumType(a)
			p += RowSize
		}
	}
	b.StopTimer()
	checkExpectedSum(b, sum)
}

// Filter rows by invoking a function.
func BenchmarkSumUsingFilterFn(b *testing.B) {
	matrix := createSliceMatrix()
	filter := func(row *[Cols]Cell) bool { return row[0] >= 0 }
	var sum SumType
	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		sum = 0
		for _, row := range matrix {
			if filter(row) && filter(row) {
				sum += SumType(row[0])
			}
		}
	}
	b.StopTimer()
	checkExpectedSum(b, sum)
}

// Filter rows by evaluating an inlined filter function.
func BenchmarkSumUsingInlineFilterFn(b *testing.B) {
	matrix := createSliceMatrix()
	var sum SumType
	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		sum = 0
		for _, row := range matrix {
			if row[0] >= 0 {
				sum += SumType(row[0])
			}
		}
	}
	b.StopTimer()
	checkExpectedSum(b, sum)
}

// TODO(philc): Re-enable this.
// func SumGroupBy() int {
// 	sum := 0
// 	l := len(input)
// 	// countGroup := make(map[int]int)
// 	countGroup := make([]int, Cols)
// 	for i := 0; i < l; i += 8 {
// 		row := input[i]
// 		countGroup[i % 5] += int(row[0])
// 	}
// 	for _, v := range countGroup {
// 		sum += v
// 	}
// 	return sum
// }

func createByteMatrix() unsafe.Pointer {
	matrix := createArrayMatrix()
	return unsafe.Pointer(matrix)
}

func createArrayMatrix() *[Rows][Cols]Cell {
	var matrix [Rows][Cols]Cell
	for i := range matrix {
		matrix[i][0] = Cell(i)
	}
	return &matrix
}

func createSliceMatrix() []*[Cols]Cell {
	matrix := make([]*[Cols]Cell, Rows)
	for i := range matrix {
		matrix[i] = new([Cols]Cell)
		for j := range matrix[i] {
			matrix[i][j] = Cell(i)
		}
	}
	return matrix
}

func createSliceOfSliceMatrix() [][]Cell {
	matrix := make([][]Cell, Rows)
	for i := range matrix {
		matrix[i] = make([]Cell, Cols)
		for j := range matrix[i] {
			matrix[i][j] = Cell(i)
		}
	}
	return matrix
}
