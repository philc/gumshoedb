// Synthetic benchmarks which test narrow, isolated conditions. These represent the ideal performance for a
// given technique. These benchmarks are helpful in providing an upper-bound for table scan speed, a
// simplified view on what affects performance, and for sanity checking the performance of the main query
// pipeline.
package synthetic

import (
	"math"
	"reflect"
	"testing"
	"unsafe"
)

const (
	Rows = 160000 // Multiple of 8, for convenience (see synthetic_parallel_bench_test.go)
	Cols = 64
)

type Cell int32
type SumType int32

// The "Cell" type and ColSize are compile time constants, but they can be changed by hand to observe the
// the effect column size and thus row size has on scan speed.
var (
	ColSize     = typeSizes["int32"]
	RowSize     = ColSize * Cols
	RowIndexSum SumType

	// Specs for tables which have mixed column types in the same row.
	MixedColumnSizeEven = typeSizes["int32"]
	MixedColumnSizeOdd  = typeSizes["int8"]
	MixedRowSize        = uintptr(int(math.Ceil(Cols/2))*int(MixedColumnSizeEven) +
		(Cols/2)*int(MixedColumnSizeOdd))
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
	if Rows%8 != 0 {
		panic("Rows must be divisible by 8.")
	}
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
	setBytes(b, RowSize)
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
	setBytes(b, RowSize)
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
	setBytes(b, RowSize)
	checkExpectedSum(b, sum)
}

// Sums columns over a virtual matrix stored in a contiguous slice.
func BenchmarkSumContiguousSliceMatrix(b *testing.B) {
	matrix := createContiguousSliceMatrix()
	var sum SumType
	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		sum = 0
		for i := 0; i < Rows; i++ {
			sum += SumType(matrix[i*Cols+0])
		}
	}
	b.StopTimer()
	setBytes(b, RowSize)
	checkExpectedSum(b, sum)
}

// Sums columns over a virtual byte matrix stored in a contiguous slice.
func BenchmarkSumContiguousSliceByteMatrix(b *testing.B) {
	matrix := createContiguousSliceByteMatrix(Cols)
	var sum SumType
	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		sum = 0
		for i := 0; i < Rows; i++ {
			v := &matrix[i*int(RowSize)+0]
			sum += *(*SumType)(unsafe.Pointer(v))
		}
	}
	b.StopTimer()
	setBytes(b, RowSize)
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
	setBytes(b, RowSize)
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
	setBytes(b, RowSize)
	checkExpectedSum(b, sum)
}

// Sum columns where the column types are mixed. Since the row width is smaller because some columns are
// smaller, this should improve performance when memory bandwidth is the bottleneck.
func BenchmarkSumByteMatrixMixedColumns(b *testing.B) {
	matrix := createSliceByteMatrixWithMixedColumns()
	var sum SumType
	var columnTypes [Cols]int
	columnOffsets := [Cols]uintptr{uintptr(0), uintptr(4)} // Prepared before-hand based on the column type.
	columnIndex := 0
	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		sum = 0
		p := uintptr(matrix)
		for i := 0; i < Rows; i++ {
			columnPtr := unsafe.Pointer(p + columnOffsets[columnIndex])
			switch columnTypes[columnIndex] {
			case 0:
				sum += SumType(*(*int32)(columnPtr))
			case 1:
				sum += SumType(*(*int8)(columnPtr))
			}
			p += MixedRowSize
		}
	}

	b.StopTimer()
	setBytes(b, MixedRowSize)
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
	setBytes(b, RowSize)
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
	setBytes(b, RowSize)
	checkExpectedSum(b, sum)
}

func BenchmarkSumGroupByUsingHashMap(b *testing.B) {
	matrix := createSliceMatrix()
	groupByColumn := 1
	var groups map[Cell]SumType
	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		groups = map[Cell]SumType{}
		for _, row := range matrix {
			groupByValue := row[groupByColumn]
			// This if statement isn't necessary in this benchmark, but it represents what we'll need to do in a
			// real implementation where the value from the map can be null.
			if _, exists := groups[groupByValue]; exists {
				groups[groupByValue] += SumType(row[0])
			} else {
				groups[groupByValue] = SumType(row[0])
			}
		}
	}
	b.StopTimer()
	setBytes(b, RowSize)
	sum := SumType(0)
	for _, v := range groups {
		sum += v
	}
	checkExpectedSum(b, sum)
}

// This implementation requires knowing the cardinality of the group-by value ahead of time.
func BenchmarkSumGroupByUsingVector(b *testing.B) {
	matrix := createSliceMatrix()
	groupByColumn := 1
	groupByCardinality := 10
	var groups []SumType
	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		groups = make([]SumType, groupByCardinality)
		for _, row := range matrix {
			groupByValue := row[groupByColumn]
			groups[groupByValue] += SumType(row[0])
		}
	}
	b.StopTimer()
	setBytes(b, RowSize)
	sum := SumType(0)
	for _, v := range groups {
		sum += v
	}
	checkExpectedSum(b, sum)
}

func createByteMatrix() unsafe.Pointer {
	matrix := createArrayMatrix()
	return unsafe.Pointer(matrix)
}

// Creates a byte matrix with mixed column widths. Even columns are int32 and odd columns are int8. This
// simulates the gains from leveraging smaller column types to achieve narrower rows.
func createSliceByteMatrixWithMixedColumns() unsafe.Pointer {
	rowSize := int(math.Ceil(Cols/2))*int(MixedColumnSizeEven) + int(Cols/2)*int(MixedColumnSizeOdd)
	matrix := make([]byte, rowSize*Rows)
	for i := 0; i < Rows; i++ {
		columnOffset := 0
		for j := 0; j < Cols; j++ {
			v := &matrix[i*int(rowSize)+columnOffset]
			if j%2 == 0 {
				*(*int32)(unsafe.Pointer(v)) = int32(i)
				columnOffset += int(MixedColumnSizeEven)
			} else {
				*(*int8)(unsafe.Pointer(v)) = int8(j)
				columnOffset += int(MixedColumnSizeOdd)
			}
		}
	}
	headerPtr := (*reflect.SliceHeader)(unsafe.Pointer(&matrix))
	return unsafe.Pointer(headerPtr.Data)
}

func createArrayMatrix() *[Rows][Cols]Cell {
	var matrix [Rows][Cols]Cell
	for i := range matrix {
		matrix[i][0] = Cell(i)
	}
	return &matrix
}

func createContiguousSliceMatrix() []Cell {
	matrix := make([]Cell, Rows*Cols)
	for i := 0; i < Rows; i++ {
		for j := 0; j < Cols; j++ {
			matrix[i*Cols+j] = Cell(i)
		}
	}
	return matrix
}

func createContiguousSliceByteMatrix(cols int) []byte {
	rowSize := int(ColSize) * cols
	matrix := make([]byte, rowSize*Rows)
	for i := 0; i < Rows; i++ {
		for j := 0; j < cols; j++ {
			v := &matrix[i*int(rowSize)+j*int(ColSize)]
			*(*Cell)(unsafe.Pointer(v)) = Cell(i)
		}
	}
	return matrix
}

func createSliceMatrix() []*[Cols]Cell {
	matrix := make([]*[Cols]Cell, Rows)
	for i := range matrix {
		matrix[i] = new([Cols]Cell)
		for j := range matrix[i] {
			matrix[i][j] = Cell(i)
		}
		// Use column 1 as the "group by" column in our "group by" benchmarks.
		matrix[i][1] = Cell(i % 10)
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

func setBytes(b *testing.B, rowSize uintptr) {
	b.SetBytes(int64(Rows * int(rowSize)))
}
