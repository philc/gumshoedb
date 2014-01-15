// Synthetic benchmarks which test narrow, isolated conditions. These represent the ideal performance for a
// given technique. These benchmarks are helpful in providing an upper-bound for table scan speed, a
// simplified view on what affects performance, and for sanity checking the performance of the main query
// pipeline.
package main

import "unsafe"

const ROWS = 100000
const COLS = 50

// The "Cell" type and COL_SIZE are compile time constants, but they can be changed by hand to observe the
// the effect column size and thus row size has on scan speed.
var COL_SIZE = typeSizes["float32"]
var ROW_SIZE = COL_SIZE * COLS

type Cell float32
type SumType float32

var typeSizes = map[string]uintptr{
	"int":     unsafe.Sizeof(*new(int)),
	"int8":    unsafe.Sizeof(*new(int8)),
	"int16":   unsafe.Sizeof(*new(int16)),
	"int32":   unsafe.Sizeof(*new(int32)),
	"uint32":  unsafe.Sizeof(*new(uint32)),
	"float32": unsafe.Sizeof(*new(float32)),
	"float64": unsafe.Sizeof(*new(float64)),
}

// Sum columns over a native array.
func SumArrayMatrix(matrix *[ROWS][COLS]Cell) int {
	var sum SumType
	for i := range matrix {
		sum += SumType(matrix[i][0])
	}
	return int(sum)
}

// Sum columns over a slice of arrays.
func SumOneRolledLoop(matrix []*[COLS]Cell) int {
	var sum SumType
	for i := range matrix {
		sum += SumType(matrix[i][0])
	}
	return int(sum)
}

// Sum columns over a slice of arrays.
func SumOneUnrolledLoop(matrix []*[COLS]Cell) int {
	var sum SumType
	for i := 0; i < len(matrix); i += 5 {
		sum += SumType(matrix[i][0])
		sum += SumType(matrix[i+1][0])
		sum += SumType(matrix[i+2][0])
		sum += SumType(matrix[i+3][0])
		sum += SumType(matrix[i+4][0])
	}
	return int(sum)
}

// Sum columns over a slice of slices.
func SumSliceOfSliceMatrix(matrix [][]Cell) int {
	var sum SumType
	for i := range matrix {
		sum += SumType(matrix[i][0])
	}
	return int(sum)
}

// Sum columns over a byte matrix.
func SumByteMatrix(matrix unsafe.Pointer) int {
	var sum SumType
	p := uintptr(matrix)
	for i := 0; i < ROWS; i++ {
		a := *(*Cell)(unsafe.Pointer(p))
		sum += SumType(a)
		p += ROW_SIZE
	}
	return int(sum)
}

// Filter rows by invoking a function.
func SumUsingFilterFn(matrix []*[COLS]Cell, filter func(*[COLS]Cell) bool) int {
	var sum SumType
	for _, row := range matrix {
		if filter(row) && filter(row) {
			sum += SumType(row[0])
		}
	}
	return int(sum)
}

// Filter rows by evaluating an inlined filter function.
func SumUsingInlineFilterFn(matrix []*[COLS]Cell) int {
	var sum SumType
	for _, row := range matrix {
		if row[0] >= 0 {
			sum += SumType(row[0])
		}
	}
	return int(sum)
}

// TODO(philc): Re-enable this.
// func SumGroupBy() int {
// 	sum := 0
// 	l := len(input)
// 	// countGroup := make(map[int]int)
// 	countGroup := make([]int, COLS)
// 	for i := 0; i < l; i += 8 {
// 		row := input[i]
// 		countGroup[i % 5] += int(row[0])
// 	}
// 	for _, v := range countGroup {
// 		sum += v
// 	}
// 	return sum
// }

func initByteMatrix() unsafe.Pointer {
	matrix := createArrayMatrix()
	return unsafe.Pointer(matrix)
}

func createArrayMatrix() *[ROWS][COLS]Cell {
	var matrix [ROWS][COLS]Cell
	for i := range matrix {
		matrix[i][0] = Cell(i)
	}
	return &matrix
}

func initSliceMatrix() []*[COLS]Cell {
	matrix := make([]*[COLS]Cell, ROWS)
	for i := range matrix {
		matrix[i] = new([COLS]Cell)
		for j := range matrix[i] {
			matrix[i][j] = Cell(i)
		}
	}
	return matrix
}

func initSliceOfSliceMatrix() [][]Cell {
	matrix := make([][]Cell, ROWS)
	for i := range matrix {
		matrix[i] = make([]Cell, COLS)
		for j := range matrix[i] {
			matrix[i][j] = Cell(i)
		}
	}
	return matrix
}

func runSyntheticBenchmarks(flags BenchmarkFlags) {
	arrayMatrix := createArrayMatrix()
	runBenchmarkFunctionWithReturnValue("SumArrayMatrix", func() int { return SumArrayMatrix(arrayMatrix) })

	if !*flags.minimalSet {
		sliceMatrix := initSliceMatrix()
		byteMatrix := initByteMatrix()
		sliceOfSliceMatrix := initSliceOfSliceMatrix()
		runBenchmarkFunctionWithReturnValue("SumOneRolledLoop", func() int { return SumOneRolledLoop(sliceMatrix) })
		runBenchmarkFunctionWithReturnValue("SumOneUnrolledLoop", func() int { return SumOneUnrolledLoop(sliceMatrix) })
		runBenchmarkFunctionWithReturnValue("SumByteMatrix", func() int { return SumByteMatrix(byteMatrix) })
		runBenchmarkFunctionWithReturnValue("SumSliceOfSliceMatrix",
			func() int { return SumSliceOfSliceMatrix(sliceOfSliceMatrix) })
		// runBenchmarkFunctionWithReturnValue("SumGroupBy", func() { SumGroupBy() })

		filterFunction := func(row *[COLS]Cell) bool { return row[0] >= 0 }
		runBenchmarkFunctionWithReturnValue("SumUsingFilterFn",
			func() int { return SumUsingFilterFn(sliceMatrix, filterFunction) })

		runBenchmarkFunctionWithReturnValue("SumUsingInlineFilterFn",
			func() int { return SumUsingInlineFilterFn(sliceMatrix) })
	}
}
