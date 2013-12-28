package main

import "fmt"
import "unsafe"
import "testing"

// const ROWS = 4
const ROWS = 1000000
const COLS = 50
var COL_SIZE int = typeSizes["float32"]
var ROW_SIZE int = COL_SIZE * COLS
type Cell float32
type SumType float32

var typeSizes map[string]int = map[string]int {
	"int": asInt(unsafe.Sizeof(*new(int))),
	"int8": asInt(unsafe.Sizeof(*new(int8))),
	"int16": asInt(unsafe.Sizeof(*new(int16))),
	"int32": asInt(unsafe.Sizeof(*new(int32))),
	"uint32": asInt(unsafe.Sizeof(*new(uint32))),
	"float32": asInt(unsafe.Sizeof(*new(float32))),
	"float64": asInt(unsafe.Sizeof(*new(float64))),
}

var input = make([]*[COLS]Cell, ROWS)

func SumArrayMatrix(matrix *[ROWS][COLS]Cell) int {
	length := len(matrix)
	var sum SumType = 0.0
	for i := 0; i < length; i++ {
		sum += SumType(matrix[i][0])
	}
  return int(sum)
}

func SumOneRolledLoop(matrix []*[COLS]Cell) int {
	var sum SumType = 0.0
	length := len(matrix)
	for i := 0; i < length; i+=1 {
		sum += SumType(matrix[i][0])
	}
	return int(sum)
}

func SumSliceOfSliceMatrix(matrix [][]Cell) int {
	var sum SumType = 0.0
	length := len(matrix)
	for i := 0; i < length; i++ {
		sum += SumType(matrix[i][0])
	}
	return int(sum)
}

func SumByteMatrix(matrix uintptr) int {
	// NOTE(philc): this doesn't currently produce the correct results. It's off by a few thousand
	var sum SumType = 0
	length := ROWS
	for i := 0; i < length; i++ {
		a := *(*Cell)(unsafe.Pointer(matrix))
		sum += SumType(a)
		matrix += uintptr(ROW_SIZE)
	}
	return int(sum)
}

func SumOneUnrolledLoop(matrix []*[COLS]Cell) int {
	var sum SumType = 0
	length := len(matrix)
	for i := 0; i < length; i+=5 {
		sum += SumType(matrix[i][0])
		sum += SumType(matrix[i+1][0])
		sum += SumType(matrix[i+2][0])
		sum += SumType(matrix[i+3][0])
		sum += SumType(matrix[i+4][0])
	}
	return int(sum)
}

func SumUsingFilterFn(matrix []*[COLS]Cell, filter func(*[COLS]Cell) bool) int {
	var sum SumType = 0
	length := len(matrix)
	for i := 0; i < length; i++ {
		row := matrix[i]
		if filter(row) && filter(row) {
			sum += SumType(row[0])
		}
	}
	return int(sum)
}

func SumUsingInlineFilterFn(matrix []*[COLS]Cell) int {
	var sum SumType = 0
	length := len(matrix)
	for i := 0; i < length; i++ {
		row := matrix[i]
		if row[0] >= 0 {
			sum += SumType(row[0])
		}
	}
	return int(sum)
}

func SumGroupBy() int {
	sum := 0
	l := len(input)
	// countGroup := make(map[int]int)
	countGroup := make([]int, COLS)

	for i := 0; i < l; i += 8 {
		row := input[i]
		countGroup[i % 5] += int(row[0])
	}
	for _, v := range countGroup {
		sum += v
	}
	return sum
}

func asUint(p uintptr) uint {
	return *(*uint)(unsafe.Pointer(&p))
}

func asInt(p uintptr) int {
	return *(*int)(unsafe.Pointer(&p))
}

func setValue(matrix uintptr, row int, col int, value Cell) {
	matrix += uintptr((ROW_SIZE * row) + (COL_SIZE * col))
	a := (* Cell)(unsafe.Pointer(matrix))
	*a = value
}

func runBenchmarkFunction(name string, f func() int) {
	functionReturnValue := 0
	benchmarkHandler := func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			functionReturnValue = f()
		}
	}
	result := testing.Benchmark(benchmarkHandler)
	fmt.Printf("%-25s %-3.2f ms Result: %d\n", name, float32(result.NsPerOp()) / 1000000.0, functionReturnValue)
}

func initByteMatrix() uintptr {
	// I don't need to allocate a slice here. I can just allocate an array.
	matrix := *new([ROWS][COLS]Cell)
	for i := 0; i < len(matrix); i++ {
		for j := 0; j < len(matrix[i]); j++ {
			matrix[i][j] = Cell(i)
		}
	}
	return uintptr(unsafe.Pointer(&matrix))
}

func createArrayMatrix() *[ROWS][COLS]Cell {
	matrix := *(new([ROWS][COLS]Cell))
	for i := 0; i < len(matrix); i++ {
		matrix[i][0] = Cell(i)
	}
	return &matrix
}

func initSliceMatrix() []*[COLS]Cell {
	matrix := make([]*[COLS]Cell, ROWS)
	for i := 0; i < len(matrix); i++ {
		matrix[i] = new([COLS]Cell)
		for j := 0; j < len(matrix[i]); j++ {
			matrix[i][j] = Cell(i)
		}
	}
	return matrix
}

func initSliceOfSliceMatrix() [][]Cell {
	matrix := make([][]Cell, ROWS)
	for i := 0; i < len(matrix); i++ {
		matrix[i] = make([]Cell, COLS)
		for j := 0; j < len(matrix[i]); j++ {
			matrix[i][j] = Cell(i)
		}
	}
	return matrix
}

func main() {
	// byteMatrix := initByteMatrix()
	// fmt.Println(SumByteMatrix(byteMatrix))
	// runBenchmarkFunction("SumByteMatrix", func() int { return SumByteMatrix(byteMatrix) })

	sliceMatrix := initSliceMatrix()
	// arrayMatrix := createArrayMatrix()
	// sliceOfSliceMatrix := initSliceOfSliceMatrix()
	runBenchmarkFunction("SumOneRolledLoop", func() int { return SumOneRolledLoop(sliceMatrix) })
	// runBenchmarkFunction("SumOneUnrolledLoop", func() int { return SumOneUnrolledLoop(sliceMatrix) })
	// runBenchmarkFunction("SumArrayMatrix", func() int { return SumArrayMatrix(arrayMatrix) })
	// runBenchmarkFunction("SumSliceOfSliceMatrix", func() int { return SumSliceOfSliceMatrix(sliceOfSliceMatrix) })
	// runBenchmarkFunction("SumMany", func() { SumMany() })
	// runBenchmarkFunction("SumGroupBy", func() { SumGroupBy() })

	filterFunction := func(row *[COLS]Cell) bool { return row[0] >= 0 }
	runBenchmarkFunction("SumUsingFilterFn", func() int { return SumUsingFilterFn(sliceMatrix, filterFunction) })
	runBenchmarkFunction("SumUsingInlineFilterFn", func() int { return SumUsingInlineFilterFn(sliceMatrix) })
	// runBenchmarkFunction("SumUsingFilter2", func() { SumUsingFilter2(filterFunction) })
}
