package main

import "fmt"
import "unsafe"
import "testing"

// const ROWS = 4
const ROWS = 10000000
const COLS = 5
var COL_SIZE int = typeSizes["int32"]
var ROW_SIZE int = COL_SIZE * COLS

var typeSizes map[string]int = map[string]int {
	"int": asInt(unsafe.Sizeof(*new(int))),
	"int8": asInt(unsafe.Sizeof(*new(int8))),
	"int16": asInt(unsafe.Sizeof(*new(int16))),
	"int32": asInt(unsafe.Sizeof(*new(int32))),
	"uint32": asInt(unsafe.Sizeof(*new(uint32))),
}

var input = make([]*[COLS]int32, ROWS)

func SumArrayMatrix(matrix *[ROWS][COLS]int32) int {
	length := len(matrix)
	var sum int = 0.0
	for i := 0; i < length; i++ {
		sum += int(matrix[i][0])
	}
  return sum
}

func SumOneRolledLoop(matrix []*[COLS]int32) int {
	var sum int = 0.0
	length := len(matrix)
	for i := 0; i < length; i+=1 {
		sum += int(matrix[i][0])
	}
	return int(sum)
}

func SumSliceOfSliceMatrix(matrix [][]int32) int {
	var sum int = 0.0
	length := len(matrix)
	for i := 0; i < length; i++ {
		sum += int(matrix[i][0])
	}
	return int(sum)
}

func SumByteMatrix(matrix uintptr) int {
	// NOTE(philc): this doesn't currently produce the correct results. It's off by a few thousand
	sum := 0
	length := ROWS
	for i := 0; i < length; i++ {
		a := *(*int32)(unsafe.Pointer(matrix))
		sum += int(a)
		matrix += uintptr(ROW_SIZE)
	}
	return sum
}

func SumOneUnrolledLoop(matrix []*[COLS]int32) int {
	sum := 0
	length := len(matrix)
	for i := 0; i < length; i+=5 {
		sum += int(matrix[i][0])
		sum += int(matrix[i+1][0])
		sum += int(matrix[i+2][0])
		sum += int(matrix[i+3][0])
		sum += int(matrix[i+4][0])
	}
	return sum
}

func SumUsingFilter(filter func(*[5]int32) bool) int {
	sum := 0
	l := len(input)
	for i := 0; i < l; i += 8 {
		row := input[i]
		if filter(row) {
			sum += int(row[0])
		}
	}
	return sum
}

func SumUsingFilter2(filter func(*[5]int32) bool) int {
	sum := 0
	l := len(input)
	for i := 0; i < l; i += 8 {
		row := input[i]
		filter(row)
		filter(row)
		filter(row)
		filter(row)
		if filter(row) {
			sum += int(row[0])
		}
	}
	return sum
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

func setValue(matrix uintptr, row int, col int, value int32) {
	matrix += uintptr((ROW_SIZE * row) + (COL_SIZE * col))
	a := (* int32)(unsafe.Pointer(matrix))
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
	matrix := *new([ROWS][COLS]int32)
	for i := 0; i < len(matrix); i++ {
		for j := 0; j < len(matrix[i]); j++ {
			matrix[i][j] = int32(i)
		}
	}
	return uintptr(unsafe.Pointer(&matrix))
}

func createArrayMatrix() *[ROWS][COLS]int32 {
	matrix := *(new([ROWS][COLS]int32))
	for i := 0; i < len(matrix); i++ {
		matrix[i][0] = int32(i)
	}
	return &matrix
}

func initSliceMatrix() []*[COLS]int32 {
	matrix := make([]*[COLS]int32, ROWS)
	for i := 0; i < len(matrix); i++ {
		matrix[i] = new([COLS]int32)
		for j := 0; j < len(matrix[i]); j++ {
			matrix[i][j] = int32(i)
		}
	}
	return matrix
}

func initSliceOfSliceMatrix() [][]int32 {
	matrix := make([][]int32, ROWS)
	for i := 0; i < len(matrix); i++ {
		matrix[i] = make([]int32, COLS)
		for j := 0; j < len(matrix[i]); j++ {
			matrix[i][j] = int32(i)
		}
	}
	return matrix
}

func main() {
	byteMatrix := initByteMatrix()
	// fmt.Println(SumByteMatrix(byteMatrix))
	runBenchmarkFunction("SumByteMatrix", func() int { return SumByteMatrix(byteMatrix) })

	sliceMatrix := initSliceMatrix()
	arrayMatrix := createArrayMatrix()
	sliceOfSliceMatrix := initSliceOfSliceMatrix()
	runBenchmarkFunction("SumOneRolledLoop", func() int { return SumOneRolledLoop(sliceMatrix) })
	runBenchmarkFunction("SumOneUnrolledLoop", func() int { return SumOneUnrolledLoop(sliceMatrix) })
	runBenchmarkFunction("SumArrayMatrix", func() int { return SumArrayMatrix(arrayMatrix) })
	runBenchmarkFunction("SumSliceOfSliceMatrix", func() int { return SumSliceOfSliceMatrix(sliceOfSliceMatrix) })
	// runBenchmarkFunction("SumMany", func() { SumMany() })
	// runBenchmarkFunction("SumGroupBy", func() { SumGroupBy() })

	// filterFunction := func(row *[5]int32) bool { return row[0] >= 0 }
	// runBenchmarkFunction("SumUsingFilter", func() { SumUsingFilter(filterFunction) })
	// runBenchmarkFunction("SumUsingFilter2", func() { SumUsingFilter2(filterFunction) })
}
