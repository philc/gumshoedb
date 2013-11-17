package main

import "fmt"
import "unsafe"
import "testing"

// These are currently compile time constants.
// const ROWS = 4
const ROWS = 10000000
const COLS = 5

var COL_SIZE int = typeSizes["int"]
var ROW_SIZE int = COL_SIZE * COLS

var typeSizes map[string]int = map[string]int {
	"int": asInt(unsafe.Sizeof(*new(int))),
	"int8": asInt(unsafe.Sizeof(*new(int8))),
	"int16": asInt(unsafe.Sizeof(*new(int16))),
	"uint32": asInt(unsafe.Sizeof(*new(uint32))),
}

var input = make([]*[COLS]int32, ROWS)
// var input = make([]*[5]int, 10000000)

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

		// sum += int(row[0])
	}
	for _, v := range countGroup {
		sum += v
	}
	return sum
}

func allocMatrix() []*[COLS]float32 {
	// for i := 0; i < rows; i++
	matrix := make([]*[COLS]float32, ROWS)
	for i, _ := range matrix {
		matrix[i] = new([COLS]float32)
	}
	return matrix
	// return unsafe.Pointer(&matrix)
}

func populateMatrixForTesting([]*[COLS]float32) {
	for i := 0; i < len(input); i++ {
		input[i][0] = int32(i % 5)
		for j := 1; j < len(input[i]); j++ {
			input[i][j] = int32(j % 5)
		}
	}
}

func asUint(p uintptr) uint {
	return *(*uint)(unsafe.Pointer(&p))
}

func asInt(p uintptr) int {
	return *(*int)(unsafe.Pointer(&p))
}

func setValue(matrix uintptr, row int, col int, value int) {
	matrix += uintptr((ROW_SIZE * row) + (COL_SIZE * col))
	a := (* int)(unsafe.Pointer(matrix))
	*a = value
}

func InitIntMatrix() uintptr {
	slice := make([]int, ROWS * COLS)
	var matrix uintptr = (uintptr)(unsafe.Pointer(&slice[0]))
	for i := 0; i < ROWS; i++ {
		for j := 0; j < COLS; j++ {
			setValue(matrix, i, j, int(i))
		}
	}
	return matrix
}

type Untyped interface {}

func runBenchmarkFunction(name string, f func()) {
	benchmarkHandler := func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			f()
		}
	}
	result := testing.Benchmark(benchmarkHandler)
	fmt.Printf("%s: %2.2f ms\n", name, float32(result.NsPerOp()) / 1000000.0)
}

func main() {
	fmt.Println("Starting")
	// matrix := InitIntMatrix()
	// fmt.Println(SumOneRawMatrix(matrix))

	// matrix := allocMatrix()
	// populateMatrixForTesting(matrix)
	runBenchmarkFunction("SumGroupBy", func() { SumGroupBy() })

	// filterFunction := func(row *[5]int32) bool { return row[0] >= 0 }
	// runBenchmarkFunction("SumUsingFilter", func() { SumUsingFilter(filterFunction) })
	// runBenchmarkFunction("SumUsingFilter2", func() { SumUsingFilter2(filterFunction) })
}
