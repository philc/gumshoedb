package main

import "fmt"

// import "unsafe"
// import "testing"

// These are currently compile time constants.
// const ROWS = 4
const ROWS = 4
const COLS = 5

type Cell float32
type Row [COLS]Cell
type Table [ROWS]Row

func createArrayMatrix() *Table {
	matrix := *(new(Table))
	for i := 0; i < len(matrix); i++ {
		matrix[i][0] = Cell(i)
	}
	return &matrix
}

func populateMatrixForTesting(matrix *Table) {
	for i := 0; i < len(matrix); i++ {
		matrix[i][0] = Cell(i % 5)
		for j := 1; j < len(matrix[i]); j++ {
			matrix[i][j] = Cell(j % 5)
		}
	}
}

func runQuery(matrix *Table, filters []func(*Row) bool, columnIndices []int) {
	// var sum float64
	// var counts [COLS]uint32
	var sums [COLS]float64

	for _, row := range matrix {
		filtered := false
		for _, filter := range filters {
			if !filter(&row) {
				filtered = true
				break
			}
		}

		if filtered {
			continue
		}

		for _, columnIndex := range columnIndices {
			// counts[columnIndex] +=
			sums[columnIndex] += float64(row[columnIndex])
		}
	}
	fmt.Println(sums)
}

func main() {
	matrix := createArrayMatrix()
	populateMatrixForTesting(matrix)
	filters := make([](func(*Row) bool), 1)
	filters[0] = func(row *Row) bool { return int32(row[0]) % 2 == 1 }
	// filters[1] = func(row *Row) bool { return int32(row[0]) % 2 == 0 }
	columnIndices := []int{1, 2}
	// results = runQuery(filters, columns, groupByColumns)
	runQuery(matrix, filters, columnIndices)
	// matrix := InitIntMatrix()
	// fmt.Println(SumOneRawMatrix(matrix))

	// matrix := allocMatrix()
	// populateMatrixForTesting(matrix)

	// filterFunction := func(row *[5]int32) bool { return row[0] >= 0 }
	// runBenchmarkFunction("SumUsingFilter", func() { SumUsingFilter(filterFunction) })
	// runBenchmarkFunction("SumUsingFilter2", func() { SumUsingFilter2(filterFunction) })
}
