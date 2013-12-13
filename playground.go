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

// For the purposes of my testing,
//   Field 1: time
//   Field 2: a group-by value (e.g. country)
func populateMatrixForTesting(matrix *Table) {
	for i := 0; i < len(matrix); i++ {
		matrix[i][0] = Cell(i)
		matrix[i][1] = Cell(i % 2)
		for j := 2; j < len(matrix[i]); j++ {
			matrix[i][j] = Cell(i % 5)
		}
	}
}

type RowAggregate struct {
	groupByValue Cell
	sums [COLS]float64
	count int
}

func runQuery(matrix *Table, filters []func(*Row) bool, columnIndices []int,
	columnIndexToGroupBy int) []*RowAggregate {
	useGrouping := columnIndexToGroupBy >= 0
	// This maps the values of the group-by column => RowAggregate.
	// For now, we support only one level of grouping.
	rowAggregatesMap := make(map[Cell]*RowAggregate)
	// When the query has no group-by, we tally results into a single RowAggregate.
	rowAggregate := new(RowAggregate)

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

		if useGrouping {
			groupByValue := row[columnIndexToGroupBy]
			var ok bool
			rowAggregate, ok = rowAggregatesMap[groupByValue]
			if !ok {
				rowAggregate = new(RowAggregate)
				(*rowAggregate).groupByValue = groupByValue
				rowAggregatesMap[groupByValue] = rowAggregate
			}
		}

		for _, columnIndex := range columnIndices {
			(*rowAggregate).sums[columnIndex] += float64(row[columnIndex])
		}
		(*rowAggregate).count++
	}

	results := make([]*RowAggregate, 0)
	if useGrouping {
		for _, value := range rowAggregatesMap {
			results = append(results, value)
		}
	} else {
		results = append(results, rowAggregate)
	}
	return results
}


func main() {
	matrix := createArrayMatrix()
	populateMatrixForTesting(matrix)
	filters := make([](func(*Row) bool), 1)
	filters[0] = func(row *Row) bool { return int32(row[0]) % 2 >= 0 }
	// filters[0] = func(row *Row) bool { return int32(row[0]) % 2 == 1 }
	// filters[1] = func(row *Row) bool { return int32(row[0]) % 2 == 0 }
	columnIndices := []int{1, 2}
	results := runQuery(matrix, filters, columnIndices, -1)
	fmt.Println("Results:")
	for _, result := range results {
		fmt.Println(*result)
	}
}
