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

type RowAggregate struct {
	groupByValue Cell
	sums         [COLS]float64
	count        int
}

var columnNameToIndex = map[string]int{
	"at":      0,
	"country": 1,
}

var columnIndexToName = []string{
	"at", "country",
}

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

func runQuery(matrix *Table, filters []func(*Row) bool, columnIndices []int,
	groupByColumnName string) []*RowAggregate {
	columnIndexToGroupBy, useGrouping := columnNameToIndex[groupByColumnName]
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

func getColumnIndiciesFromQuery(query Query) []int {
	columnIndicies := make([]int, 0)
	for _, queryAggregate := range query.Aggregates {
		columnIndicies = append(columnIndicies, columnNameToIndex[queryAggregate.Column])
	}
	return columnIndicies
}

func mapRowAggregatesToJsonResults(query Query, rowAggregates []*RowAggregate) [](map[string]Untyped) {
	jsonRows := make([](map[string]Untyped), 0)
	for _, rowAggregate := range rowAggregates {
		jsonRow := make(map[string]Untyped)
		for _, queryAggregate := range query.Aggregates {
			columnIndex := columnNameToIndex[queryAggregate.Column]
			jsonRow[queryAggregate.Name] = (*rowAggregate).sums[columnIndex]
		}
		jsonRows = append(jsonRows, jsonRow)
	}
	return jsonRows
}

func invokeQuery(matrix *Table) {
	jsonString := `{"aggregates": [
  {"type": "sum", "name": "countrySum", "column": "country"},
  {"type": "sum", "name": "atSum", "column": "at"}
],
 "groupings": [{"column": "country", "name":"country1"}]
}`
	query := ParseJsonQuery(jsonString)
	columnIndicies := getColumnIndiciesFromQuery(query)
	var groupByColumn string
	if len(query.Groupings) > 0 {
		groupByColumn = query.Groupings[0].Column
	}
	results := runQuery(matrix, nil, columnIndicies, groupByColumn)
	fmt.Println("RowAggregate Results:")
	for _, result := range results {
		fmt.Println(*result)
	}
	fmt.Println("Json Results:")
	jsonResultRows := mapRowAggregatesToJsonResults(query, results)
	fmt.Println(jsonResultRows)
}

func main() {
	matrix := createArrayMatrix()
	populateMatrixForTesting(matrix)
	invokeQuery(matrix)
	// filters := make([](func(*Row) bool), 1)
	// filters[0] = func(row *Row) bool { return int32(row[0]) % 2 >= 0 }
	// filters[0] = func(row *Row) bool { return int32(row[0]) % 2 == 1 }
	// filters[1] = func(row *Row) bool { return int32(row[0]) % 2 == 0 }
	// columnIndices := []int{1, 2}
	// results := runQuery(matrix, filters, columnIndices, -1)
}
