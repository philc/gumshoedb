package main

import "fmt"

// The size of the fact table is currently a compile time constant, so we can use native arrays instead of
// ranges.

const ROWS = 5
const COLS = 4

type Cell float32
type FactRow [COLS]Cell

type FactTable struct {
	Rows [ROWS]FactRow
	NextInsertPosition int
	// The number of used rows in the table. This is <= ROWS.
	Count int
	Capacity int
	// A mapping of column index => column's dimension table.
	DimensionTables [COLS]*DimensionTable
}

type DimensionTable struct {
  Rows []string
	ValueToId map[string]int32
}

func NewDimensionTable() *DimensionTable {
  table := new(DimensionTable)
	table.ValueToId = make(map[string]int32)
  return table
}

type RowAggregate struct {
	groupByValue Cell
	sums         [COLS]float64
	count        int
}

var columnNameToIndex = map[string]int{
	"at":      0,
	"country": 1,
	"impressions": 2,
	"clicks": 3,
}

var columnIndexToName = []string{
	"at", "country", "impressions", "clicks",
}


func NewFactTable() *FactTable {
	table := new(FactTable)
	for i, _ := range table.DimensionTables {
		table.DimensionTables[i] = NewDimensionTable()
	}
	table.Capacity = len(table.Rows)
	return table
}

func populateTableWithTestingData(table *FactTable) {
	rows := make([]map[string]Untyped, 0, ROWS)
	countries := map[int]string {
		0: "Japan",
		1: "USA",
	}
	for i := 0; i < len(table.Rows); i++ {
		row := make(map[string]Untyped)
		row["at"] = i
		row["country"] = countries[i % 2]
		rows = append(rows, row)
	}

	for _, row := range rows {
		normalizedRow := normalizeRow(table, convertRowMapToRowArray(row))
		insertNormalizedRow(table, normalizedRow)
	}
}

func convertRowMapToRowArray(rowMap map[string]Untyped) []Untyped {
	result := make([]Untyped, COLS)
	for columnName, value := range rowMap {
		columnIndex, ok := columnNameToIndex[columnName]
		if !ok {
			// TODO(philc): Return an error here if there's an unrecognizable column.
		}
    result[columnIndex] = value
	}
	return result
}

func addRowToDimensionTable(dimensionTable *DimensionTable, rowValue string) int32 {
	nextId := int32(len(dimensionTable.Rows))
	dimensionTable.Rows = append(dimensionTable.Rows, rowValue)
	dimensionTable.ValueToId[rowValue] = nextId
	return nextId
}

func isString(value interface{}) bool {
	// TODO(philc): There must be a built-in for this.
	result := false
	switch value.(type) {
	case string:
		result = true
	}
	return result
}

func convertUntypedToCell(v Untyped) Cell {
	var result Cell
	switch v.(type) {
	case float32:
		result = Cell(v.(float32))
	case float64:
		result = Cell(v.(float64))
	case int:
		result = Cell(v.(int))
	case int8:
		result = Cell(v.(int8))
	case int32:
		result = Cell(v.(int32))
	case int64:
		result = Cell(v.(int64))
	}
	return result
}

// Takes a row of mixed types, like strings and ints. For every string column, replaces its value with the
// matching ID from the dimension table, inserting a row into the dimension table if one doesn't already
// exist.
func normalizeRow(table *FactTable, jsonRow []Untyped) FactRow {
	var row FactRow
	for columnIndex, value := range jsonRow {
		usesDimensionTable := isString(value)
		if usesDimensionTable {
			stringValue := value.(string)
			dimensionTable := table.DimensionTables[columnIndex]
			dimensionRowId, ok := dimensionTable.ValueToId[stringValue]
			if !ok {
				dimensionRowId = addRowToDimensionTable(dimensionTable, stringValue)
			}
			row[columnIndex] = Cell(dimensionRowId)
		} else {
			row[columnIndex] = convertUntypedToCell(value)
		}
	}
	return row
}

func insertNormalizedRow(table *FactTable, row FactRow) {
	table.Rows[table.NextInsertPosition] = row
	table.NextInsertPosition = (table.NextInsertPosition + 1) % table.Capacity
	if table.Count < table.Capacity {
		table.Count++
	}
}

// TODO(philc): Can I change this return value to a slice of RowAggregates?
func runQuery(table *FactTable, filters []func(*FactRow) bool, columnIndices []int,
	groupByColumnName string) []RowAggregate {
	columnIndexToGroupBy, useGrouping := columnNameToIndex[groupByColumnName]
	// This maps the values of the group-by column => RowAggregate.
	// For now, we support only one level of grouping.
	rowAggregatesMap := make(map[Cell]*RowAggregate)
	// When the query has no group-by, we tally results into a single RowAggregate.
	rowAggregate := new(RowAggregate)

	outerLoop: for i, row := range table.Rows {
		if i >= table.Count {
			break
		}
		for _, filter := range filters {
			if !filter(&row) {
				continue outerLoop
			}
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

	results := make([]RowAggregate, 0)
	if useGrouping {
		for _, value := range rowAggregatesMap {
			results = append(results, *value)
		}
	} else {
		results = append(results, *rowAggregate)
	}
	return results
}

// TODO(philc): This function probably be inlined.
func getColumnIndiciesFromQuery(query Query) []int {
	columnIndicies := make([]int, 0)
	for _, queryAggregate := range query.Aggregates {
		columnIndicies = append(columnIndicies, columnNameToIndex[queryAggregate.Column])
	}
	return columnIndicies
}

func mapRowAggregatesToJsonResults(query Query, rowAggregates []RowAggregate) [](map[string]Untyped) {
	jsonRows := make([](map[string]Untyped), 0)
	for _, rowAggregate := range rowAggregates {
		jsonRow := make(map[string]Untyped)
		for _, queryAggregate := range query.Aggregates {
			columnIndex := columnNameToIndex[queryAggregate.Column]
			jsonRow[queryAggregate.Name] = rowAggregate.sums[columnIndex]
		}
		jsonRow["rowCount"] = rowAggregate.count
		jsonRows = append(jsonRows, jsonRow)
	}
	return jsonRows
}

func invokeQuery(table *FactTable) {
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
	results := runQuery(table, nil, columnIndicies, groupByColumn)
	fmt.Println("RowAggregate Results:")
	for _, result := range results {
		fmt.Println(result)
	}
	fmt.Println("Json Results:")
	jsonResultRows := mapRowAggregatesToJsonResults(query, results)
	fmt.Println(jsonResultRows)
}

func main() {
	table := NewFactTable()
	populateTableWithTestingData(table)
	invokeQuery(table)
	// filters := make([](func(*Row) bool), 1)
	// filters[0] = func(row *Row) bool { return int32(row[0]) % 2 >= 0 }
	// filters[0] = func(row *Row) bool { return int32(row[0]) % 2 == 1 }
	// filters[1] = func(row *Row) bool { return int32(row[0]) % 2 == 0 }
	// columnIndices := []int{1, 2}
	// results := runQuery(matrix, filters, columnIndices, -1)
}
