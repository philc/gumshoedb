package core

import "fmt"

// The size of the fact table is currently a compile time constant, so we can use native arrays instead of
// ranges.
const ROWS = 1000000
const COLS = 42

type Cell float32
type FactRow [COLS]Cell

type FactTable struct {
	Rows               [ROWS]FactRow
	NextInsertPosition int
	Count              int // The number of used rows currently in the table. This is <= ROWS.
	ColumnCount        int // The number of columns in use in the table. This is <= COLS.
	Capacity           int
	DimensionTables    [COLS]*DimensionTable // Column index => column's dimension table.
	ColumnNameToIndex  map[string]int
	ColumnIndexToName  []string
}

type DimensionTable struct {
	Name      string
	Rows      []string
	ValueToId map[string]int32
}

func NewDimensionTable(name string) *DimensionTable {
	table := new(DimensionTable)
	table.Name = name
	table.ValueToId = make(map[string]int32)
	return table
}

// TODO(philc): Capitalize these field names.
type RowAggregate struct {
	GroupByValue Cell
	Sums         [COLS]float64
	Count        int
}

type FactTableFilterFunc func(*FactRow) bool

func NewFactTable(columnNames []string) *FactTable {
	if len(columnNames) > COLS {
		panic(fmt.Sprintf("You provided %d columns, but this table is configured to have only %d columns.",
			len(columnNames), COLS))
	}

	table := new(FactTable)
	for i, name := range columnNames {
		table.DimensionTables[i] = NewDimensionTable(name)
	}
	table.Capacity = len(table.Rows)
	table.ColumnCount = len(columnNames)
	table.ColumnIndexToName = make([]string, len(columnNames))
	table.ColumnNameToIndex = make(map[string]int, len(columnNames))
	for i, name := range columnNames {
		table.ColumnIndexToName[i] = name
		table.ColumnNameToIndex[name] = i
	}
	return table
}

func populateTableWithTestingData(table *FactTable) {
	rows := make([]map[string]Untyped, 0, ROWS)
	countries := map[int]string{
		0: "Japan",
		1: "USA",
	}
	for i := 0; i < len(table.Rows); i++ {
		row := make(map[string]Untyped)
		row["at"] = i
		row["country"] = countries[i%2]
		rows = append(rows, row)
	}

	InsertRowMaps(table, rows)
}

// Given a cell from a row vector, returns either the cell if this column isn't already normalized,
// or the denormalized value. E.g. denormalizeColumnValue(213, 1) => "Japan"
func denormalizeColumnValue(table *FactTable, columnValue Cell, columnIndex int) Untyped {
	// TODO(philc): I'm using the implicit condition that if a dimension table is empty, this column isn't a
	// normalized dimension. This should be represented explicitly by a user-defined schema.
	dimensionTable := table.DimensionTables[columnIndex]
	if len(dimensionTable.Rows) > 0 {
		return dimensionTable.Rows[int(columnValue)]
	} else {
		return columnValue
	}
}

// Takes a normalized FactRow vector and returns a map consistent of column names and values pulled from
// the dimension tables.
func DenormalizeRow(table *FactTable, row *FactRow) map[string]Untyped {
	result := make(map[string]Untyped)
	for i := 0; i < table.ColumnCount; i++ {
		result[table.ColumnIndexToName[i]] = denormalizeColumnValue(table, row[i], i)
	}
	return result
}

// Takes a map of column names => values, and returns a vector with the map's values in the correct column
// position according to the table's schema.
// Returns an error if there are unrecognized columns, or if a column is missing.
func convertRowMapToRowArray(table *FactTable, rowMap map[string]Untyped) ([]Untyped, error) {
	result := make([]Untyped, COLS)
	for columnName, value := range rowMap {
		columnIndex, found := table.ColumnNameToIndex[columnName]
		if !found {
			return nil, fmt.Errorf("Unrecognized column name: %s", columnName)
		}
		result[columnIndex] = value
	}
	return result, nil
}

// Takes a row of mixed types, like strings and ints, and converts it to a FactRow (a vector of ints). For
// every string column, replaces its value with the matching ID from the dimension table, inserting a row into
// the dimension table if one doesn't already exist.
func normalizeRow(table *FactTable, rowMap map[string]Untyped) (*FactRow, error) {
	rowAsArray, error := convertRowMapToRowArray(table, rowMap)
	if error != nil {
		return nil, error
	}
	var row FactRow
	for columnIndex, value := range rowAsArray {
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
			row[columnIndex] = Cell(convertUntypedToFloat64(value))
		}
	}
	return &row, nil
}

func insertNormalizedRow(table *FactTable, row *FactRow) {
	table.Rows[table.NextInsertPosition] = *row
	table.NextInsertPosition = (table.NextInsertPosition + 1) % table.Capacity
	if table.Count < table.Capacity {
		table.Count++
	}
}

// Inserts the given rows into the table.
func InsertRowMaps(table *FactTable, rows []map[string]Untyped) error {
	for _, rowMap := range rows {
		normalizedRow, error := normalizeRow(table, rowMap)
		if error != nil {
			return error
		}
		insertNormalizedRow(table, normalizedRow)
	}
	return nil
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

func convertUntypedToFloat64(v Untyped) float64 {
	var result float64
	switch v.(type) {
	case float32:
		result = float64(v.(float32))
	case float64:
		result = v.(float64)
	case int:
		result = float64(v.(int))
	case int8:
		result = float64(v.(int8))
	case int32:
		result = float64(v.(int32))
	case int64:
		result = float64(v.(int64))
	}
	return result
}

// TODO(philc): Can I change this return value to a slice of RowAggregates?
func scanTable(table *FactTable, filters []FactTableFilterFunc, columnIndices []int,
	groupByColumnName string) []RowAggregate {
	columnIndexToGroupBy, useGrouping := table.ColumnNameToIndex[groupByColumnName]
	// This maps the values of the group-by column => RowAggregate.
	// For now, we support only one level of grouping.
	rowAggregatesMap := make(map[Cell]*RowAggregate)
	// When the query has no group-by, we tally results into a single RowAggregate.
	rowAggregate := new(RowAggregate)
	rows := &table.Rows
	rowCount := table.Count
	columnCountInQuery := len(columnIndices)
	filterCount := len(filters)
outerLoop:
	for i := 0; i < rowCount; i++ {
		row := &rows[i]
		for filterIndex := 0; filterIndex < filterCount; filterIndex++ {
			if !filters[filterIndex](row) {
				continue outerLoop
			}
		}

		if useGrouping {
			groupByValue := row[columnIndexToGroupBy]
			var ok bool
			rowAggregate, ok = rowAggregatesMap[groupByValue]
			if !ok {
				rowAggregate = new(RowAggregate)
				(*rowAggregate).GroupByValue = groupByValue
				rowAggregatesMap[groupByValue] = rowAggregate
			}
		}

		for j := 0; j < columnCountInQuery; j++ {
			columnIndex := columnIndices[j]
			(*rowAggregate).Sums[columnIndex] += float64(row[columnIndex])
		}
		(*rowAggregate).Count++
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
func getColumnIndiciesFromQuery(query *Query, table *FactTable) []int {
	columnIndicies := make([]int, 0)
	for _, queryAggregate := range query.Aggregates {
		columnIndicies = append(columnIndicies, table.ColumnNameToIndex[queryAggregate.Column])
	}
	return columnIndicies
}

func mapRowAggregatesToJsonResultsFormat(query *Query, table *FactTable,
	rowAggregates []RowAggregate) [](map[string]Untyped) {
	jsonRows := make([](map[string]Untyped), 0)
	for _, rowAggregate := range rowAggregates {
		jsonRow := make(map[string]Untyped)
		for _, queryAggregate := range query.Aggregates {
			columnIndex := table.ColumnNameToIndex[queryAggregate.Column]
			// TODO(philc): Change this to an enum
			sums := rowAggregate.Sums[columnIndex]
			if queryAggregate.Type == "sum" {
				jsonRow[queryAggregate.Name] = sums
			} else if queryAggregate.Type == "average" {
				jsonRow[queryAggregate.Name] = sums / float64(rowAggregate.Count)
			}
		}
		// TODO(philc): This code does not handle multiple groupings.
		for _, grouping := range query.Groupings {
			columnIndex := table.ColumnNameToIndex[grouping.Column]
			jsonRow[grouping.Name] = denormalizeColumnValue(table, rowAggregate.GroupByValue, columnIndex)
		}
		jsonRow["rowCount"] = rowAggregate.Count
		jsonRows = append(jsonRows, jsonRow)
	}
	return jsonRows
}

// Given a list of values, looks up the corresponding row IDs for those values. If those values don't
// exist in the dimension table, they're omitted.
func getDimensionRowIdsForValues(dimensionTable *DimensionTable, values []string) []Cell {
	rowIds := make([]Cell, 0)
	for _, value := range values {
		if id, ok := dimensionTable.ValueToId[value]; ok {
			rowIds = append(rowIds, Cell(id))
		}
	}
	return rowIds
}

func convertQueryFilterToFilterFunc(queryFilter QueryFilter, table *FactTable) FactTableFilterFunc {
	columnIndex := table.ColumnNameToIndex[queryFilter.Column]
	// TODO(philc): We're assuming this is a float for now.
	var f FactTableFilterFunc
	var valueAsCell Cell
	if queryFilter.Type != "in" {
		valueAsCell = Cell(convertUntypedToFloat64(queryFilter.Value))
	}

	switch queryFilter.Type {
	// TODO(philc): Add <= and >= once this turns out to be useful.
	case "greaterThan", ">":
		f = func(row *FactRow) bool {
			return row[columnIndex] > valueAsCell
		}
	case "lessThan", "<":
		f = func(row *FactRow) bool {
			return row[columnIndex] < valueAsCell
		}
	case "equal", "=":
		f = func(row *FactRow) bool {
			return row[columnIndex] == valueAsCell
		}
	case "in":
		// Convert this slice of untyped objects to []string. We encounter a panic if we try to cast straight
		// to []string; I'm not sure why.
		queryValues := queryFilter.Value.([]interface{})
		queryValuesAstrings := make([]string, 0, len(queryValues))
		for _, element := range queryValues {
			queryValuesAstrings = append(queryValuesAstrings, element.(string))
		}

		dimensionTable := table.DimensionTables[columnIndex]
		matchingColumnIds := getDimensionRowIdsForValues(dimensionTable, queryValuesAstrings)
		count := len(matchingColumnIds)
		// TODO(philc): At some threshold, indexing into a query set of a given size will be more efficiently done
		// as a hash table lookup. We should figure out what that threshold is and use a hash table in that case.
		f = func(row *FactRow) bool {
			columnValue := row[columnIndex]
			for i := 0; i < count; i++ {
				if columnValue == matchingColumnIds[i] {
					return true
				}
			}
			return false
		}
	}
	return f
}

func InvokeQuery(table *FactTable, query *Query) map[string]Untyped {
	columnIndicies := getColumnIndiciesFromQuery(query, table)
	var groupByColumn string
	if len(query.Groupings) > 0 {
		groupByColumn = query.Groupings[0].Column
	}

	filterFuncs := make([]FactTableFilterFunc, 0, len(query.Filters))
	for _, queryFilter := range query.Filters {
		filterFuncs = append(filterFuncs, convertQueryFilterToFilterFunc(queryFilter, table))
	}

	results := scanTable(table, filterFuncs, columnIndicies, groupByColumn)
	jsonResultRows := mapRowAggregatesToJsonResultsFormat(query, table, results)
	return map[string]Untyped{
		"results": jsonResultRows,
	}
}
