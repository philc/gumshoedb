package core

import (
	json "encoding/json"
	"fmt"
)

type Untyped interface{}

type QueryAggregate struct {
	Type   string
	Column string
	Name   string
}

type QueryGrouping struct {
	// This provides a means of specifying an optional date truncation function, assuming the column is a
	// timestamp. It makes it posisble to group by time intervals (minute, hour, day).
	TimeTransform string
	Column        string
	Name          string
}

type QueryFilter struct {
	Type   string
	Column string
	Value  Untyped
}

type Query struct {
	TableName  string
	Aggregates []QueryAggregate
	Groupings  []QueryGrouping
	Filters    []QueryFilter
}

func isValidColumn(table *FactTable, columnName string) bool {
	_, ok := table.ColumnNameToIndex[columnName]
	return ok
}

func ValidateQuery(table *FactTable, query *Query) error {
	for _, queryAggregate := range query.Aggregates {
		if !isValidColumn(table, queryAggregate.Column) {
			return fmt.Errorf("Unrecognized column name in aggregates clause: %s", queryAggregate.Column)
		}
	}
	for _, grouping := range query.Groupings {
		if !isValidColumn(table, grouping.Column) {
			return fmt.Errorf("Unrecognized column name in grouping clause: %s", grouping.Column)
		}
		if grouping.TimeTransform != "" && grouping.TimeTransform != "minute" &&
			grouping.TimeTransform != "hour" && grouping.TimeTransform != "day" {
			return fmt.Errorf("Unrecogized time transform function: %s. Use one of {minute, hour, day}.",
				grouping.TimeTransform)
		}
	}
	for _, filter := range query.Filters {
		if !isValidColumn(table, filter.Column) {
			return fmt.Errorf("Unrecognized column name in filter clause: %s", filter.Column)
		}
	}
	return nil
}

func ParseJsonQuery(jsonString string) (*Query, error) {
	result := new(Query)
	err := json.Unmarshal([]byte(jsonString), result)
	if err != nil {
		return nil, err
	}
	return result, nil
}
