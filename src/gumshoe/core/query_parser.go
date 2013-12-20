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
	TimeFunction string
	Column       string
	Name         string
}

// TODO(philc): This needs to wait for join tables.
type QueryFilter struct {
	Type   string
	Column string
	Value  Untyped
}

type Query struct {
	Table      string
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
