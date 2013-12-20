package core

import (
	json "encoding/json"
	"fmt"
)

type Untyped interface{}

type QueryAggregate struct {
	Type string
	Column        string
	Name    string
}

type QueryGrouping struct {
	TimeFunction string
	Column       string
	Name   string
}

// TODO(philc): This needs to wait for join tables.
type QueryFilter struct {
	Type string
	Column string
	Value Untyped
}

type Query struct {
	Table      string
	Aggregates []QueryAggregate
	Groupings []QueryGrouping
	Filters []QueryFilter
}

func ValidateQuery(table *FactTable, query *Query) error {
	for _, queryAggregate := range query.Aggregates {
		if _, ok := table.ColumnNameToIndex[queryAggregate.Column]; !ok {
			return fmt.Errorf("Unrecognized column name: %s", queryAggregate.Column)
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
