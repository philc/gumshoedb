// Functions and data types for parsing JSON queries into typed structs.
package gumshoe

import (
	"encoding/json"
	"fmt"
	"io"
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

func isValidColumn(table *FactTable, name string) bool {
	_, ok := table.ColumnNameToIndex[name]
	return ok || name == table.TimestampColumnName
}

var validFilterTypes = make(map[string]bool)

func init() {
	for _, filterType := range []string{"=", "!=", ">", ">=", "<", "<=", "in"} {
		validFilterTypes[filterType] = true
	}
}

func ValidateQuery(table *FactTable, query *Query) error {
	for _, aggregate := range query.Aggregates {
		if aggregate.Type != "sum" && aggregate.Type != "average" {
			return fmt.Errorf("Unrecognized aggregate type: %s", aggregate.Type)
		}
		_, ok := table.Schema.MetricColumns[aggregate.Column]
		if !ok {
			return fmt.Errorf("Only metric columns can be used in aggregates. %s is not a metric column.",
				aggregate.Column)
		}
	}
	for _, grouping := range query.Groupings {
		if _, ok := table.Schema.DimensionColumns[grouping.Column]; !ok {
			return fmt.Errorf("Only dimension columns can be used in grouping. %s is not a dimension column.",
				grouping.Column)
		}
		switch grouping.TimeTransform {
		case "", "minute", "hour", "day":
		default:
			return fmt.Errorf("Unrecogized time transform function: %s. Use one of {minute, hour, day}.",
				grouping.TimeTransform)
		}
	}
	for _, filter := range query.Filters {
		if !isValidColumn(table, filter.Column) {
			return fmt.Errorf("Unrecognized column name in filter clause: %s", filter.Column)
		}
		if _, ok := validFilterTypes[filter.Type]; !ok {
			return fmt.Errorf("%s is not a valid filter type.", filter.Type)
		}
	}
	return nil
}

func ParseJSONQuery(r io.Reader) (*Query, error) {
	query := new(Query)
	decoder := json.NewDecoder(r)
	if err := decoder.Decode(query); err != nil {
		return nil, err
	}
	return query, nil
}
