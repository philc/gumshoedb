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

func ParseJsonQuery(jsonString string) Query {
	var result Query
	err := json.Unmarshal([]byte(jsonString), &result)
	if err != nil {
		fmt.Println(err)
	}
	return result
}
