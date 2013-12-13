package main

import (
	json "encoding/json"
	"fmt"
)

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
	value Untyped
}

// type Query struct {
// 	Aggregates []QueryAggregate
// 	Groupings  []QueryGrouping
// }

type Untyped interface{}

// type JsonQueryGrouping struct {
// 	timeFunction

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

// grouppBy []JsonQueryGrouping

// filter [](map[string]

// Goal: Transform the JSON data structure into something that can he deconstructed and passed
// to the query function
