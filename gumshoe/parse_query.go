// Functions and data types for parsing JSON queries into typed structs.

package gumshoe

import (
	"encoding/json"
	"fmt"
	"io"
)

func ParseJSONQuery(r io.Reader) (*Query, error) {
	query := new(Query)
	decoder := json.NewDecoder(r)
	if err := decoder.Decode(query); err != nil {
		return nil, err
	}
	return query, nil
}

type Query struct {
	Aggregates []QueryAggregate
	Groupings  []QueryGrouping
	Filters    []QueryFilter
}

type QueryAggregate struct {
	Type   AggregateType
	Column string
	Name   string
}

type QueryGrouping struct {
	// This provides a means of specifying an optional date truncation function, assuming the column is a
	// timestamp. It makes it possible to group by time intervals (minute, hour, day).
	TimeTransform TimeTruncationType
	Column        string
	Name          string
}

type QueryFilter struct {
	Type   FilterType
	Column string
	Value  Untyped
}

func (a *QueryAggregate) UnmarshalJSON(b []byte) error {
	var agg struct {
		Type   AggregateType
		Column string
		Name   string
	}
	if err := json.Unmarshal(b, &agg); err != nil {
		return err
	}
	*a = QueryAggregate(agg)
	if a.Name == "" {
		a.Name = a.Column
	}
	return nil
}

func (g *QueryGrouping) UnmarshalJSON(b []byte) error {
	errInvalid := fmt.Errorf("invalid grouping: %q", b)
	if len(b) < 2 {
		return errInvalid
	}
	if b[0] == '"' {
		var s string
		if err := json.Unmarshal(b, &s); err != nil {
			return errInvalid
		}
		*g = QueryGrouping{Column: s}
	} else {
		var grouping struct {
			TimeTransform TimeTruncationType
			Column        string
			Name          string
		}
		if err := json.Unmarshal(b, &grouping); err != nil {
			return fmt.Errorf("invalid grouping: %q (%s)", b, err)
		}
		*g = QueryGrouping(grouping)
	}
	if g.Name == "" {
		g.Name = g.Column
	}
	return nil
}

type AggregateType int

const (
	AggregateSum AggregateType = iota
	AggregateAvg
)

func (t AggregateType) MarshalJSON() ([]byte, error) {
	switch t {
	case AggregateSum:
		return []byte(`"sum"`), nil
	case AggregateAvg:
		return []byte(`"average"`), nil
	default:
		panic("bad type")
	}
}

func (t *AggregateType) UnmarshalJSON(b []byte) error {
	var name string
	if err := json.Unmarshal(b, &name); err != nil {
		return err
	}
	switch name {
	case "sum":
		*t = AggregateSum
	case "average":
		*t = AggregateAvg
	default:
		return fmt.Errorf("bad aggregate type: %q", name)
	}
	return nil
}

type TimeTruncationType int

const (
	TimeTruncationNone TimeTruncationType = iota
	TimeTruncationMinute
	TimeTruncationHour
	TimeTruncationDay
)

func (t TimeTruncationType) MarshalJSON() ([]byte, error) {
	var name string
	switch t {
	case TimeTruncationNone:
		return []byte("null"), nil
	case TimeTruncationMinute:
		name = "minute"
	case TimeTruncationHour:
		name = "hour"
	case TimeTruncationDay:
		name = "day"
	default:
		panic("bad time truncation type")
	}
	return []byte(fmt.Sprintf("%q", name)), nil
}

func (t *TimeTruncationType) UnmarshalJSON(b []byte) error {
	if string(b) == "null" {
		*t = TimeTruncationNone
		return nil
	}

	var name string
	if err := json.Unmarshal(b, &name); err != nil {
		return err
	}
	switch name {
	case "minute":
		*t = TimeTruncationMinute
	case "hour":
		*t = TimeTruncationHour
	case "day":
		*t = TimeTruncationDay
	default:
		return fmt.Errorf("bad time truncation function: %q", name)
	}
	return nil
}

// See FilterType definitions in type_gen.go

func (t FilterType) MarshalJSON() ([]byte, error) {
	if int(t) >= len(filterTypeToName) {
		panic("bad filter type")
	}
	name := filterTypeToName[t]
	return []byte(fmt.Sprintf("%q", name)), nil
}

func (t *FilterType) UnmarshalJSON(b []byte) error {
	var name string
	if err := json.Unmarshal(b, &name); err != nil {
		return err
	}
	typ, ok := filterNameToType[name]
	if !ok {
		return fmt.Errorf("%q is not a valid filter type", name)
	}
	*t = typ
	return nil
}
