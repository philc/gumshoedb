package utils

import (
	"encoding/json"
	"fmt"
	"reflect"

	. "github.com/cespare/a"
)

// TODO(caleb): remove with HasEqualJSON
func convertToJSONAndBack(o interface{}) interface{} {
	b, err := json.Marshal(o)
	if err != nil {
		panic(err)
	}
	result := new(interface{})
	json.Unmarshal(b, result)
	return *result
}

// A variant of DeepEquals which is less finicky about which numeric type you're using in maps.
// TODO(caleb): delete; deprecated by deepEqual
func HasEqualJSON(args ...interface{}) (ok bool, message string) {
	o1 := convertToJSONAndBack(args[0])
	o2 := convertToJSONAndBack(args[1])
	return DeepEquals(o1, o2)
}

// DeepConvertibleEquals is a checker like DeepEqual which converts all numeric types to float64 before comparing.
// DeepConvertibleEquals only handles simple types, slices, and maps (but it is recursive).
// NOTE(caleb): Add more types as needed.
func DeepConvertibleEquals(args ...interface{}) (ok bool, message string) {
	params, message, err := ExpectNArgs(2, args)
	if err != nil {
		return false, err.Error()
	}
	if !deepValueConvertibleEquals(reflect.ValueOf(params[0]), reflect.ValueOf(params[1])) {
		if message != "" {
			return false, message
		}
		return false, fmt.Sprintf("deep equal: %+v does not equal %+v", params[0], params[1])
	}
	return true, ""
}

// DeepEqualsUnordered is a checker similar to DeepConvertibleEquals but expects two top-level slices, and does
// order-independent comparisons (for the top level only).
func DeepEqualsUnordered(args ...interface{}) (ok bool, message string) {
	params, message, err := ExpectNArgs(2, args)
	if err != nil {
		return false, err.Error()
	}
	v1 := reflect.ValueOf(params[0])
	v2 := reflect.ValueOf(params[1])
	if v1.Kind() != reflect.Slice || v2.Kind() != reflect.Slice {
		return false, "expect a slice for both arguments"
	}
	if v1.Len() != v2.Len() {
		return false, "slices have different length"
	}

	// This does the naive n^2 comparison between the slices.
	found := make([]bool, v1.Len()) // index in v2
outer:
	for i := 0; i < v1.Len(); i++ {
		item1 := v1.Index(i)
		for j := 0; j < v1.Len(); j++ {
			if found[j] {
				continue
			}
			item2 := v2.Index(j)
			if deepValueConvertibleEquals(item1, item2) {
				found[j] = true
				continue outer
			}
		}
		return false, fmt.Sprintf("element %+v found in first slice but not second", item1.Interface())
	}
	return true, ""
}

func deepValueConvertibleEquals(v1, v2 reflect.Value) bool {
	if !v1.Type().ConvertibleTo(v2.Type()) {
		return false
	}
	switch v2.Kind() {
	// We want to convert to floats if either is a float, so if v2 is a float, swap before converting.
	case reflect.Float32, reflect.Float64:
		v1, v2 = v2, v1
	}
	v2 = v2.Convert(v1.Type())
	kind := v1.Kind()
	switch kind {
	case reflect.Slice:
		if v1.IsNil() != v2.IsNil() {
			return false
		}
		if v1.Len() != v2.Len() {
			return false
		}
		for i := 0; i < v1.Len(); i++ {
			if !deepValueConvertibleEquals(v1.Index(i), v2.Index(i)) {
				return false
			}
		}
		return true
	case reflect.Map:
		if v1.IsNil() != v2.IsNil() {
			return false
		}
		if v1.Len() != v2.Len() {
			return false
		}
		for _, k := range v1.MapKeys() {
			if !deepValueConvertibleEquals(v1.MapIndex(k), v2.MapIndex(k)) {
				return false
			}
		}
		return true
	case reflect.Struct:
		if v1.NumField() != v2.NumField() {
			return false
		}
		for i := 0; i < v1.NumField(); i++ {
			if !deepValueConvertibleEquals(v1.Field(i), v2.Field(i)) {
				return false
			}
		}
		return true
	case reflect.Interface:
		if v1.IsNil() || v2.IsNil() {
			return v1.IsNil() == v2.IsNil()
		}
		return deepValueConvertibleEquals(v1.Elem(), v2.Elem())
	case reflect.String:
		return v1.String() == v2.String()
	case reflect.Bool:
		return v1.Bool() == v2.Bool()
	case reflect.Float32, reflect.Float64:
		// Ignore small rounding errors
		f1, f2 := v1.Float(), v2.Float()
		if f1 > f2 {
			f1, f2 = f2, f1
		}
		if f2 == 0 {
			return f1 == 0
		}
		return (f2-f1)/f2 < 0.001
	case reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uint:
		return v1.Uint() == v2.Uint()
	case reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64, reflect.Int:
		return v1.Int() == v2.Int()
	}
	panic(fmt.Sprintf("deepEqual doesn't know about type %s", v1.Type()))
}
