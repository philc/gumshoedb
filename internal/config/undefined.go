package config

import (
	"fmt"
	"reflect"

	"github.com/philc/gumshoedb/internal/github.com/BurntSushi/toml"
)

func checkUndefinedFields(meta toml.MetaData, v interface{}) error {
	for _, name := range nestedTOMLFields(reflect.ValueOf(v), nil) {
		if !meta.IsDefined(name...) {
			return fmt.Errorf("field %q not provided in config", toml.Key(name))
		}
	}
	return nil
}

// nestedTOMLFields accepts a pointer to a struct type and returns nested list of toml field names (names
// given in the "toml" struct tag).
//
// Example:
//
//     type A struct {
//         B int `toml:"bar"`
//         C struct {
//           D int
//         }
//     }
//
// corresponds to
//
//     [][]string{{"bar"}, {"c", "d"}}
func nestedTOMLFields(v reflect.Value, prefix []string) [][]string {
	switch v.Kind() {
	case reflect.Ptr:
		if v.IsNil() {
			return nil
		}
		return nestedTOMLFields(v.Elem(), prefix)
	case reflect.Struct:
		var names [][]string
		for i := 0; i < v.NumField(); i++ {
			field := v.Field(i)
			if !field.CanSet() {
				continue
			}
			tag := v.Type().Field(i).Tag.Get("toml")
			if tag == "" || tag == "-" {
				continue
			}
			prefixCopy := make([]string, len(prefix))
			copy(prefixCopy, prefix)
			prefixedName := append(prefixCopy, tag)
			names = append(names, prefixedName)
			names = append(names, nestedTOMLFields(field, prefixedName)...)
		}
		return names
	}
	return nil
}
