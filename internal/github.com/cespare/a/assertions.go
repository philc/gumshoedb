package a

import (
	"fmt"
	"reflect"
	"regexp"
	"strings"
)

// DeepEquals is a checker which compares two values using reflect.DeepEquals.
func DeepEquals(args ...interface{}) (ok bool, message string) {
	params, msg, err := expectNArgs(2, args)
	if err != nil {
		return false, err.Error()
	}
	if reflect.DeepEqual(params[0], params[1]) {
		return true, ""
	}
	if msg == "" {
		return false, fnPrefix("expected %#v, but got %#v", params[1], params[0])
	}
	return false, msg
}

func Equals(args ...interface{}) (ok bool, message string) {
	params, msg, err := expectNArgs(2, args)
	if err != nil {
		return false, err.Error()
	}
	defer func() {
		if err := recover(); err != nil {
			ok = false
			message = fmt.Sprint(err)
		}
	}()
	if params[0] == params[1] {
		return true, ""
	}
	if msg == "" {
		return false, fnPrefix("expected %#v, but got %#v", params[1], params[0])
	}
	return false, msg
}

func IsTrue(args ...interface{}) (ok bool, message string) {
	params, msg, err := expectNArgs(1, args)
	if err != nil {
		return false, err.Error()
	}
	defer func() {
		if err := recover(); err != nil {
			ok = false
			message = fmt.Sprint(err)
		}
	}()
	if params[0] == true {
		return true, ""
	}
	if msg == "" {
		return false, fnPrefix("expected true, but got %#v", params[0])
	}
	return false, msg
}

func IsFalse(args ...interface{}) (ok bool, message string) {
	params, msg, err := expectNArgs(1, args)
	if err != nil {
		return false, err.Error()
	}
	defer func() {
		if err := recover(); err != nil {
			ok = false
			message = fmt.Sprint(err)
		}
	}()
	if params[0] == false {
		return true, ""
	}
	if msg == "" {
		return false, fnPrefix("expected false, but got %#v", params[0])
	}
	return false, msg
}

func IsNil(args ...interface{}) (ok bool, message string) {
	params, msg, err := expectNArgs(1, args)
	if err != nil {
		return false, err.Error()
	}
	defer func() {
		if err := recover(); err != nil {
			ok = false
			message = fmt.Sprint(err)
		}
	}()
	if params[0] == nil {
		return true, ""
	}
	switch v := reflect.ValueOf(params[0]); v.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Ptr, reflect.Slice:
		if v.IsNil() {
			return true, ""
		}
	}
	if msg == "" {
		return false, fnPrefix("expected nil, but got %#v", params[0])
	}
	return false, msg
}

func NotNil(args ...interface{}) (ok bool, message string) {
	params, msg, err := expectNArgs(1, args)
	if err != nil {
		return false, err.Error()
	}
	isNil, _ := IsNil(args...)
	if !isNil {
		return true, ""
	}
	if msg == "" {
		return false, fnPrefix("expected non-nil, but got %#v", params[0])
	}
	return false, msg
}

// StringContains is a checker accepts two strings, s1 and s2, and checks that s2 is a substring of s1.
func StringContains(args ...interface{}) (ok bool, message string) {
	params, msg, err := expectNArgs(2, args)
	if err != nil {
		return false, err.Error()
	}
	s1, ok := params[0].(string)
	if !ok {
		return false, fnPrefix("expected string arguments, but got %#v", params[0])
	}
	s2, ok := params[1].(string)
	if !ok {
		return false, fnPrefix("expected string arguments, but got %#v", params[1])
	}
	if strings.Contains(s1, s2) {
		return true, ""
	}
	return false, msg
}

// StringContains is a checker accepts two strings, s and r, compiles r to a regexp.Regexp, and checks that s
// matches it.
func StringMatches(args ...interface{}) (ok bool, message string) {
	params, msg, err := expectNArgs(2, args)
	if err != nil {
		return false, err.Error()
	}
	s, ok := params[0].(string)
	if !ok {
		return false, fnPrefix("expected string arguments, but got %#v", params[0])
	}
	rs, ok := params[1].(string)
	if !ok {
		return false, fnPrefix("expected string arguments, but got %#v", params[1])
	}
	r, err := regexp.Compile(rs)
	if err != nil {
		return false, fnPrefix("could not compile regexp: %s", err)
	}
	if r.MatchString(s) {
		return true, ""
	}
	return false, msg
}
