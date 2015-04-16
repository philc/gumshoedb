// Package a provides some simple assertions for tests using package testing.
//
// Assertions are used in tests like this:
//
//     a.Assert(t, foo, a.Equals, 5)
//
// where t is a *testing.T (or a *testing.B). Check may be substituted for Assert if the test should continue
// on failure. An error message about expected vs. got will be automatically generated, but a custom error
// message may be provided instead:
//
//     a.Check(t, bar, a.IsNil, "bar was expected to be nil")
//
// A Checker function is always the third argument to Check or Assert; the value under test is always the
// second. Subsequent arguments to the checker (if any) are in the fourth and later positions. The last
// argument is always the optional error message.
//
// A custom checker may be created by simply making a CheckerFunc (or a function with the same signature). The
// checker is called by passing in the Assert/Check arguments, starting with the value under test, then any
// further arguments, and finally the message if provided. The checker returns a boolean indicating
// success/failure and an error message to display. For example, in this statement:
//
//     a.Assert(t, foo, a.Equals, "hello world", "custom message")
//
// the Equals checker is called like this:
//
//     Equals(foo, "hello world", "custom message")
//
// Most checkers do not have specific documentation because their names fully describe their behavior.
package a

import (
	"errors"
	"fmt"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"unicode"
)

// Assert runs a check and aborts the test (calling t.Fatal) if it fails.
func Assert(t testing.TB, args ...interface{}) {
	if ok, message := assert("Assert", args...); !ok {
		t.Fatal(message)
	}
}

// Check runs a check and registers an error (calling t.Error) if it fails.
func Check(t testing.TB, args ...interface{}) {
	if ok, message := assert("Check", args...); !ok {
		t.Error(message)
	}
}

// A CheckerFunc is a function that runs a check. It is provided with one or more checker arguments and the
// custom error message, if provided. It returns a bool to indicate if the check succeeded and an error
// message to display.
type CheckerFunc func(args ...interface{}) (ok bool, message string)

func assert(fn string, args ...interface{}) (ok bool, message string) {
	if len(args) < 2 {
		return false, fmt.Sprintf(format("a.%s: too few arguments"), fn)
	}
	checker, ok := args[1].(func(...interface{}) (bool, string))
	if !ok {
		checker, ok = args[1].(CheckerFunc)
		if !ok {
			// Third argument because the first one is the testing.TB, not passed to assert.
			return false, fmt.Sprintf(format("a.%s: third argument not an a.CheckerFunc"), fn)
		}
	}
	if ok, message := checker(append(args[:1], args[2:]...)...); !ok {
		return false, format(message)
	}
	return true, ""
}

var (
	ErrNotEnoughArgs  = errors.New("ExpectNArgs: not enough arguments")
	ErrTooManyArgs    = errors.New("ExpectNArgs: too many arguments")
	ErrBadMessageType = errors.New("ExpectNArgs: last argument passed is not a string, error, or fmt.Stringer")
)

// ExpectNArgs is a helper function for writing custom CheckerFuncs which expect a fixed number of arguments.
// It takes an expected number of arguments and the argument list with an optional message at the end. It
// verifies that the argument list is of the correct length and coerces the optional message into a string
// (accepting either a string, an error, or a fmt.Stringer). If there are any problems, ExpectNArgs returns
// ErrNotEnoughArgs, ErrTooManyArgs, or ErrBadMessageType as appropriate. If err is nil, then len(params) is
// equal to n.
func ExpectNArgs(n int, args []interface{}) (params []interface{}, message string, err error) {
	if len(args) < n {
		return nil, "", ErrNotEnoughArgs
	}
	if len(args) > n+1 {
		return nil, "", ErrTooManyArgs
	}
	if len(args) == n+1 {
		switch s := args[n].(type) {
		case string:
			message = s
		case fmt.Stringer:
			message = s.String()
		case error:
			message = s.Error()
		default:
			return nil, "", ErrBadMessageType
		}
	}
	return args[:n], message, nil
}

func expectNArgs(n int, args []interface{}) (params []interface{}, message string, err error) {
	params, message, err = ExpectNArgs(n, args)
	switch err {
	case nil:
		return params, message, nil
	case ErrNotEnoughArgs:
		return nil, "", errors.New(fnPrefix("not enough arguments"))
	case ErrTooManyArgs:
		return nil, "", errors.New(fnPrefix("too many arguments"))
	case ErrBadMessageType:
		e := "last argument passed is not a string, error, or fmt.Stringer: %#v"
		return nil, "", errors.New(fnPrefix(e, args[n]))
	default:
		return nil, "", err
	}
}

func format(err string) string {
	return fmt.Sprintf("\r\t%s: %s", caller(), err)
}

func fnPrefix(format string, args ...interface{}) string {
	return fmt.Sprintf(publicFn()+": "+format, args...)
}

func isInternal(path string) bool {
	dir, filename := filepath.Split(path)
	return strings.HasSuffix(dir, "github.com/cespare/a/") && filename != "a_test.go"
}

// caller returns the file:lineno of the first caller up the stack that's not in package a.
func caller() string {
	for i := 0; ; i++ {
		_, file, line, ok := runtime.Caller(i)
		if !ok {
			return ""
		}
		if isInternal(file) {
			continue
		}
		return fmt.Sprintf("%s:%d", filepath.Base(file), line)
	}
}

// publicFn traverses up the stack until it finds an exported function from this package and returns its name.
// This is useful to formulate error messages which have the right public function name in them from inside
// helper functions.
func publicFn() string {
	for i := 0; ; i++ {
		pc, file, _, ok := runtime.Caller(i)
		if !ok {
			return ""
		}
		if !isInternal(file) {
			return ""
		}
		name := filepath.Base(runtime.FuncForPC(pc).Name())
		parts := strings.Split(name, ".")
		if len(parts) != 2 || parts[1] == "" {
			return ""
		}
		if unicode.IsUpper([]rune(parts[1])[0]) {
			// Exported
			return name
		}
	}
}
