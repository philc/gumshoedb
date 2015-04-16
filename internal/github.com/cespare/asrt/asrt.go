package asrt

import (
	"fmt"
	"path/filepath"
	"reflect"
	"runtime"
	"strings"
	"testing"
)

// Assert calls t.Fatal unless pred is true.
func Assert(t testing.TB, pred bool) {
	if !pred {
		fatal(t, "assertion failed")
	}
}

// Equal calls t.Fatal unless got == want.
func Equal(t testing.TB, got, want interface{}) {
	if got != want {
		fatal(t, fmt.Sprintf("not equal: got %v; want %v", got, want))
	}
}

// DeepEqual calls t.Fatal unless got equals want under reflect.DeepEqual.
func DeepEqual(t testing.TB, got, want interface{}) {
	if !reflect.DeepEqual(got, want) {
		fatal(t, fmt.Sprintf("deep equal failed: got %v; want %v", got, want))
	}
}

func fatal(t testing.TB, msg string) {
	// Hack to override the default line number printing.
	// This is the standard trick used by gocheck and others.
	// See https://code.google.com/p/go/issues/detail?id=4899
	// for one better solution for the future.
	t.Fatalf("\r\t%s: %s", caller(), msg)
}

// caller returns the file:lineno of the first caller up the stack
// that's not in this package.
func caller() string {
	for i := 0; ; i++ {
		_, file, line, ok := runtime.Caller(i)
		if !ok {
			return ""
		}
		dir, _ := filepath.Split(file)
		if strings.HasSuffix(dir, pkgPath+"/") {
			// Internal
			continue
		}
		return fmt.Sprintf("%s:%d", filepath.Base(file), line)
	}
}

type unexported int

// "github.com/cespare/asrt
var pkgPath = reflect.TypeOf(unexported(0)).PkgPath()
