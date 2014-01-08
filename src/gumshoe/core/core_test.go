package core

import (
	"testing"
	. "github.com/smartystreets/goconvey/convey"
)

func tableFixture() *FactTable {
	return NewFactTable("", []string{"col1", "col2"})
}

func TestConvertRowMapToRowArray(t *testing.T) {
	Convey("throws an error for an unrecognized column", t, func() {
		_, error := convertRowMapToRowArray(tableFixture(), map[string]Untyped{"col1": 5, "unknownColumn": 10})
		So(error, ShouldNotBeNil)
	})
}
