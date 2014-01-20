package gumshoe_test

import (
	"testing"
	"unsafe"
)

// A filter is a fake representation of a filtering function with a type and value. For instance, one type may
// represent "less than", and the value is the number to compare with.
type filter struct {
	Type  int
	Value Cell
}

func BenchmarkFilterSliceUnrolled(b *testing.B) {
	matrix := createContiguousSliceByteMatrix(Cols)
	var sum SumType
	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		sum = 0
		for i := 0; i < Rows; i++ {
			v := &matrix[i*int(RowSize)+0]
			cell := *(*Cell)(unsafe.Pointer(v))
			if cell < 0 {
				continue
			}
			if cell < -1 {
				continue
			}
			sum += SumType(cell)
		}
	}
	b.StopTimer()
	checkExpectedSum(b, sum)
}

// Sums columns over a virtual byte matrix stored in a contiguous slice.
func BenchmarkFilterSlice(b *testing.B) {
	matrix := createContiguousSliceByteMatrix(Cols)
	filters := []filter{{1, 0}, {1, -1}}
	var sum SumType
	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		sum = 0
		for i := 0; i < Rows; i++ {
			v := &matrix[i*int(RowSize)+0]
			cell := *(*Cell)(unsafe.Pointer(v))
			for _, filter := range filters {
				switch filter.Type {
				// Suppose there are 3 possible kinds of filters, and unluckily ours is the last to be checked.
				case 0:
				case 2:
				case 1:
					if cell < filter.Value {
						continue
					}
				}
			}
			sum += SumType(cell)
		}
	}
	b.StopTimer()
	checkExpectedSum(b, sum)
}
