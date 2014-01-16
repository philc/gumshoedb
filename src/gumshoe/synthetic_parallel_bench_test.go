// Make sure to run these with GOMAXPROCS=8 (or -test.cpu=8) for full effect. (And run on a machine with >= 8
// cores to get meaningful numbers for the 8-way parallel case.)
package gumshoe_test

import (
	"sync"
	"testing"
	"unsafe"
)

func BenchmarkParallelMatrix8Col1Thread(b *testing.B)    { runParallelBenchmark(b, 8, 1) }
func BenchmarkParallelMatrix8Col2Threads(b *testing.B)   { runParallelBenchmark(b, 8, 2) }
func BenchmarkParallelMatrix8Col4Threads(b *testing.B)   { runParallelBenchmark(b, 8, 4) }
func BenchmarkParallelMatrix8Col8Threads(b *testing.B)   { runParallelBenchmark(b, 8, 8) }
func BenchmarkParallelMatrix128Col1Thread(b *testing.B)  { runParallelBenchmark(b, 128, 1) }
func BenchmarkParallelMatrix128Col2Threads(b *testing.B) { runParallelBenchmark(b, 128, 2) }
func BenchmarkParallelMatrix128Col4Threads(b *testing.B) { runParallelBenchmark(b, 128, 4) }
func BenchmarkParallelMatrix128Col8Threads(b *testing.B) { runParallelBenchmark(b, 128, 8) }

func runParallelBenchmark(b *testing.B, cols, parallelism int) {
	rowSize := ColSize * uintptr(cols)
	matrix := createContiguousSliceByteMatrix(cols)
	var sum SumType
	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		sum = 0
		var wg sync.WaitGroup
		wg.Add(parallelism)
		sums := make(chan SumType)
		rowsPerWorker := Rows / parallelism
		for i := 0; i < parallelism; i++ {
			p := uintptr(unsafe.Pointer(&matrix[0])) + uintptr(i)*rowSize*uintptr(rowsPerWorker)
			go func(p uintptr) {
				defer wg.Done()
				var s SumType
				for i := 0; i < rowsPerWorker; i++ {
					s += *(*SumType)(unsafe.Pointer(p))
					p += rowSize
				}
				sums <- s
			}(p)
		}
		done := make(chan bool)
		go func() {
			wg.Wait()
			done <- true
		}()
	wait:
		for {
			select {
			case s := <-sums:
				sum += s
			case <-done:
				break wait
			}
		}
	}
	b.StopTimer()
	checkExpectedSum(b, sum)
}
