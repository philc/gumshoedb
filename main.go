package main

import (
	"fmt"
	"testing"
	"time"
)

// func BenchmarkSumOne(b *testing.B) {
// 	InitInput()
// 	b.ResetTimer()
// 	for i := 0; i < b.N; i++ {
// 		SumOne()
// 	}
// }

// func BenchmarkSumMany(b *testing.B) {
// 	InitInput()
// 	b.ResetTimer()
// 	for i := 0; i < b.N; i++ {
// 		SumMany()
// 	}
// }

// func trace(s string) (string, time.Time) {
//     fmt.Println("START:", s)
//     return s, time.Now()
// }

// func un(s string, startTime time.Time) {
//     endTime := time.Now()
//     fmt.Println("  END:", s, "ElapsedTime in seconds:", endTime.Sub(startTime))
// }

// func runBenchmark() {
// 	defer un(trace("Time check"))
// 	SumOne()
// }



func main() {
	// fmt.Println("yo")
	// br := testing.Benchmark(BenchmarkSumOne)
	// fmt.Println(br)
	InitInput()
	runBenchmark()
}
