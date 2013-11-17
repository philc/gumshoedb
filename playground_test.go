package main

import "testing"

func BenchmarkSumOneUnrolledLoop(b *testing.B) {
	InitInput()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		SumOneUnrolledLoop()
	}
}

func BenchmarkSumOne(b *testing.B) {
	InitInput()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		SumOne()
	}
}

func BenchmarkSumOneRawMatrix(b *testing.B) {
	matrix := InitIntMatrix()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		SumOneRawMatrix(matrix)
	}
}

// func BenchmarkSumMany(b *testing.B) {
// 	InitInput()
// 	b.ResetTimer()
// 	for i := 0; i < b.N; i++ {
// 		SumMany()
// 	}
// }


func BenchmarkSumUsingFilter(b *testing.B) {
	InitInput()
	filterFunction := func(row *[5]int32) bool {
		// return true
		return (row[0] == 123 || row[1] == 123 || row[2] == 123)
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		SumUsingFunction(filterFunction)
	}
}
