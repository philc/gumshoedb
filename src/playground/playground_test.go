package playground

import "testing"
import "fmt"

func BenchmarkSumOne(b *testing.B) {
	InitInput()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		playground.SumOne()
	}
}

func BenchmarkSumMany(b *testing.B) {
	InitInput()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		playground.SumMany()
	}
}
