package main

import (
	"flag"
	"testing"
	"fmt"
)

func runBenchmarkFunction(displayName string, f func()) {
	benchmarkHandler := func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			f()
		}
	}
	result := testing.Benchmark(benchmarkHandler)
	fmt.Printf("%-25s %-3.2f ms\n", displayName, float32(result.NsPerOp()) / 1000000.0)
}

func runBenchmarkFunctionWithReturnValue(displayName string, f func() int) {
	returnValue := 0
	benchmarkHandler := func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			returnValue = f()
		}
	}
	result := testing.Benchmark(benchmarkHandler)
	fmt.Printf("%-25s %-3.2f ms Result: %d\n", displayName, float32(result.NsPerOp()) / 1000000.0, returnValue)
}

type BenchmarkFlags struct {
	cpuprofile *string
	minimalSet *bool
}

func parseCliFlags() BenchmarkFlags {
	flags := BenchmarkFlags{}
	flags.cpuprofile = flag.String("cpuprofile", "",
		"Enable profiling for the non-synthetic benchmarks and write results to the given file")
	// TODO(philc): This flag is just for convenience while developing. This should instead be a regexp filter
	// like with `go test`.
	flags.minimalSet = flag.Bool("minimal-set", false, "Run only the minimal set of benchmarks")
	flag.Parse()
	return flags
}

func main() {
	flags := parseCliFlags()
	runCoreBenchmarks(flags)
	runSyntheticBenchmarks(flags)
}
